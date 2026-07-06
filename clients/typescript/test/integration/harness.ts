/**
 * Integration harness: boots the REAL `whdr-server` binary against temp dirs,
 * mirroring `crates/whdr-test-support`'s `ServerBuilder`. It installs a scripted
 * fake extension (`whdr-ext-<id>`, echo behaviour), mints tokens over the admin
 * control socket with the `whdr` CLI, and POSTs to the ingest listener to emit
 * events. No cargo — uses prebuilt binaries.
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createServer } from "node:net";
import { mkdtempSync, copyFileSync, writeFileSync, chmodSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const REPO_ROOT = process.env["WHDR_REPO_ROOT"] ?? "/home/xertrov/src/whdr";
const SERVER_BIN =
  process.env["WHDR_SERVER_BIN"] ?? join(REPO_ROOT, "target/debug/whdr-server");
const CLI_BIN = process.env["WHDR_CLI_BIN"] ?? join(REPO_ROOT, "target/debug/whdr");
const FAKE_EXT_BIN =
  process.env["WHDR_FAKE_EXT_BIN"] ??
  join(REPO_ROOT, "target/debug/examples/whdr-ext-fake");

/** Whether all binaries the harness needs are present (for describe.skipIf). */
export function binariesAvailable(): boolean {
  return [SERVER_BIN, CLI_BIN, FAKE_EXT_BIN].every(existsSync);
}

export function missingBinariesMessage(): string {
  const missing = [SERVER_BIN, CLI_BIN, FAKE_EXT_BIN].filter((p) => !existsSync(p));
  return `integration binaries missing: ${missing.join(", ")}`;
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const address = srv.address();
      if (address === null || typeof address === "string") {
        srv.close();
        reject(new Error("could not determine free port"));
        return;
      }
      const { port } = address;
      srv.close(() => resolve(port));
    });
  });
}

export interface ServerOptions {
  /** Extension id installed as `whdr-ext-<id>` (echo behaviour). Default "alpha". */
  extId?: string;
  /** Enable durable delivery. Extra `[delivery]` lines (e.g. "max_events = 1"). */
  delivery?: { extraLines?: string } | false;
  /** Extra `[limits]` lines (e.g. "sub_queue_len = 1"). */
  limits?: string;
}

export class WhdrServer {
  #child: ChildProcess | undefined;

  private constructor(
    readonly tempDir: string,
    readonly ingestPort: number,
    readonly subPort: number,
    readonly metricsPort: number,
    readonly controlSocket: string,
    readonly configPath: string,
    readonly extId: string,
    readonly storePath: string | null,
  ) {}

  /** The `ws://` subscribe URL for the running server. */
  get subUrl(): string {
    return `ws://127.0.0.1:${this.subPort}/subscribe`;
  }

  static async start(options: ServerOptions = {}): Promise<WhdrServer> {
    const extId = options.extId ?? "alpha";
    const temp = mkdtempSync(join(tmpdir(), "whdr-ts-it-"));
    const extDir = join(temp, "exts");
    spawnSync("mkdir", ["-p", extDir]);

    // Install the fake ext (echo) under whdr-ext-<id> + empty behaviour file.
    const extBin = join(extDir, `whdr-ext-${extId}`);
    copyFileSync(FAKE_EXT_BIN, extBin);
    chmodSync(extBin, 0o755);
    writeFileSync(join(extDir, `whdr-ext-${extId}.toml`), "");

    // Secrets file (0600), one entry per enabled ext.
    const secretsPath = join(temp, "secrets.toml");
    writeFileSync(secretsPath, `${extId} = "secret-${extId}"\n`);
    chmodSync(secretsPath, 0o600);

    const ingestPort = await freePort();
    const subPort = await freePort();
    const metricsPort = await freePort();
    const controlSocket = join(temp, "ctl.sock");
    const tokenStore = join(temp, "tokens.toml");
    const storePath =
      options.delivery !== false && options.delivery !== undefined
        ? join(temp, "delivery.redb")
        : null;

    const deliveryBlock =
      storePath !== null
        ? `[delivery]\nenabled = true\nstore_path = "${storePath}"\n${options.delivery && typeof options.delivery === "object" ? (options.delivery.extraLines ?? "") : ""}\n`
        : "";

    const configPath = join(temp, "config.toml");
    writeFileSync(
      configPath,
      `[server]
listen_addr = "127.0.0.1:${ingestPort}"
sub_addr = "127.0.0.1:${subPort}"
metrics_addr = "127.0.0.1:${metricsPort}"
control_socket = "${controlSocket}"

[subscribers]
token_store = "${tokenStore}"

[extensions]
enabled = ["${extId}"]

[limits]
${options.limits ?? ""}

[timeouts]

${deliveryBlock}[secrets]
file = "${secretsPath}"
`,
    );

    const server = new WhdrServer(
      temp,
      ingestPort,
      subPort,
      metricsPort,
      controlSocket,
      configPath,
      extId,
      storePath,
    );
    await server.#spawn();
    return server;
  }

  async #spawn(): Promise<void> {
    const extDir = join(this.tempDir, "exts");
    const child = spawn(SERVER_BIN, ["--config", this.configPath], {
      env: { ...process.env, PATH: `${extDir}:${process.env["PATH"] ?? ""}` },
      stdio: ["ignore", "pipe", "pipe"],
    });
    this.#child = child;
    await this.#waitControlReady();
    await this.#waitExtReady(this.extId);
  }

  async #waitControlReady(): Promise<void> {
    const deadline = Date.now() + 10_000;
    for (;;) {
      const result = spawnSync(CLI_BIN, ["--socket", this.controlSocket, "status", "--json"], {
        encoding: "utf8",
      });
      if (result.status === 0) return;
      if (Date.now() > deadline) throw new Error("whdr-server did not become ready");
      await delay(50);
    }
  }

  status(): unknown {
    const result = spawnSync(CLI_BIN, ["--socket", this.controlSocket, "status", "--json"], {
      encoding: "utf8",
    });
    if (result.status !== 0) throw new Error(`status failed: ${result.stderr}`);
    return JSON.parse(result.stdout);
  }

  async #waitExtReady(extId: string): Promise<void> {
    const deadline = Date.now() + 10_000;
    for (;;) {
      const status = this.status() as { extensions?: Array<{ id?: string; state?: string }> };
      const ext = status.extensions?.find((e) => e.id === extId);
      if (ext?.state === "Ready") return;
      if (Date.now() > deadline) throw new Error(`ext ${extId} never became Ready`);
      await delay(50);
    }
  }

  /** Mint a subscriber token; returns the `tok_…` value. */
  tokenAdd(name: string): string {
    const result = spawnSync(CLI_BIN, ["--socket", this.controlSocket, "token", "add", name], {
      encoding: "utf8",
    });
    if (result.status !== 0) throw new Error(`token add failed: ${result.stderr}`);
    // Output form: "<name>: tok_XXXX"
    const match = result.stdout.match(/:\s*(tok_\S+)/);
    if (!match) throw new Error(`could not parse token from: ${result.stdout}`);
    return match[1]!;
  }

  /** POST a webhook body to the ext path, emitting one event on `<ext>.echo`. */
  async emit(body: string, path = `/${this.extId}`): Promise<number> {
    const res = await fetch(`http://127.0.0.1:${this.ingestPort}${path}`, {
      method: "POST",
      body,
    });
    // Drain the body so the socket is released.
    await res.arrayBuffer();
    return res.status;
  }

  async stop(): Promise<void> {
    const child = this.#child;
    this.#child = undefined;
    if (!child || child.exitCode !== null) return;
    await new Promise<void>((resolve) => {
      child.once("exit", () => resolve());
      child.kill("SIGTERM");
      setTimeout(() => {
        if (child.exitCode === null) child.kill("SIGKILL");
      }, 5_000);
    });
  }
}

/** Ensure a directory exists (used by callers that stat the store). */
export function storeDir(server: WhdrServer): string | null {
  return server.storePath ? dirname(server.storePath) : null;
}
