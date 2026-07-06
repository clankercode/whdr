"""Boot a real ``whdr-server`` for integration tests.

Mirrors ``crates/whdr-test-support`` (config shape, PATH-scoped fake extensions,
control-socket readiness, token minting), but in Python so the tests exercise
the actual client library against the real daemon. If the prebuilt binaries are
absent the tests that use this harness skip.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import stat
import tempfile
from pathlib import Path
from typing import Any

# Prebuilt binaries (current master, durable delivery included). Do NOT build.
# When running inside a git worktree the compiled target/ lives in the main
# checkout, so walk up parents until we find target/debug/whdr-server.
def _find_target_debug() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "target" / "debug" / "whdr-server"
        if candidate.exists():
            return parent / "target" / "debug"
    return None


_DEBUG = _find_target_debug()
SERVER_BIN = (_DEBUG or Path("target/debug")) / "whdr-server"
WHDR_BIN = (_DEBUG or Path("target/debug")) / "whdr"
FAKE_EXT_BIN = (_DEBUG or Path("target/debug")) / "examples" / "whdr-ext-fake"

BINARIES_AVAILABLE = SERVER_BIN.exists() and WHDR_BIN.exists() and FAKE_EXT_BIN.exists()
SKIP_REASON = "whdr prebuilt binaries (target/debug/whdr-server, whdr, examples/whdr-ext-fake) not found"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    """A spawned whdr-server plus helpers to drive it."""

    def __init__(self, *, delivery: bool, ext_id: str = "alpha") -> None:
        self._delivery = delivery
        self._ext_id = ext_id
        self._tmp = tempfile.TemporaryDirectory(prefix="whdr-pyit-")
        self.root = Path(self._tmp.name)
        self.ingest_port = _free_port()
        self.sub_port = _free_port()
        self.metrics_port = _free_port()
        self.control_socket = self.root / "ctl.sock"
        self._proc: asyncio.subprocess.Process | None = None

    @property
    def sub_url(self) -> str:
        return f"ws://127.0.0.1:{self.sub_port}/subscribe"

    def _write_layout(self) -> None:
        exts = self.root / "exts"
        exts.mkdir()
        bin_dst = exts / f"whdr-ext-{self._ext_id}"
        shutil.copy(FAKE_EXT_BIN, bin_dst)
        os.chmod(bin_dst, 0o755)
        (exts / f"whdr-ext-{self._ext_id}.toml").write_text("")  # echo behaviour

        secrets = self.root / "secrets.toml"
        secrets.write_text(f'{self._ext_id} = "secret-{self._ext_id}"\n')
        os.chmod(secrets, 0o600)

        delivery_block = ""
        if self._delivery:
            store = self.root / "delivery.redb"
            delivery_block = (
                "[delivery]\n"
                "enabled = true\n"
                f'store_path = "{store}"\n'
                "prune_interval_secs = 1\n\n"
            )

        (self.root / "config.toml").write_text(
            f"""[server]
listen_addr = "127.0.0.1:{self.ingest_port}"
sub_addr = "127.0.0.1:{self.sub_port}"
metrics_addr = "127.0.0.1:{self.metrics_port}"
control_socket = "{self.control_socket}"

[subscribers]
token_store = "{self.root / 'tokens.toml'}"

[extensions]
enabled = ["{self._ext_id}"]

[limits]

[timeouts]

{delivery_block}[secrets]
file = "{secrets}"
"""
        )

    async def start(self) -> Server:
        self._write_layout()
        env = dict(os.environ)
        env["PATH"] = f"{self.root / 'exts'}{os.pathsep}{env.get('PATH', '')}"
        self._log = open(self.root / "server.log", "wb")
        self._proc = await asyncio.create_subprocess_exec(
            str(SERVER_BIN),
            "--config",
            str(self.root / "config.toml"),
            env=env,
            stdout=self._log,
            stderr=self._log,
        )
        await self._wait_ready()
        await self._wait_ext_ready()
        return self

    async def _control(self, *args: str) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            str(WHDR_BIN),
            "--socket",
            str(self.control_socket),
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await proc.communicate()
        return proc.returncode or 0, out.decode()

    async def _wait_ready(self, timeout: float = 15.0) -> None:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            code, _ = await self._control("status")
            if code == 0:
                return
            await asyncio.sleep(0.1)
        raise RuntimeError(f"server not ready; log:\n{self.logs()}")

    async def _wait_ext_ready(self, timeout: float = 15.0) -> None:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            code, out = await self._control("status", "--json")
            if code == 0 and '"Ready"' in out and self._ext_id in out:
                return
            await asyncio.sleep(0.1)
        raise RuntimeError(f"extension never Ready; log:\n{self.logs()}")

    async def token_add(self, name: str) -> str:
        code, out = await self._control("token", "add", name)
        if code != 0:
            raise RuntimeError(f"token add failed: {out}")
        # Output form: "<name>: tok_XXXX"
        token = out.strip().split(":", 1)[1].strip()
        assert token.startswith("tok_"), out
        return token

    async def emit(self, body: bytes, path: str | None = None) -> int:
        """POST a webhook body to the ingest listener; returns the HTTP status.

        Routes to the fake extension, which echoes it as an event on channel
        ``<ext_id>.echo``.
        """
        path = path or f"/{self._ext_id}"
        reader, writer = await asyncio.open_connection("127.0.0.1", self.ingest_port)
        head = (
            f"POST {path} HTTP/1.1\r\nHost: whdr\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
        ).encode()
        writer.write(head + body)
        await writer.drain()
        data = await reader.read()
        writer.close()
        await writer.wait_closed()
        return int(data.split()[1])

    def store_path(self) -> Path:
        return self.root / "delivery.redb"

    def store_mode(self) -> int:
        return stat.S_IMODE(self.store_path().stat().st_mode)

    def logs(self) -> str:
        try:
            return (self.root / "server.log").read_text()
        except OSError:
            return "<no log>"

    async def stop(self) -> None:
        if self._proc is not None and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), 10.0)
            except asyncio.TimeoutError:
                self._proc.kill()
                await self._proc.wait()
        try:
            self._log.close()
        except Exception:
            pass
        self._tmp.cleanup()

    async def __aenter__(self) -> Server:
        return await self.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()
