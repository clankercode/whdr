#!/usr/bin/env bash
set -euo pipefail

prefix="/usr/local"
config_dir="/etc/whdr"
state_dir="/var/lib/whdr"
service_dir="/etc/systemd/system"
service_name="whdr"
user="whdr"
group="whdr"
profile="release"
listen_addr="127.0.0.1:8787"
dry_run=0
enable_service=1
start_service=1
build_bins=1
tunnel_provider="none"
tunnel_service_name="whdr-tunnel-cloudflare"
tunnel_config_dir="/etc/cloudflared"
public_host=""
cloudflare_tunnel=""
cloudflare_credentials_file=""
cloudflared_bin="/usr/bin/cloudflared"

usage() {
  cat <<'EOF'
Usage: scripts/install-service.sh [options]

Build and install whdr as a systemd service.

Options:
  --dry-run                 Print the install plan and generated config/unit.
  --prefix DIR              Install binaries under DIR/bin (default: /usr/local).
  --config-dir DIR          whdr config directory (default: /etc/whdr).
  --state-dir DIR           whdr state directory (default: /var/lib/whdr).
  --service-dir DIR         systemd unit directory (default: /etc/systemd/system).
  --user USER               Service user (default: whdr).
  --group GROUP             Service group (default: whdr).
  --listen-addr ADDR        whdr ingest listen address (default: 127.0.0.1:8787).
  --debug                   Install debug-profile binaries from target/debug.
  --skip-build              Do not run cargo build first.
  --no-enable               Do not run systemctl enable.
  --no-start                Do not restart the service after install.
  --tunnel-provider PROVIDER       Optional external tunnel companion: none or cloudflare (default: none).
  --public-host HOST               Public hostname routed to whdr ingest when using a tunnel.
  --cloudflare-tunnel NAME         Cloudflare Tunnel name when --tunnel-provider cloudflare.
  --cloudflare-credentials-file FILE
                               Cloudflare Tunnel credentials JSON file.
  --cloudflared-bin FILE           cloudflared binary path (default: /usr/bin/cloudflared).
  --tunnel-config-dir DIR          Tunnel config directory (default: /etc/cloudflared).
  --tunnel-service-name NAME       Companion systemd service name (default: whdr-tunnel-cloudflare).
  -h, --help                Show this help.

Default layout:
  binaries:     /usr/local/bin
  config:       /etc/whdr/config.toml
  secrets:      /etc/whdr/secrets.toml
  token store:  /var/lib/whdr/tokens.toml
  control UDS:  /run/whdr/ctl.sock
EOF
}

while (($#)); do
  case "$1" in
    --dry-run) dry_run=1 ;;
    --prefix) prefix="${2:?missing value for --prefix}"; shift ;;
    --config-dir) config_dir="${2:?missing value for --config-dir}"; shift ;;
    --state-dir) state_dir="${2:?missing value for --state-dir}"; shift ;;
    --service-dir) service_dir="${2:?missing value for --service-dir}"; shift ;;
    --user) user="${2:?missing value for --user}"; shift ;;
    --group) group="${2:?missing value for --group}"; shift ;;
    --listen-addr) listen_addr="${2:?missing value for --listen-addr}"; shift ;;
    --debug) profile="debug" ;;
    --skip-build) build_bins=0 ;;
    --no-enable) enable_service=0 ;;
    --no-start) start_service=0 ;;
    --tunnel-provider) tunnel_provider="${2:?missing value for --tunnel-provider}"; shift ;;
    --public-host) public_host="${2:?missing value for --public-host}"; shift ;;
    --cloudflare-tunnel) cloudflare_tunnel="${2:?missing value for --cloudflare-tunnel}"; shift ;;
    --cloudflare-credentials-file) cloudflare_credentials_file="${2:?missing value for --cloudflare-credentials-file}"; shift ;;
    --cloudflared-bin) cloudflared_bin="${2:?missing value for --cloudflared-bin}"; shift ;;
    --tunnel-config-dir) tunnel_config_dir="${2:?missing value for --tunnel-config-dir}"; shift ;;
    --tunnel-service-name) tunnel_service_name="${2:?missing value for --tunnel-service-name}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
bindir="${prefix%/}/bin"
target_dir="$repo_root/target/$profile"
config_file="${config_dir%/}/config.toml"
secrets_file="${config_dir%/}/secrets.toml"
unit_file="${service_dir%/}/${service_name}.service"
profile_flag=()
if [[ "$profile" == "release" ]]; then
  profile_flag=(--release)
fi

validate_listen_addr() {
  if [[ -z "$listen_addr" ]]; then
    echo "--listen-addr must not be empty" >&2
    exit 2
  fi
  if [[ "$listen_addr" =~ [[:space:]] ]]; then
    echo "--listen-addr must not contain whitespace: $listen_addr" >&2
    exit 2
  fi
  if [[ "$listen_addr" != *:* ]]; then
    echo "--listen-addr must include a :port part: $listen_addr" >&2
    exit 2
  fi
}

validate_tunnel_options() {
  case "$tunnel_provider" in
    none)
      return 0
      ;;
    cloudflare)
      if [[ -z "$public_host" ]]; then
        echo "--public-host is required when --tunnel-provider cloudflare" >&2
        exit 2
      fi
      if [[ -z "$cloudflare_tunnel" ]]; then
        echo "--cloudflare-tunnel is required when --tunnel-provider cloudflare" >&2
        exit 2
      fi
      if [[ "$cloudflare_tunnel" == */* || "$cloudflare_tunnel" == *..* ]]; then
        echo "--cloudflare-tunnel must be a name, not a path" >&2
        exit 2
      fi
      if [[ -z "$cloudflare_credentials_file" ]]; then
        echo "--cloudflare-credentials-file is required when --tunnel-provider cloudflare" >&2
        exit 2
      fi
      ;;
    *)
      echo "unsupported --tunnel-provider: $tunnel_provider" >&2
      exit 2
      ;;
  esac
}

validate_listen_addr
validate_tunnel_options

cloudflare_config_file="${tunnel_config_dir%/}/${cloudflare_tunnel}.yml"
tunnel_unit_file="${service_dir%/}/${tunnel_service_name}.service"

render_config() {
  cat <<EOF
[server]
listen_addr = "$listen_addr"
sub_addr = "127.0.0.1:8788"
control_socket = "/run/whdr/ctl.sock"

[subscribers]
token_store = "$state_dir/tokens.toml"
allow_plaintext_lan = false
ws_idle_timeout_ms = 30000

[extensions]
enabled = []
autostart_all = false

[limits]
max_body_bytes = 1048576
max_in_flight = 64
sub_queue_len = 1024
dispatch_timeout_ms = 10000

[timeouts]
register_ms = 5000
drain_ms = 5000
term_grace_ms = 3000

[secrets]
file = "$secrets_file"
EOF
}

render_secrets() {
  cat <<'EOF'
# Provider secrets keyed by extension id.
# Keep this file mode 0600.
github = "replace-me"
teams = "replace-me"
EOF
}

render_unit() {
  cat <<EOF
[Unit]
Description=Webhook Dynamic Router
After=network.target

[Service]
Environment=PATH=$bindir:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin
ExecStart=$bindir/whdr-server --config $config_file
Restart=on-failure
User=$user
Group=$group
RuntimeDirectory=whdr
StateDirectory=whdr

[Install]
WantedBy=multi-user.target
EOF
}

render_cloudflare_tunnel_config() {
  cat <<EOF
tunnel: $cloudflare_tunnel
credentials-file: $cloudflare_credentials_file

ingress:
  - hostname: $public_host
    service: http://$listen_addr
  - service: http_status:404
EOF
}

render_cloudflare_tunnel_unit() {
  cat <<EOF
[Unit]
Description=WHDR Cloudflare Tunnel
After=network-online.target $service_name.service
Wants=network-online.target
Requires=$service_name.service

[Service]
ExecStart=$cloudflared_bin tunnel --config $cloudflare_config_file run $cloudflare_tunnel
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
}

plan() {
  cat <<EOF
install whdr-server -> $bindir/whdr-server
install whdr -> $bindir/whdr
install whdr-ext-dev -> $bindir/whdr-ext-dev
install whdr-ext-github -> $bindir/whdr-ext-github
install whdr-ext-teams -> $bindir/whdr-ext-teams
write config -> $config_file
write secrets -> $secrets_file
install systemd unit -> $unit_file
EOF
  if [[ "$tunnel_provider" == "cloudflare" ]]; then
    echo "write cloudflare tunnel config -> $cloudflare_config_file"
    echo "install cloudflare tunnel systemd unit -> $tunnel_unit_file"
  fi
  echo
  echo "# config.toml"
  render_config
  echo
  echo "# ${service_name}.service"
  render_unit
  if [[ "$tunnel_provider" == "cloudflare" ]]; then
    echo
    echo "# $(basename "$cloudflare_config_file")"
    render_cloudflare_tunnel_config
    echo
    echo "# ${tunnel_service_name}.service"
    render_cloudflare_tunnel_unit
  fi
}

if [[ "$dry_run" == "1" ]]; then
  plan
  exit 0
fi

if [[ "${EUID:-$(id -u)}" != "0" ]]; then
  echo "install-service.sh must run as root; try: sudo $0" >&2
  exit 1
fi

if [[ "$build_bins" == "1" ]]; then
  cargo build --workspace --bins "${profile_flag[@]}"
fi

if ! getent group "$group" >/dev/null; then
  groupadd --system "$group"
fi
if ! id -u "$user" >/dev/null 2>&1; then
  useradd --system --no-create-home --shell /usr/sbin/nologin --gid "$group" "$user"
fi

install -d -m 0755 "$bindir"
install -d -m 0755 "$config_dir"
install -d -m 0750 -o "$user" -g "$group" "$state_dir"

for bin in whdr-server whdr whdr-ext-dev whdr-ext-github whdr-ext-teams; do
  install -m 0755 "$target_dir/$bin" "$bindir/$bin"
done

if [[ ! -e "$config_file" ]]; then
  tmp="$(mktemp)"
  render_config > "$tmp"
  install -m 0644 "$tmp" "$config_file"
  rm -f "$tmp"
else
  echo "keeping existing $config_file"
fi

if [[ ! -e "$secrets_file" ]]; then
  tmp="$(mktemp)"
  render_secrets > "$tmp"
  install -m 0600 -o "$user" -g "$group" "$tmp" "$secrets_file"
  rm -f "$tmp"
else
  chmod 0600 "$secrets_file"
  chown "$user:$group" "$secrets_file"
  echo "keeping existing $secrets_file"
fi

tmp="$(mktemp)"
render_unit > "$tmp"
install -m 0644 "$tmp" "$unit_file"
rm -f "$tmp"

if [[ "$tunnel_provider" == "cloudflare" ]]; then
  install -d -m 0755 "$tunnel_config_dir"

  tmp="$(mktemp)"
  render_cloudflare_tunnel_config > "$tmp"
  install -m 0644 "$tmp" "$cloudflare_config_file"
  rm -f "$tmp"

  tmp="$(mktemp)"
  render_cloudflare_tunnel_unit > "$tmp"
  install -m 0644 "$tmp" "$tunnel_unit_file"
  rm -f "$tmp"
fi

systemctl daemon-reload
if [[ "$enable_service" == "1" ]]; then
  systemctl enable "$service_name.service"
  if [[ "$tunnel_provider" == "cloudflare" ]]; then
    systemctl enable "$tunnel_service_name.service"
  fi
fi
if [[ "$start_service" == "1" ]]; then
  systemctl restart "$service_name.service"
  if [[ "$tunnel_provider" == "cloudflare" ]]; then
    systemctl restart "$tunnel_service_name.service"
  fi
fi

cat <<EOF
Installed $service_name.

Config:  $config_file
Secrets: $secrets_file
Status:  systemctl status $service_name.service
Logs:    journalctl -u $service_name.service -f
CLI:     sudo whdr --socket /run/whdr/ctl.sock status

The control socket is owned by $user:$group with mode 0660. Use sudo for one-off
admin commands, or add trusted administrators to the $group group.
EOF
