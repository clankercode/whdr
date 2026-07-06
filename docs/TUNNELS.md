# WHDR Tunnel Companions

WHDR is tunnel agnostic. `whdr-server` accepts HTTP ingest on its configured local listener and does not start, stop, discover, or configure tunnel processes.

The supported deployment model is:

1. WHDR listens on `127.0.0.1:8787`. The installer derives the tunnel ingress target from the configured listen address (`--listen-addr`), so the two never drift apart.
2. A separate tunnel or reverse proxy terminates public HTTPS.
3. The tunnel forwards provider webhook traffic to `http://127.0.0.1:8787`.
4. WHDR subscriber WebSocket and admin UDS surfaces remain private.

## Cloudflare Tunnel

Create a Cloudflare Tunnel and credentials with `cloudflared`:

```bash
cloudflared tunnel login
cloudflared tunnel create whdr-hooks
cloudflared tunnel route dns whdr-hooks hooks.example.com
```

Install WHDR and render the companion service:

```bash
sudo scripts/install-service.sh \
  --tunnel-provider cloudflare \
  --public-host hooks.example.com \
  --cloudflare-tunnel whdr-hooks \
  --cloudflare-credentials-file /etc/cloudflared/whdr-hooks.json
```

The generated tunnel sends public traffic for `hooks.example.com` to WHDR ingest:

```yaml
tunnel: whdr-hooks
credentials-file: /etc/cloudflared/whdr-hooks.json

ingress:
  - hostname: hooks.example.com
    service: http://127.0.0.1:8787
  - service: http_status:404
```

Check both services:

```bash
systemctl status whdr.service
systemctl status whdr-tunnel-cloudflare.service
journalctl -u whdr.service -f
journalctl -u whdr-tunnel-cloudflare.service -f
```

## Security Boundary

Only expose the ingest listener through a public tunnel. Do not expose the subscriber listener or the control socket through a public tunnel.

Provider signatures are still verified by extensions against the exact raw request body delivered through WHDR. Tunnel termination does not replace provider signature checks.

## `whdr-tunnel-*` Helper Convention

Future tunnel helpers may use binary names like `whdr-tunnel-cloudflare` or `whdr-tunnel-ngrok`. These helpers are installer and operator tools, not `whdr-ext-*` runtime extensions.

Recommended helper commands:

```text
whdr-tunnel-<provider> plan --ingest-url http://127.0.0.1:8787 --public-host HOST
whdr-tunnel-<provider> install --ingest-url http://127.0.0.1:8787 --public-host HOST
```

WHDR core must not scan for, spawn, supervise, or communicate with `whdr-tunnel-*` helpers.
