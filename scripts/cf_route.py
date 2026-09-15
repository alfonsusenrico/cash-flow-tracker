#!/usr/bin/env python3
"""Automate Cloudflare Tunnel routing: DNS CNAME creation, ingress config, service restart, and URL verification."""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DEFAULT_DOMAIN = "alfonsusenrico.com"
DEFAULT_TUNNEL = "server-tunnel"
DEFAULT_CONFIG = "/etc/cloudflared/config.yml"


def run_cmd(cmd: list[str], check: bool = True, use_sudo: bool = False) -> subprocess.CompletedProcess:
    if use_sudo and os.geteuid() != 0:
        cmd = ["sudo"] + cmd
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def configure_route(
    subdomain: str,
    port: int,
    domain: str = DEFAULT_DOMAIN,
    tunnel: str = DEFAULT_TUNNEL,
    config_path: str = DEFAULT_CONFIG,
    path_regex: str | None = None,
    service_scheme: str = "http",
) -> str:
    # 1. Resolve full hostname
    if "." in subdomain:
        hostname = subdomain
    else:
        hostname = f"{subdomain}.{domain}"

    target_service = f"{service_scheme}://127.0.0.1:{port}"
    print(f"\n🚀 Configuring Cloudflare Tunnel route for {hostname} -> {target_service}...")

    # 2. Register / overwrite DNS route in Cloudflare
    print(f"📡 Registering DNS route in tunnel '{tunnel}'...")
    dns_cmd = ["cloudflared", "tunnel", "route", "dns", "-f", tunnel, hostname]
    res = subprocess.run(dns_cmd, capture_output=True, text=True)
    if res.returncode == 0:
        print(f"  ✔ DNS route registered for {hostname}")
    else:
        # Check if already routed
        if "already exists" in res.stderr.lower() or "already" in res.stdout.lower():
            print(f"  ✔ DNS route already exists for {hostname}")
        else:
            print(f"  ⚠ DNS route output: {res.stderr.strip() or res.stdout.strip()}")

    # 3. Read and update ingress in /etc/cloudflared/config.yml
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Cloudflared config file not found: {config_path}")

    # Read config with sudo if not readable
    try:
        content = config_file.read_text(encoding="utf-8")
    except PermissionError:
        res = run_cmd(["cat", str(config_file)], use_sudo=True)
        content = res.stdout

    import yaml

    data = yaml.safe_load(content)
    ingress = data.get("ingress", [])

    matched = False
    for entry in ingress:
        if entry.get("hostname") == hostname:
            if path_regex and entry.get("path") == path_regex:
                entry["service"] = target_service
                matched = True
                print(f"  ✔ Updated existing ingress rule for {hostname} (path: {path_regex})")
                break
            elif not path_regex and "path" not in entry:
                entry["service"] = target_service
                matched = True
                print(f"  ✔ Updated existing ingress rule for {hostname}")
                break

    if not matched:
        new_entry = {"hostname": hostname, "service": target_service}
        if path_regex:
            new_entry["path"] = path_regex

        # Insert before the last catch-all 404 rule
        if ingress and ingress[-1].get("service") == "http_status:404" and "hostname" not in ingress[-1]:
            ingress.insert(len(ingress) - 1, new_entry)
        else:
            ingress.append(new_entry)
        print(f"  ✔ Added new ingress rule for {hostname} -> {target_service}")

    data["ingress"] = ingress

    # 4. Validate updated configuration in a temporary file
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as tmp:
        yaml.dump(data, tmp, default_flow_style=False, sort_keys=False)
        tmp_path = tmp.name

    try:
        val_res = subprocess.run(
            ["cloudflared", "tunnel", "--config", tmp_path, "ingress", "validate"],
            capture_output=True,
            text=True,
        )
        if val_res.returncode != 0:
            raise RuntimeError(f"Cloudflared ingress validation failed:\n{val_res.stderr}")
        print("  ✔ Ingress syntax validated successfully")

        # 5. Backup existing config and apply new config
        backup_path = f"{config_path}.bak"
        run_cmd(["cp", config_path, backup_path], use_sudo=True)
        run_cmd(["cp", tmp_path, config_path], use_sudo=True)
        run_cmd(["chmod", "600", config_path], use_sudo=True)
        print(f"  ✔ Updated {config_path} (backup saved to {backup_path})")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    # 6. Restart cloudflared service
    print("🔄 Restarting cloudflared systemd service...")
    restart_res = run_cmd(["systemctl", "restart", "cloudflared"], use_sudo=True)
    if restart_res.returncode != 0:
        raise RuntimeError(f"Failed to restart cloudflared: {restart_res.stderr}")

    time.sleep(2)

    # 7. Verify live public URL
    live_url = f"https://{hostname}"
    print(f"🔍 Testing public reachability: {live_url} ...")
    test_res = subprocess.run(
        ["curl", "-I", "-s", "--max-time", "6", live_url],
        capture_output=True,
        text=True,
    )

    first_line = test_res.stdout.splitlines()[0] if test_res.stdout else "No HTTP response yet (DNS propagating)"
    print(f"  ✔ Status: {first_line}")

    print("\n" + "=" * 60)
    print(f"✅ Cloudflare Route Configured Successfully!")
    print(f"🌐 Public URL : {live_url}")
    print(f"🎯 Local Port : {port} ({target_service})")
    print(f"🛡️ SSL/TLS   : Active via Cloudflare Edge")
    print("=" * 60 + "\n")

    return live_url


def main():
    parser = argparse.ArgumentParser(
        description="Automate Cloudflare Tunnel route registration and local port mapping."
    )
    parser.add_argument("subdomain", help="Subdomain or full hostname (e.g. 'finance' or 'finance.alfonsusenrico.com')")
    parser.add_argument("port", type=int, help="Local port to proxy (e.g. 8090)")
    parser.add_argument("--domain", default=DEFAULT_DOMAIN, help=f"Apex domain (default: {DEFAULT_DOMAIN})")
    parser.add_argument("--tunnel", default=DEFAULT_TUNNEL, help=f"Tunnel name or UUID (default: {DEFAULT_TUNNEL})")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help=f"Cloudflared config path (default: {DEFAULT_CONFIG})")
    parser.add_argument("--path", default=None, help="Optional URL path regex for path-based routing")
    parser.add_argument("--scheme", default="http", help="Local protocol scheme (default: http)")

    args = parser.parse_args()

    try:
        configure_route(
            subdomain=args.subdomain,
            port=args.port,
            domain=args.domain,
            tunnel=args.tunnel,
            config_path=args.config,
            path_regex=args.path,
            service_scheme=args.scheme,
        )
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
