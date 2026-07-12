#!/usr/bin/env python3
"""Detect Avenor backend executables and lightweight configuration signals."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
from pathlib import Path
from urllib.parse import urlsplit


PREFERENCE = [
    "pi",
    "opencode-acp",
    "codex-app-server",
    "claude",
    "gemini-acp",
    "cursor-acp",
    "claude-channel",
    "opencode-http",
]


def existing(paths: list[Path]) -> list[str]:
    return [str(path) for path in paths if path.exists()]


def load_json(path: Path) -> object | None:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def object_keys(path: Path, nested_key: str | None = None) -> list[str]:
    payload = load_json(path)
    if nested_key and isinstance(payload, dict):
        payload = payload.get(nested_key)
    if isinstance(payload, dict):
        return sorted(str(key) for key in payload)
    if isinstance(payload, list):
        return sorted(
            str(item["name"])
            for item in payload
            if isinstance(item, dict) and item.get("name")
        )
    return []


def pi_details(home: Path) -> tuple[list[str], list[str], bool]:
    agent_dir = Path(os.environ.get("PI_CODING_AGENT_DIR", home / ".pi" / "agent"))
    profiles = object_keys(agent_dir / "agents.json", "agents")
    if not profiles:
        profiles = object_keys(agent_dir / "agents.json")

    settings_paths = [agent_dir / "settings.json", Path.cwd() / ".pi" / "settings.json"]
    extension_detected = any(
        "pi-agents" in path.read_text(errors="ignore")
        for path in settings_paths
        if path.is_file()
    ) or (agent_dir / "extensions" / "pi-agents").exists()

    signals = existing([agent_dir / "auth.json", *settings_paths])
    return profiles, signals, extension_detected


def opencode_details(home: Path) -> tuple[list[str], list[str]]:
    paths: list[Path] = []
    if explicit := os.environ.get("OPENCODE_CONFIG"):
        paths.append(Path(explicit).expanduser())
    if config_dir := os.environ.get("OPENCODE_CONFIG_DIR"):
        base = Path(config_dir).expanduser()
    elif xdg := os.environ.get("XDG_CONFIG_HOME"):
        base = Path(xdg).expanduser() / "opencode"
    else:
        base = home / ".config" / "opencode"
    paths.extend([base / "opencode.json", base / "opencode.jsonc"])

    profiles: set[str] = set()
    for path in paths:
        profiles.update(object_keys(path, "agent"))
    signals = existing(paths + [home / ".local" / "share" / "opencode" / "auth.json"])
    return sorted(profiles), signals


def backend(command: str, signals: list[str], *, profiles: list[str] | None = None) -> dict:
    executable = shutil.which(command)
    if not executable:
        state = "unavailable"
    elif signals:
        state = "configured"
    else:
        state = "unknown"
    return {
        "available": executable is not None,
        "command": executable,
        "configuration": state,
        "configuration_signals": signals,
        "agent_profiles": profiles or [],
    }


def http_backend(server_url: str | None) -> dict:
    result = {
        "available": False,
        "command": None,
        "configuration": "unavailable",
        "configuration_signals": [],
        "agent_profiles": [],
    }
    if not server_url:
        result["note"] = "Supply server_url or AVENOR_OPENCODE_URL."
        return result

    try:
        parsed = urlsplit(server_url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if not host:
            raise ValueError("missing host")
        with socket.create_connection((host, port), timeout=0.5):
            pass
    except (OSError, ValueError):
        result["configuration"] = "unreachable"
        result["note"] = "A server URL was supplied, but its endpoint was not reachable."
        return result

    result["available"] = True
    result["configuration"] = "configured"
    result["configuration_signals"] = ["server_url supplied and endpoint reachable"]
    return result


def detect(server_url: str | None) -> dict:
    home = Path.home()
    pi_profiles, pi_signals, pi_agents = pi_details(home)
    opencode_profiles, opencode_signals = opencode_details(home)

    backends = {
        "pi": backend("pi", pi_signals, profiles=pi_profiles),
        "opencode-acp": backend("opencode", opencode_signals, profiles=opencode_profiles),
        "codex-app-server": backend("codex", existing([home / ".codex" / "auth.json"])),
        "gemini-acp": backend(
            "gemini",
            existing([home / ".gemini" / "oauth_creds.json", home / ".gemini" / "settings.json"]),
        ),
        "cursor-acp": backend("agent", existing([home / ".cursor"])),
        "claude": backend("claude", existing([home / ".claude.json", home / ".claude"])),
        "claude-channel": backend("claude", existing([home / ".claude.json", home / ".claude"])),
        "opencode-http": http_backend(server_url),
    }

    backends["pi"]["pi_agents_extension"] = pi_agents
    tmux = shutil.which("tmux")
    backends["claude-channel"]["tmux"] = tmux
    if backends["claude-channel"]["available"] and not tmux:
        backends["claude-channel"]["available"] = False
        backends["claude-channel"]["configuration"] = "unavailable"
        backends["claude-channel"]["note"] = "claude-channel also requires tmux."

    recommended = next(
        (
            name
            for name in PREFERENCE
            if backends[name]["available"]
            and backends[name]["configuration"] in {"configured", "unknown"}
        ),
        None,
    )
    return {
        "recommended": recommended,
        "preference": PREFERENCE,
        "backends": backends,
        "caveat": "Configuration signals are heuristic; authentication is verified only when a run starts.",
    }


def print_human(payload: dict) -> None:
    for name in payload["preference"]:
        item = payload["backends"][name]
        marker = "yes" if item["available"] else "no"
        suffix = " (recommended)" if name == payload["recommended"] else ""
        print(f"{name:18} available={marker:3} config={item['configuration']}{suffix}")
    print(payload["caveat"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit JSON")
    parser.add_argument(
        "--server-url",
        default=os.environ.get("AVENOR_OPENCODE_URL"),
        help="optional opencode-http endpoint; defaults to AVENOR_OPENCODE_URL",
    )
    args = parser.parse_args()
    payload = detect(args.server_url)
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print_human(payload)


if __name__ == "__main__":
    main()
