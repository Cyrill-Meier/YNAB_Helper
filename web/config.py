"""WebConfig dataclass — single source of truth for web-UI runtime config."""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List


def _parse_bool(val):
    if val is None:
        return False
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _parse_networks(raw):
    if not raw:
        return []
    nets = []
    for piece in str(raw).split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            nets.append(ipaddress.ip_network(piece, strict=False))
        except ValueError:
            # Try as a single host
            try:
                nets.append(ipaddress.ip_network(f"{piece}/32"))
            except ValueError:
                pass
    return nets


@dataclass
class WebConfig:
    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 8080
    public_url: str = ""
    secret_key: str = ""
    login_ttl: int = 300        # seconds — how long a /login URL stays valid
    session_ttl: int = 1800     # seconds — sliding TTL for cookie sessions
    session_absolute_ttl: int = 12 * 3600  # max wall-clock for any session
    allowed_ips: List = field(default_factory=list)  # ip_network objects
    data_dir: Path = field(default_factory=lambda: Path("./bot_data"))

    @classmethod
    def from_env(cls, data_dir):
        cfg = cls(
            enabled=_parse_bool(os.environ.get("WEB_UI_ENABLED", "0")),
            host=os.environ.get("WEB_UI_HOST", "0.0.0.0"),
            port=int(os.environ.get("WEB_UI_PORT", "8080")),
            public_url=os.environ.get("WEB_UI_PUBLIC_URL", "").rstrip("/"),
            secret_key=os.environ.get("WEB_UI_SECRET_KEY", ""),
            login_ttl=int(os.environ.get("WEB_UI_LOGIN_TTL", "300")),
            session_ttl=int(os.environ.get("WEB_UI_SESSION_TTL", "1800")),
            session_absolute_ttl=int(
                os.environ.get("WEB_UI_SESSION_ABS_TTL", str(12 * 3600))
            ),
            allowed_ips=_parse_networks(os.environ.get("WEB_UI_ALLOWED_IPS", "")),
            data_dir=Path(data_dir),
        )
        return cfg

    def validate(self):
        """Return a list of human-readable problems, [] if config is OK."""
        problems = []
        if not self.enabled:
            return problems
        if not self.secret_key or len(self.secret_key) < 32:
            problems.append(
                "WEB_UI_SECRET_KEY is missing or too short (need ≥ 32 chars). "
                "Generate with: python3 -c 'import secrets; "
                "print(secrets.token_urlsafe(48))'"
            )
        if not self.public_url:
            problems.append(
                "WEB_UI_PUBLIC_URL is missing — the bot needs to know what "
                "URL to send users to. Example: "
                "https://your-tunnel-hostname.example.com"
            )
        elif not (self.public_url.startswith("https://")
                  or self.public_url.startswith("http://")):
            problems.append(
                f"WEB_UI_PUBLIC_URL ({self.public_url!r}) must start with "
                f"http:// or https://."
            )
        if self.port < 1 or self.port > 65535:
            problems.append(f"WEB_UI_PORT ({self.port}) is out of range.")
        return problems

    def ip_allowed(self, ip):
        """Whether `ip` (string) is in the allowlist. Empty list = allow all."""
        if not self.allowed_ips:
            return True
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return False
        return any(addr in net for net in self.allowed_ips)

    def login_url(self, token):
        """Build the URL we hand to the user via Telegram."""
        return f"{self.public_url}/auth?t={token}"
