"""Web UI configuration from the ``WEB_UI_*`` variables (docs/DESIGN.md §9.1, §11).

:meth:`WebConfig.from_settings` never raises. A malformed value is reported as a problem
instead (F60: v1 called ``int()`` on ``WEB_UI_SESSION_TTL=30m`` and crash-looped the whole
bot). Any problem keeps the web UI off while the bot keeps running, and the admin learns
why from the startup DM (:func:`web_disabled_reason`).

The IP allowlist fails closed (F59). ``WEB_UI_ALLOWED_IPS`` left empty allows every
address. When it is set, every entry must parse (a network strictly, without host bits,
so a prefix-length typo cannot widen it), and at least one must remain; otherwise the web
UI refuses to start, and :meth:`WebConfig.allows` admits only loopback.

Variables and defaults (§11): ``WEB_UI_ENABLED`` (off), ``WEB_UI_HOST`` (``0.0.0.0``),
``WEB_UI_PORT`` (8080), ``WEB_UI_PUBLIC_URL`` (required), ``WEB_UI_SECRET_KEY`` (required,
at least 32 characters, the HMAC key for tokens and CSRF), ``WEB_UI_LOGIN_TTL`` (300 s),
``WEB_UI_SESSION_TTL`` (1800 s, sliding), ``WEB_UI_SESSION_ABS_TTL`` (43200 s),
``WEB_UI_ALLOWED_IPS`` (all) and ``WEB_UI_TRUSTED_PROXIES`` (``*``: the container is only
reachable through Caddy, so ``X-Forwarded-For`` from any peer is trusted).

This module must not import FastAPI (the bot and the health check import it).
"""

from __future__ import annotations

import ipaddress
import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final
from urllib.parse import quote, urlsplit

from ynab_helper import config
from ynab_helper.sessions import MIN_SECRET_LENGTH, TokenHasher

log = logging.getLogger(__name__)

type IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network
type IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

ENV_ENABLED: Final = "WEB_UI_ENABLED"
ENV_HOST: Final = "WEB_UI_HOST"
ENV_PORT: Final = "WEB_UI_PORT"
ENV_PUBLIC_URL: Final = "WEB_UI_PUBLIC_URL"
ENV_SECRET_KEY: Final = "WEB_UI_SECRET_KEY"
ENV_LOGIN_TTL: Final = "WEB_UI_LOGIN_TTL"
ENV_SESSION_TTL: Final = "WEB_UI_SESSION_TTL"
ENV_SESSION_ABS_TTL: Final = "WEB_UI_SESSION_ABS_TTL"
ENV_ALLOWED_IPS: Final = "WEB_UI_ALLOWED_IPS"
ENV_TRUSTED_PROXIES: Final = "WEB_UI_TRUSTED_PROXIES"

DEFAULT_HOST: Final = "0.0.0.0"
DEFAULT_PORT: Final = 8080
DEFAULT_LOGIN_TTL: Final = 300
DEFAULT_SESSION_TTL: Final = 1800
DEFAULT_SESSION_ABS_TTL: Final = 12 * 3600
DEFAULT_TRUSTED_PROXIES: Final = "*"
#: Upper bound for every TTL: browsers cap cookie lifetimes at 400 days anyway.
MAX_TTL: Final = 400 * 24 * 3600
#: How many problems :func:`web_disabled_reason` spells out before summarising the rest.
MAX_REASON_PROBLEMS: Final = 3

# Problems attributed to these keys are reported even while the web UI is off: a malformed
# WEB_UI_ENABLED is the reason it is off. _UNREADABLE marks a configuration that could not
# be read at all.
_UNREADABLE: Final = "WEB_UI"
_REPORTED_WHEN_DISABLED: Final = frozenset({ENV_ENABLED, _UNREADABLE})

_ALLOWLIST_SEPARATORS: Final = re.compile(r"[,\s]+")
_HOSTNAME_RE: Final = re.compile(
    r"(?=.{1,253}\Z)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*\.?"
)
_NUMERIC_HOST_RE: Final = re.compile(r"[0-9.]+")
_UNPRINTABLE_RE: Final = re.compile(r"[^\x20-\x7e]")
_FALLBACK_SUFFIX_RE: Final = re.compile(r"; using -?[0-9]+\.\Z")
# Values as long as a valid secret key may be one pasted into the wrong variable: such
# values are shown as a short prefix only (the same policy as config._echo).
_MAX_ECHO: Final = MIN_SECRET_LENGTH - 1
_ECHO_PREFIX: Final = 8
_MAX_LISTED: Final = 5
_MAX_IP_TEXT: Final = 64
_NETWORK_TEXT_RE: Final = re.compile(r"[0-9A-Fa-f.:/]{1,64}")


@dataclass(frozen=True, slots=True)
class WebConfig:
    """Web UI runtime configuration (immutable).

    ``parse_problems`` holds ``(variable, message)`` pairs for values that
    :meth:`from_settings` could not use. A value like that has already been replaced by its
    default or dropped, so the problem lives here, and :meth:`validate` reports it.
    """

    enabled: bool = False
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    public_url: str = ""
    secret_key: str = field(default="", repr=False)
    login_ttl: int = DEFAULT_LOGIN_TTL
    session_ttl: int = DEFAULT_SESSION_TTL
    session_absolute_ttl: int = DEFAULT_SESSION_ABS_TTL
    allowed_networks: tuple[IPNetwork, ...] = ()
    trusted_proxies: str = DEFAULT_TRUSTED_PROXIES
    data_dir: Path = config.DEFAULT_DATA_DIR
    parse_problems: tuple[tuple[str, str], ...] = field(default=(), repr=False, compare=False)

    # ── construction ──

    @classmethod
    def from_settings(cls, settings: config.Settings) -> tuple[WebConfig, list[str]]:
        """Read ``WEB_UI_*`` from ``settings.environ``; return the config and its problems.

        Never raises. The returned problems equal :meth:`validate`. They are empty when
        the web UI is cleanly disabled, and non-empty whenever it must not start
        (:attr:`is_usable` is then False). The caller logs them and tells the admin.
        """
        try:
            cfg = cls._from_environ(settings.environ, data_dir=Path(settings.data_dir))
            return cfg, cfg.validate()
        except Exception:  # the "never raises" contract outranks the details (F60)
            log.exception("could not read the web UI configuration")
            cfg = cls(
                enabled=False,
                parse_problems=(
                    (
                        _UNREADABLE,
                        "The web UI configuration could not be read (see the log); "
                        "the web UI stays off.",
                    ),
                ),
            )
            return cfg, cfg.validate()

    @classmethod
    def _from_environ(cls, env: Mapping[str, str], *, data_dir: Path) -> WebConfig:
        problems: list[tuple[str, str]] = []

        raw_enabled = _get(env, ENV_ENABLED)
        enabled = config.parse_bool(raw_enabled)
        if enabled is None:
            problems.append(
                (
                    ENV_ENABLED,
                    f"{ENV_ENABLED} must be one of 1/true/yes/on or 0/false/no/off, got "
                    f"{_echo(raw_enabled)}; the web UI stays off.",
                )
            )
            enabled = False

        port = _int_var(env, ENV_PORT, DEFAULT_PORT, problems, minimum=1, maximum=65535)
        login_ttl = _ttl_var(env, ENV_LOGIN_TTL, DEFAULT_LOGIN_TTL, problems)
        session_ttl = _ttl_var(env, ENV_SESSION_TTL, DEFAULT_SESSION_TTL, problems)
        absolute_ttl = _ttl_var(env, ENV_SESSION_ABS_TTL, DEFAULT_SESSION_ABS_TTL, problems)

        networks, allowlist_problems = parse_allowlist(env.get(ENV_ALLOWED_IPS))
        problems.extend((ENV_ALLOWED_IPS, message) for message in allowlist_problems)

        return cls(
            enabled=enabled,
            host=_get(env, ENV_HOST) or DEFAULT_HOST,
            port=port,
            public_url=_get(env, ENV_PUBLIC_URL).rstrip("/"),
            secret_key=_get(env, ENV_SECRET_KEY),
            login_ttl=login_ttl,
            session_ttl=session_ttl,
            session_absolute_ttl=absolute_ttl,
            allowed_networks=networks,
            trusted_proxies=_normalise_proxies(_get(env, ENV_TRUSTED_PROXIES)),
            data_dir=data_dir,
            parse_problems=tuple(problems),
        )

    # ── validation ──

    def validate(self) -> list[str]:
        """Everything that keeps the web UI from starting (empty list = OK).

        A cleanly disabled web UI has no problems. A malformed ``WEB_UI_ENABLED`` is
        reported even though it leaves the web UI off. When enabled, the checks are: the
        parse problems; ``WEB_UI_PUBLIC_URL`` set, http(s), with a host and without
        query or fragment; ``WEB_UI_SECRET_KEY`` at least 32 characters; a sane host;
        port 1–65535; TTLs between 1 s and 400 days with session TTL <= absolute TTL;
        the allowlist (fail closed, F59); trusted proxies ``*`` or IPs/networks.
        """
        if not self.enabled:
            return [msg for var, msg in self.parse_problems if var in _REPORTED_WHEN_DISABLED]
        problems = [msg for _, msg in self.parse_problems]
        problems.extend(_public_url_problems(self.public_url))
        if not isinstance(self.secret_key, str) or len(self.secret_key) < MIN_SECRET_LENGTH:
            problems.append(
                f"{ENV_SECRET_KEY} is missing or too short (it needs at least "
                f"{MIN_SECRET_LENGTH} characters). Generate one with: "
                "python3 -c 'import secrets; print(secrets.token_urlsafe(48))'"
            )
        problems.extend(_host_problems(self.host))
        if not _int_in(self.port, 1, 65535):
            problems.append(f"{ENV_PORT} must be between 1 and 65535, got {self.port!r}.")
        ttls = (
            (ENV_LOGIN_TTL, self.login_ttl),
            (ENV_SESSION_TTL, self.session_ttl),
            (ENV_SESSION_ABS_TTL, self.session_absolute_ttl),
        )
        bad_ttl = False
        for name, value in ttls:
            if not _int_in(value, 1, MAX_TTL):
                bad_ttl = True
                problems.append(f"{name} must be between 1 and {MAX_TTL} seconds, got {value!r}.")
        if not bad_ttl and self.session_ttl > self.session_absolute_ttl:
            problems.append(
                f"{ENV_SESSION_TTL} ({self.session_ttl}) must not exceed "
                f"{ENV_SESSION_ABS_TTL} ({self.session_absolute_ttl})."
            )
        problems.extend(_trusted_proxy_problems(self.trusted_proxies))
        return problems

    @property
    def is_usable(self) -> bool:
        """Enabled and without problems: only then may the web UI start."""
        return self.enabled and not self.validate()

    @property
    def cookie_secure(self) -> bool:
        """Mark the session cookie ``Secure``: the public URL is https."""
        return self.public_url.startswith("https://")

    @property
    def allowlist_failed(self) -> bool:
        """``WEB_UI_ALLOWED_IPS`` is set but unusable; :meth:`allows` then admits loopback only."""
        return any(var == ENV_ALLOWED_IPS for var, _ in self.parse_problems)

    # ── helpers for the bot and the web ──

    def login_url(self, token: str) -> str:
        """The sign-in link sent by ``/login``: ``{public_url}/auth?t={token}``."""
        return f"{self.public_url}/auth?t={quote(token, safe='')}"

    def allows(self, ip: str | None) -> bool:
        """Whether a client address may use the web UI.

        No allowlist means every address. Loopback (``127.0.0.0/8``, ``::1``, and their
        IPv4-mapped forms) is always allowed, so the container's health check works. An
        IPv4-mapped IPv6 address is matched against IPv4 networks. With an allowlist, an
        unparsable address is refused, and so is every non-loopback address when the
        allowlist itself failed to parse (fail closed, F59).
        """
        if not self.allowed_networks and not self.allowlist_failed:
            return True
        addr = _parse_ip(ip)
        if addr is None:
            return False
        if addr.is_loopback:
            return True
        if self.allowlist_failed:
            return False
        return any(addr in network for network in self.allowed_networks)

    def token_hasher(self) -> TokenHasher:
        """A :class:`~ynab_helper.sessions.TokenHasher` keyed with ``secret_key``.

        Raises :class:`ValueError` if the key is shorter than 32 characters; check
        :attr:`is_usable` first.
        """
        return TokenHasher(self.secret_key)


def web_disabled_reason(cfg: WebConfig, problems: Sequence[str]) -> str | None:
    """Why the web UI is not running, for the admin's startup DM; None if it may start.

    The text names variables and malformed (non-secret) values, never the secret key.
    At most :data:`MAX_REASON_PROBLEMS` problems are spelled out. Empty ``problems`` for
    an enabled config are re-checked with :meth:`WebConfig.validate`, so a stale list
    never makes an unusable config look startable.
    """
    listed = [str(p) for p in problems]
    if cfg.enabled and not listed:
        listed = cfg.validate()
        if not listed:
            return None
    if not listed:
        return f"turned off ({ENV_ENABLED} is not set to 1)"
    text = " ".join(listed[:MAX_REASON_PROBLEMS])
    hidden = len(listed) - MAX_REASON_PROBLEMS
    if hidden > 0:
        text += f" (and {hidden} more problem{'s' if hidden > 1 else ''}, see the log)"
    return f"configuration problem: {text}"


def parse_allowlist(raw: str | None) -> tuple[tuple[IPNetwork, ...], list[str]]:
    """Parse ``WEB_UI_ALLOWED_IPS`` into networks and problems (F59).

    Entries are separated by commas and/or whitespace; each is an IPv4/IPv6 address
    (a single host) or a network in CIDR notation written with its first address.
    Blank or unset means "no allowlist". Any unparsable entry is a problem, and so is a
    value that is set but holds no entry at all.

    A network with host bits set (``203.0.113.7/0``, ``10.0.0.5/8``) is a problem too,
    not silently widened as v1 did: a typo in the prefix length would otherwise turn the
    allowlist into "allow half the internet" without anyone noticing. The problem names
    both readings, the single address and the whole network, so the fix is obvious.
    Entries with problems are not returned.
    """
    text = "" if raw is None else str(raw).strip()
    if not text:
        return (), []
    networks: list[IPNetwork] = []
    invalid: list[str] = []
    widened: list[str] = []
    for entry in _ALLOWLIST_SEPARATORS.split(text):
        if not entry:
            continue
        try:
            networks.append(ipaddress.ip_network(entry))
        except ValueError:
            # strict parsing failed: either host bits are set or it is not a network.
            try:
                network = ipaddress.ip_network(entry, strict=False)
                address = ipaddress.ip_interface(entry).ip
            except ValueError:
                invalid.append(entry)
            else:
                widened.append(
                    f"{_network_echo(entry)} (for one address write "
                    f"{_network_echo(str(address), quote=False)}, for the whole network "
                    f"{_network_echo(str(network), quote=False)})"
                )
    problems: list[str] = []
    if invalid:
        problems.append(
            f"{ENV_ALLOWED_IPS} has entries that are not IP addresses or networks: "
            f"{_list_echo(invalid)}. Separate entries with commas, e.g. "
            "203.0.113.7,198.51.100.0/24. The web UI stays off until this is fixed."
        )
    if widened:
        problems.append(
            f"{ENV_ALLOWED_IPS} has networks with host bits set, which would widen them "
            f"silently: {_join_listed(widened)}. The web UI stays off until this is fixed."
        )
    if not invalid and not widened and not networks:
        problems.append(
            f"{ENV_ALLOWED_IPS} is set but lists no address. Leave it empty to allow every "
            "address. The web UI stays off until this is fixed."
        )
    return tuple(networks), problems


# ── private helpers ───────────────────────────────────────────────────────────


def _get(env: Mapping[str, str], key: str) -> str:
    value = env.get(key)
    return "" if value is None else str(value).strip()


def _int_var(
    env: Mapping[str, str],
    name: str,
    default: int,
    problems: list[tuple[str, str]],
    *,
    minimum: int,
    maximum: int,
) -> int:
    found: list[str] = []
    value = config.parse_int(
        env.get(name),
        name=name,
        default=default,
        problems=found,
        minimum=minimum,
        maximum=maximum,
    )
    # parse_int says "...; using <default>." — the web UI does not start with a fallback.
    problems.extend((name, _FALLBACK_SUFFIX_RE.sub(".", message)) for message in found)
    return value


def _ttl_var(
    env: Mapping[str, str], name: str, default: int, problems: list[tuple[str, str]]
) -> int:
    return _int_var(env, name, default, problems, minimum=1, maximum=MAX_TTL)


def _int_in(value: object, minimum: int, maximum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and minimum <= value <= maximum


def _public_url_problems(url: object) -> list[str]:
    if not url:
        return [
            f"{ENV_PUBLIC_URL} is not set; the bot needs it to build login links "
            "(e.g. https://bot.example.com)."
        ]
    bad = [
        f"{ENV_PUBLIC_URL} must be an http:// or https:// URL with a host name and without "
        f"query or fragment, got {_echo(str(url))}."
    ]
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        return bad
    if _UNPRINTABLE_RE.search(url) or " " in url or "?" in url or "#" in url:
        return bad
    try:
        parts = urlsplit(url)
        _ = parts.port  # raises ValueError for a malformed or out-of-range port
    except ValueError:
        return bad
    if not parts.hostname or "@" in parts.netloc:
        return bad
    return []


def _host_problems(host: object) -> list[str]:
    if isinstance(host, str) and host:
        try:
            ipaddress.ip_address(host)
            return []
        except ValueError:
            # All-numeric names that are not valid IPs (e.g. 10.0.0.300) are typos.
            if _HOSTNAME_RE.fullmatch(host) and not _NUMERIC_HOST_RE.fullmatch(host):
                return []
    shown = _echo(host) if isinstance(host, str) else repr(type(host).__name__)
    return [f"{ENV_HOST} must be an IP address or host name, got {shown}."]


def _normalise_proxies(raw: str) -> str:
    """Blank → ``*``; otherwise the entries with surrounding whitespace removed."""
    if not raw:
        return DEFAULT_TRUSTED_PROXIES
    entries = [entry.strip() for entry in raw.split(",")]
    return ",".join(entries) if all(entries) else raw


def _trusted_proxy_problems(value: object) -> list[str]:
    """uvicorn's ``forwarded_allow_ips`` silently treats anything it cannot parse as a
    literal that never matches; report such entries instead."""
    if value == "*":
        return []
    if not isinstance(value, str):
        return [f"{ENV_TRUSTED_PROXIES} must be a string, got {type(value).__name__}."]
    invalid: list[str] = []
    for entry in (part.strip() for part in value.split(",")):
        try:
            if "/" in entry:
                ipaddress.ip_network(entry)  # strict, as uvicorn parses it
            else:
                ipaddress.ip_address(entry)
        except ValueError:
            invalid.append(entry)
    if not invalid:
        return []
    return [
        f"{ENV_TRUSTED_PROXIES} must be * or a comma-separated list of IP addresses and "
        f"networks; invalid: {_list_echo(invalid)}."
    ]


def _parse_ip(ip: object) -> IPAddress | None:
    if not isinstance(ip, str) or not ip or len(ip) > _MAX_IP_TEXT:
        return None
    try:
        addr = ipaddress.ip_address(ip.strip())
    except ValueError:
        return None
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        return addr.ipv4_mapped
    return addr


def _echo(value: str) -> str:
    """Quote a value for a problem message: printable ASCII only, and a long value is cut
    to a short prefix (in case a secret was pasted into the wrong variable)."""
    shown = _UNPRINTABLE_RE.sub("?", value)
    if len(shown) > _MAX_ECHO:
        return f"{shown[:_ECHO_PREFIX]!r}… ({len(value)} characters)"
    return repr(shown)


def _network_echo(value: str, *, quote: bool = True) -> str:
    """Show text that parsed as an IP network in full: it cannot be a pasted secret.

    Anything outside the plain address alphabet (e.g. an IPv6 scope id) falls back to
    :func:`_echo`, which may shorten it.
    """
    if _NETWORK_TEXT_RE.fullmatch(value):
        return repr(value) if quote else value
    return _echo(value)


def _join_listed(shown: Sequence[str]) -> str:
    text = ", ".join(shown[:_MAX_LISTED])
    extra = len(shown) - _MAX_LISTED
    return f"{text} and {extra} more" if extra > 0 else text


def _list_echo(values: Sequence[str]) -> str:
    return _join_listed([_echo(v) for v in values])
