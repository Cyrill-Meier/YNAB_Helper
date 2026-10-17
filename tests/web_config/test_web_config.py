"""ynab_helper.web.config: WEB_UI_* parsing and validation (DESIGN §9.1, §11; F59, F60)."""

from __future__ import annotations

import dataclasses
import ipaddress
import random
import subprocess
import sys
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

from ynab_helper import config
from ynab_helper.sessions import MIN_SECRET_LENGTH, TokenHasher, new_token
from ynab_helper.web.config import (
    DEFAULT_SESSION_ABS_TTL,
    MAX_TTL,
    WebConfig,
    parse_allowlist,
    web_disabled_reason,
)

SECRET = "w" * 20 + "-web-secret-0123456789"  # 42 characters
BASE = {
    "WEB_UI_ENABLED": "1",
    "WEB_UI_PUBLIC_URL": "https://bot.example.com",
    "WEB_UI_SECRET_KEY": SECRET,
}
BOT_ENV = {"TELEGRAM_BOT_TOKEN": "123:abc", "TELEGRAM_ADMIN_ID": "59213669"}
INT_VARS = {
    "WEB_UI_PORT": 8080,
    "WEB_UI_LOGIN_TTL": 300,
    "WEB_UI_SESSION_TTL": 1800,
    "WEB_UI_SESSION_ABS_TTL": 43200,
}


def _load(env: dict[str, str] | None = None, **extra: str) -> tuple[WebConfig, list[str]]:
    merged = {**BASE, **(env or {}), **extra}
    return WebConfig.from_settings(config.Settings.from_env(merged))


def _load_raw(env: dict[str, str]) -> tuple[WebConfig, list[str]]:
    return WebConfig.from_settings(config.Settings.from_env(env))


# ── defaults and full parsing ─────────────────────────────────────────────────


def test_defaults_match_design_section_11() -> None:
    cfg, problems = _load_raw({})
    assert problems == []
    assert cfg.enabled is False
    assert (cfg.host, cfg.port) == ("0.0.0.0", 8080)
    assert (cfg.login_ttl, cfg.session_ttl, cfg.session_absolute_ttl) == (300, 1800, 43200)
    assert cfg.trusted_proxies == "*"
    assert cfg.allowed_networks == ()
    assert (cfg.public_url, cfg.secret_key) == ("", "")
    assert cfg.data_dir == config.DEFAULT_DATA_DIR
    assert cfg.is_usable is False
    assert cfg.cookie_secure is False
    assert WebConfig() == cfg  # dataclass defaults agree with the parser


def test_every_variable_is_parsed(tmp_path: Path) -> None:
    cfg, problems = _load_raw(
        {
            "WEB_UI_ENABLED": "yes",
            "WEB_UI_HOST": " 127.0.0.1 ",
            "WEB_UI_PORT": " 9090 ",
            "WEB_UI_PUBLIC_URL": "https://bot.netcope.ch/",
            "WEB_UI_SECRET_KEY": f"  {SECRET}  ",
            "WEB_UI_LOGIN_TTL": "120",
            "WEB_UI_SESSION_TTL": "600",
            "WEB_UI_SESSION_ABS_TTL": "7200",
            "WEB_UI_ALLOWED_IPS": "203.0.113.7, 198.51.100.0/24,2001:db8::/32",
            "WEB_UI_TRUSTED_PROXIES": "172.18.0.0/16, 10.0.0.2",
            "BOT_DATA_DIR": str(tmp_path),
        }
    )
    assert problems == []
    assert cfg.enabled is True and cfg.is_usable is True
    assert (cfg.host, cfg.port) == ("127.0.0.1", 9090)
    assert cfg.public_url == "https://bot.netcope.ch"
    assert cfg.secret_key == SECRET
    assert (cfg.login_ttl, cfg.session_ttl, cfg.session_absolute_ttl) == (120, 600, 7200)
    assert cfg.allowed_networks == (
        ipaddress.ip_network("203.0.113.7/32"),
        ipaddress.ip_network("198.51.100.0/24"),
        ipaddress.ip_network("2001:db8::/32"),
    )
    assert cfg.trusted_proxies == "172.18.0.0/16,10.0.0.2"
    assert cfg.data_dir == tmp_path
    assert cfg.cookie_secure is True
    assert web_disabled_reason(cfg, problems) is None


def test_production_bot_env_shape_is_usable() -> None:
    """Production sets only WEB_UI_ENABLED, WEB_UI_PUBLIC_URL and WEB_UI_SECRET_KEY (§2)."""
    cfg, problems = _load_raw({**BOT_ENV, **BASE, "WEB_UI_PUBLIC_URL": "https://bot.netcope.ch"})
    assert problems == [] and cfg.is_usable
    assert (cfg.host, cfg.port) == ("0.0.0.0", 8080)
    assert cfg.login_url("abc") == "https://bot.netcope.ch/auth?t=abc"


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "Yes", "on", " on "])
def test_enabled_truthy_values(raw: str) -> None:
    cfg, problems = _load(WEB_UI_ENABLED=raw)
    assert cfg.enabled is True and problems == []


@pytest.mark.parametrize("raw", ["0", "false", "no", "off", "", "  "])
def test_enabled_falsy_values(raw: str) -> None:
    cfg, problems = _load(WEB_UI_ENABLED=raw)
    assert cfg.enabled is False and problems == []
    assert not cfg.is_usable


@pytest.mark.parametrize("raw", ["maybe", "2", "enabled", "y"])
def test_malformed_enabled_is_reported_and_keeps_the_web_off(raw: str) -> None:
    cfg, problems = _load(WEB_UI_ENABLED=raw)
    assert cfg.enabled is False and not cfg.is_usable
    assert len(problems) == 1 and problems[0].startswith("WEB_UI_ENABLED ")
    assert cfg.validate() == problems
    reason = web_disabled_reason(cfg, problems)
    assert reason is not None and "WEB_UI_ENABLED" in reason


def test_problems_of_a_disabled_web_ui_are_not_reported() -> None:
    cfg, problems = _load_raw(
        {
            "WEB_UI_ENABLED": "0",
            "WEB_UI_PORT": "eighty",
            "WEB_UI_SESSION_TTL": "30m",
            "WEB_UI_ALLOWED_IPS": "garbage",
            "WEB_UI_PUBLIC_URL": "ftp://nope",
        }
    )
    assert problems == [] and cfg.validate() == []
    assert web_disabled_reason(cfg, problems) == "turned off (WEB_UI_ENABLED is not set to 1)"


# ── F60: malformed integers never crash anything ──────────────────────────────

_MALFORMED_INTS = ["30m", "abc", "1.5", "1e3", "0x10", "1_000", "٣", "-", "9" * 30, "\x00"]


@pytest.mark.parametrize("name", list(INT_VARS))
@pytest.mark.parametrize("raw", _MALFORMED_INTS, ids=lambda v: repr(v)[:12])
def test_F60_malformed_integer_disables_the_web_ui_but_not_the_bot(name: str, raw: str) -> None:
    """F60: v1 called int('30m') at startup; the ValueError crash-looped the whole bot."""
    settings = config.Settings.from_env({**BOT_ENV, **BASE, name: raw})
    cfg, problems = WebConfig.from_settings(settings)  # must not raise
    assert settings.validate_for_bot() == []  # the bot still starts
    assert not cfg.is_usable
    assert any(p.startswith(f"{name} ") for p in problems)
    assert getattr(cfg, _field_for(name)) == INT_VARS[name]  # default kept, never used
    assert all("using" not in p for p in problems)  # the web does not "use" a fallback
    reason = web_disabled_reason(cfg, problems)
    assert reason is not None and name in reason


@pytest.mark.parametrize(
    ("name", "raw"),
    [
        ("WEB_UI_PORT", "0"),
        ("WEB_UI_PORT", "65536"),
        ("WEB_UI_PORT", "-80"),
        ("WEB_UI_LOGIN_TTL", "0"),
        ("WEB_UI_SESSION_TTL", "-1"),
        ("WEB_UI_SESSION_ABS_TTL", str(MAX_TTL + 1)),
    ],
)
def test_out_of_range_integers_are_problems(name: str, raw: str) -> None:
    cfg, problems = _load({name: raw})
    assert not cfg.is_usable
    assert len(problems) == 1 and problems[0].startswith(f"{name} must be between")


@pytest.mark.parametrize("name", list(INT_VARS))
def test_blank_integers_use_the_default(name: str) -> None:
    cfg, problems = _load({name: "  "})
    assert problems == []
    assert getattr(cfg, _field_for(name)) == INT_VARS[name]


def _field_for(name: str) -> str:
    return {
        "WEB_UI_PORT": "port",
        "WEB_UI_LOGIN_TTL": "login_ttl",
        "WEB_UI_SESSION_TTL": "session_ttl",
        "WEB_UI_SESSION_ABS_TTL": "session_absolute_ttl",
    }[name]


def test_port_boundaries() -> None:
    assert _load(WEB_UI_PORT="1")[0].is_usable
    assert _load(WEB_UI_PORT="65535")[0].is_usable


def test_session_ttl_must_not_exceed_absolute_ttl() -> None:
    cfg, problems = _load(WEB_UI_SESSION_TTL="50000")
    assert problems == [
        "WEB_UI_SESSION_TTL (50000) must not exceed WEB_UI_SESSION_ABS_TTL (43200)."
    ]
    assert not cfg.is_usable
    equal, problems = _load(WEB_UI_SESSION_TTL="3600", WEB_UI_SESSION_ABS_TTL="3600")
    assert problems == [] and equal.is_usable


def test_F60_randomized_web_environment_never_raises() -> None:
    rng = random.Random(20261017)
    alphabet = "0123456789-+_.:/,; abcxyz*\t\x00é∞\n#'\"=~[]%"
    names = [
        "WEB_UI_ENABLED",
        "WEB_UI_HOST",
        "WEB_UI_PORT",
        "WEB_UI_PUBLIC_URL",
        "WEB_UI_SECRET_KEY",
        "WEB_UI_LOGIN_TTL",
        "WEB_UI_SESSION_TTL",
        "WEB_UI_SESSION_ABS_TTL",
        "WEB_UI_ALLOWED_IPS",
        "WEB_UI_TRUSTED_PROXIES",
    ]
    for _ in range(400):
        env = {
            name: "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 24)))
            for name in names
            if rng.random() < 0.8
        }
        env.setdefault("WEB_UI_ENABLED", rng.choice(["1", "0", "x"]))
        cfg, problems = WebConfig.from_settings(config.Settings.from_env(env))
        assert cfg.is_usable == (cfg.enabled and not problems)
        assert problems == cfg.validate()
        web_disabled_reason(cfg, problems)
        cfg.allows("203.0.113.9")
        cfg.allows("127.0.0.1")


def test_from_settings_never_raises_even_on_a_broken_settings_object(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class Exploding(dict[str, str]):
        def get(self, key: str, default: Any = None) -> Any:
            raise RuntimeError("boom")

    broken = dataclasses.replace(config.Settings.from_env({}), environ=Exploding(BASE))
    cfg, problems = WebConfig.from_settings(broken)
    assert cfg.enabled is False and not cfg.is_usable
    assert len(problems) == 1 and "could not be read" in problems[0]
    assert "could not read the web UI configuration" in caplog.text


# ── public URL, secret, host, trusted proxies ─────────────────────────────────


def test_public_url_required() -> None:
    cfg, problems = _load(WEB_UI_PUBLIC_URL="")
    assert len(problems) == 1 and problems[0].startswith("WEB_UI_PUBLIC_URL is not set")
    assert not cfg.is_usable


@pytest.mark.parametrize(
    "url",
    [
        "bot.example.com",
        "ftp://bot.example.com",
        "https://",
        "https:///path",
        "https://bot.example.com/?x=1",
        "https://bot.example.com?",
        "https://bot.example.com/#frag",
        "https://bot example.com",
        "https://bot.example.com:99999",
        "https://bot.example.com:port",
        "https://user:pw@bot.example.com",
        "https://bot.example.com/\x00",
        "javascript:alert(1)",
        "HTTPS://bot.example.com",
    ],
)
def test_invalid_public_urls(url: str) -> None:
    cfg, problems = _load(WEB_UI_PUBLIC_URL=url)
    assert len(problems) == 1 and problems[0].startswith("WEB_UI_PUBLIC_URL must be")
    assert not cfg.is_usable


@pytest.mark.parametrize(
    ("raw", "stripped", "secure"),
    [
        ("https://bot.example.com/", "https://bot.example.com", True),
        ("https://bot.example.com///", "https://bot.example.com", True),
        ("http://localhost:8080", "http://localhost:8080", False),
        ("https://example.com/ynab/", "https://example.com/ynab", True),
        ("http://[::1]:8080/", "http://[::1]:8080", False),
    ],
)
def test_public_url_trailing_slash_and_cookie_secure(raw: str, stripped: str, secure: bool) -> None:
    cfg, problems = _load(WEB_UI_PUBLIC_URL=raw)
    assert problems == []
    assert cfg.public_url == stripped
    assert cfg.cookie_secure is secure
    assert cfg.login_url("tok") == f"{stripped}/auth?t=tok"


def test_login_url_with_a_real_token() -> None:
    cfg, _ = _load()
    token = new_token()
    assert cfg.login_url(token) == f"https://bot.example.com/auth?t={token}"
    # Defensive: anything that is not URL-safe is percent-encoded.
    assert cfg.login_url("a b&c=d") == "https://bot.example.com/auth?t=a%20b%26c%3Dd"


@pytest.mark.parametrize("secret", ["", "x" * (MIN_SECRET_LENGTH - 1), " " * 40])
def test_secret_key_too_short(secret: str) -> None:
    cfg, problems = _load(WEB_UI_SECRET_KEY=secret)
    assert len(problems) == 1 and problems[0].startswith("WEB_UI_SECRET_KEY is missing or too")
    assert not cfg.is_usable


def test_secret_key_of_exactly_32_characters_and_the_hasher() -> None:
    key = "q" * MIN_SECRET_LENGTH
    cfg, problems = _load(WEB_UI_SECRET_KEY=key)
    assert problems == [] and cfg.is_usable
    token = new_token()
    assert cfg.token_hasher().hash(token) == TokenHasher(key).hash(token)


def test_short_secret_hasher_raises() -> None:
    cfg, _ = _load(WEB_UI_SECRET_KEY="short")
    with pytest.raises(ValueError):
        cfg.token_hasher()


def test_secret_never_appears_in_repr_problems_or_reason() -> None:
    secret = "S3cr3t-value-that-must-stay-hidden-123"
    cfg, problems = _load_raw(
        {
            "WEB_UI_ENABLED": "1",
            "WEB_UI_SECRET_KEY": secret,
            # The same secret pasted into the wrong variables is cut to a short prefix.
            "WEB_UI_PUBLIC_URL": secret,
            "WEB_UI_PORT": secret,
            "WEB_UI_ALLOWED_IPS": secret,
            "WEB_UI_HOST": secret + "!",
        }
    )
    assert problems
    text = "\n".join(problems) + str(web_disabled_reason(cfg, problems))
    assert secret not in text
    only_key, _ = _load_raw({"WEB_UI_ENABLED": "1", "WEB_UI_SECRET_KEY": secret})
    assert secret not in repr(only_key)


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "127.0.0.1", "localhost", "web.internal"])
def test_valid_hosts(host: str) -> None:
    cfg, problems = _load(WEB_UI_HOST=host)
    assert problems == [] and cfg.host == host


@pytest.mark.parametrize("host", ["bad host", "host_name", "10.0.0.300", "-lead.example", "é.com"])
def test_invalid_hosts(host: str) -> None:
    cfg, problems = _load(WEB_UI_HOST=host)
    assert len(problems) == 1 and problems[0].startswith("WEB_UI_HOST must be")
    assert not cfg.is_usable


def test_blank_host_uses_the_default() -> None:
    cfg, problems = _load(WEB_UI_HOST="  ")
    assert problems == [] and cfg.host == "0.0.0.0"


@pytest.mark.parametrize(
    ("raw", "normalised"),
    [
        ("", "*"),
        ("*", "*"),
        ("127.0.0.1", "127.0.0.1"),
        (" 172.18.0.0/16 , ::1 ", "172.18.0.0/16,::1"),
    ],
)
def test_trusted_proxies(raw: str, normalised: str) -> None:
    cfg, problems = _load(WEB_UI_TRUSTED_PROXIES=raw)
    assert problems == [] and cfg.trusted_proxies == normalised


@pytest.mark.parametrize("raw", ["caddy", "*,10.0.0.1", "10.0.0.1,,10.0.0.2", "10.0.0.5/8"])
def test_invalid_trusted_proxies(raw: str) -> None:
    """uvicorn would silently treat these as literals that never match."""
    cfg, problems = _load(WEB_UI_TRUSTED_PROXIES=raw)
    assert len(problems) == 1 and problems[0].startswith("WEB_UI_TRUSTED_PROXIES must be")
    assert not cfg.is_usable


# ── F59: the IP allowlist fails closed ────────────────────────────────────────


@pytest.mark.parametrize(
    "raw",
    [
        "203.0.113.700",  # v1 dropped it silently and allowed everyone
        "10.0.0.0/8;192.168.0.0/16",
        "10.0.0.0/8, bogus",
        "10.0.0.0/33",
        "localhost",
        "*",
    ],
)
def test_F59_unparsable_allowlist_entries_disable_the_web_ui(raw: str) -> None:
    cfg, problems = _load(WEB_UI_ALLOWED_IPS=raw)
    assert not cfg.is_usable
    assert len(problems) == 1 and problems[0].startswith("WEB_UI_ALLOWED_IPS has entries")
    assert cfg.allowlist_failed
    # Even a caller that ignores is_usable gets a closed door (loopback excepted).
    assert not cfg.allows("6.6.6.6")
    assert not cfg.allows("10.1.2.3")
    assert cfg.allows("127.0.0.1")


@pytest.mark.parametrize("raw", [",", " , ;", ",,,"])
def test_F59_allowlist_set_but_empty_after_parsing_fails_closed(raw: str) -> None:
    networks, problems = parse_allowlist(raw)
    if ";" in raw:
        assert problems and problems[0].startswith("WEB_UI_ALLOWED_IPS has entries")
    else:
        assert networks == ()
        assert problems == [
            "WEB_UI_ALLOWED_IPS is set but lists no address. Leave it empty to allow every "
            "address. The web UI stays off until this is fixed."
        ]
    cfg, cfg_problems = _load(WEB_UI_ALLOWED_IPS=raw)
    assert cfg_problems == problems
    assert not cfg.is_usable
    assert not cfg.allows("6.6.6.6")


def test_F59_space_separated_allowlist_is_understood() -> None:
    """v1 split on commas only, so '203.0.113.7 198.51.100.0/24' became 'allow all'."""
    cfg, problems = _load(WEB_UI_ALLOWED_IPS="203.0.113.7 198.51.100.0/24")
    assert problems == []
    assert cfg.allowed_networks == (
        ipaddress.ip_network("203.0.113.7/32"),
        ipaddress.ip_network("198.51.100.0/24"),
    )
    assert cfg.allows("203.0.113.7")
    assert cfg.allows("198.51.100.200")
    assert not cfg.allows("6.6.6.6")


def test_F59_many_bad_entries_are_summarised() -> None:
    raw = ",".join(f"bad{i}" for i in range(12))
    _, problems = parse_allowlist(raw)
    assert len(problems) == 1
    assert "and 7 more" in problems[0]


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_unset_allowlist_allows_everyone(raw: str | None) -> None:
    networks, problems = parse_allowlist(raw)
    assert (networks, problems) == ((), [])
    cfg, cfg_problems = _load({} if raw is None else {"WEB_UI_ALLOWED_IPS": raw})
    assert cfg_problems == []
    for ip in ("6.6.6.6", "2001:db8::1", "127.0.0.1", "testclient", "", None):
        assert cfg.allows(ip)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("raw", "address", "widened"),
    [
        # v1 widened these silently: a prefix-length typo became "allow everyone".
        ("203.0.113.7/0", "203.0.113.7", "0.0.0.0/0"),
        ("10.0.0.1/1", "10.0.0.1", "0.0.0.0/1"),
        ("10.0.0.5/8", "10.0.0.5", "10.0.0.0/8"),
        ("10.0.0.5/255.0.0.0", "10.0.0.5", "10.0.0.0/8"),
        ("2001:db8:85a3::8a2e:370:7334/64", "2001:db8:85a3::8a2e:370:7334", "2001:db8:85a3::/64"),
        ("2001:db8::1/0", "2001:db8::1", "::/0"),
    ],
)
def test_F59_allowlist_host_bits_fail_closed(raw: str, address: str, widened: str) -> None:
    networks, problems = parse_allowlist(raw)
    assert networks == ()
    assert problems == [
        f"WEB_UI_ALLOWED_IPS has networks with host bits set, which would widen them "
        f"silently: {raw!r} (for one address write {address}, for the whole network "
        f"{widened}). The web UI stays off until this is fixed."
    ]
    cfg, cfg_problems = _load(WEB_UI_ALLOWED_IPS=raw)
    assert cfg_problems == problems
    assert not cfg.is_usable and cfg.allowlist_failed
    assert cfg.allowed_networks == ()
    for outsider in ("6.6.6.6", "10.1.2.3", "203.0.113.7", "2001:db8::1", "::ffff:6.6.6.6"):
        assert not cfg.allows(outsider)
    assert cfg.allows("127.0.0.1") and cfg.allows("::1")


def test_F59_host_bits_and_garbage_are_both_reported() -> None:
    networks, problems = parse_allowlist("198.51.100.0/24, 203.0.113.7/0, bogus")
    assert networks == (ipaddress.ip_network("198.51.100.0/24"),)
    assert len(problems) == 2
    assert problems[0].startswith("WEB_UI_ALLOWED_IPS has entries that are not IP addresses")
    assert "'bogus'" in problems[0] and "203.0.113.7/0" not in problems[0]
    assert problems[1].startswith("WEB_UI_ALLOWED_IPS has networks with host bits set")
    assert "'203.0.113.7/0'" in problems[1]
    cfg, cfg_problems = _load(WEB_UI_ALLOWED_IPS="198.51.100.0/24, 203.0.113.7/0, bogus")
    assert cfg_problems == problems
    assert not cfg.allows("198.51.100.1")  # even the valid entry: the allowlist failed
    reason = web_disabled_reason(cfg, cfg_problems)
    assert reason is not None and "host bits" in reason


def test_F59_many_host_bit_entries_are_summarised() -> None:
    raw = ",".join(f"10.0.{i}.1/16" for i in range(8))
    _, problems = parse_allowlist(raw)
    assert len(problems) == 1
    assert problems[0].count("for one address write") == 5
    assert "and 3 more." in problems[0]


def test_allowlist_network_written_with_its_first_address_is_accepted() -> None:
    networks, problems = parse_allowlist("10.0.0.0/8 203.0.113.7/32 2001:db8::/32 ::1/128")
    assert problems == []
    assert networks == (
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("203.0.113.7/32"),
        ipaddress.ip_network("2001:db8::/32"),
        ipaddress.ip_network("::1/128"),
    )


def test_allowlist_scoped_host_bits_entry_is_not_echoed_unbounded() -> None:
    """A scope id is free text: it goes through the shortening echo, not shown verbatim."""
    raw = "fe80::1%" + "s" * 40 + "/64"
    _, problems = parse_allowlist(raw)
    assert len(problems) == 1 and "host bits" in problems[0]
    assert "s" * 40 not in problems[0]


@pytest.mark.parametrize(
    ("ip", "allowed"),
    [
        ("10.1.2.3", True),
        ("11.0.0.1", False),
        ("2001:db8::abcd", True),
        ("2001:db9::1", False),
        ("::ffff:10.1.2.3", True),  # IPv4-mapped form of an allowed address
        ("::ffff:11.0.0.1", False),
        ("127.0.0.1", True),  # loopback: the container health check
        ("127.8.9.10", True),
        ("::1", True),
        ("::ffff:127.0.0.1", True),
        ("", False),
        ("not-an-ip", False),
        ("testclient", False),
        (None, False),
        ("10.1.2.3" + " " * 100, False),
    ],
)
def test_allows(ip: str | None, allowed: bool) -> None:
    cfg, problems = _load(WEB_UI_ALLOWED_IPS="10.0.0.0/8,2001:db8::/32")
    assert problems == []
    assert cfg.allows(ip) is allowed


def test_loopback_is_allowed_even_when_not_listed() -> None:
    cfg = WebConfig(enabled=True, allowed_networks=(ipaddress.ip_network("203.0.113.0/24"),))
    assert cfg.allows("127.0.0.1") and cfg.allows("::1")
    assert not cfg.allows("192.0.2.1")


# ── WebConfig as a value, validate() on direct construction ───────────────────


def test_web_config_is_frozen_and_hides_the_secret() -> None:
    cfg, _ = _load()
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.port = 1  # type: ignore[misc]
    assert SECRET not in repr(cfg)


def test_validate_on_a_directly_constructed_config() -> None:
    assert WebConfig().validate() == []
    problems = WebConfig(enabled=True).validate()
    assert [p.split(" ")[0] for p in problems] == ["WEB_UI_PUBLIC_URL", "WEB_UI_SECRET_KEY"]
    bad = WebConfig(
        enabled=True,
        public_url="https://x.example",
        secret_key=SECRET,
        port=0,
        login_ttl=0,
        session_ttl=True,  # type: ignore[arg-type]
        session_absolute_ttl=-1,
        host="",
        trusted_proxies="",
    )
    names = [p.split(" ")[0] for p in bad.validate()]
    assert names == [
        "WEB_UI_HOST",
        "WEB_UI_PORT",
        "WEB_UI_LOGIN_TTL",
        "WEB_UI_SESSION_TTL",
        "WEB_UI_SESSION_ABS_TTL",
        "WEB_UI_TRUSTED_PROXIES",
    ]
    assert not bad.is_usable
    good = WebConfig(enabled=True, public_url="https://x.example", secret_key=SECRET)
    assert good.validate() == [] and good.is_usable
    assert good.session_absolute_ttl == DEFAULT_SESSION_ABS_TTL


def test_from_settings_reads_the_settings_snapshot_not_os_environ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WEB_UI_PORT", "1234")
    settings = config.Settings.from_env({**BASE, "WEB_UI_PORT": "4321"})
    cfg, _ = WebConfig.from_settings(settings)
    assert cfg.port == 4321
    frozen = dataclasses.replace(settings, environ=MappingProxyType({**BASE}))
    assert WebConfig.from_settings(frozen)[0].port == 8080


# ── web_disabled_reason ───────────────────────────────────────────────────────


def test_web_disabled_reason() -> None:
    usable, problems = _load()
    assert web_disabled_reason(usable, problems) is None
    off, problems = _load_raw({})
    assert web_disabled_reason(off, problems) == "turned off (WEB_UI_ENABLED is not set to 1)"
    broken, problems = _load(WEB_UI_SECRET_KEY="", WEB_UI_PUBLIC_URL="")
    reason = web_disabled_reason(broken, problems)
    assert reason is not None and reason.startswith("configuration problem: ")
    assert "WEB_UI_PUBLIC_URL" in reason and "WEB_UI_SECRET_KEY" in reason


def test_web_disabled_reason_rechecks_an_empty_problem_list() -> None:
    cfg = WebConfig(enabled=True)  # no public URL, no secret
    reason = web_disabled_reason(cfg, [])
    assert reason is not None and "WEB_UI_PUBLIC_URL" in reason


def test_web_disabled_reason_summarises_many_problems() -> None:
    cfg, problems = _load_raw(
        {
            "WEB_UI_ENABLED": "1",
            "WEB_UI_PORT": "x",
            "WEB_UI_LOGIN_TTL": "x",
            "WEB_UI_SESSION_TTL": "x",
            "WEB_UI_SESSION_ABS_TTL": "x",
        }
    )
    assert len(problems) == 6  # 4 integers + public URL + secret
    reason = web_disabled_reason(cfg, problems)
    assert reason is not None
    assert reason.endswith("(and 3 more problems, see the log)")
    assert "WEB_UI_PORT" in reason and "WEB_UI_SECRET_KEY" not in reason
    one_more = web_disabled_reason(cfg, problems[:4])
    assert one_more is not None and one_more.endswith("(and 1 more problem, see the log)")


# ── import hygiene ────────────────────────────────────────────────────────────


def test_web_config_does_not_import_fastapi() -> None:
    """The bot and the health check import this module; FastAPI must stay unloaded."""
    code = (
        "import sys; import ynab_helper.web.config, ynab_helper.sessions, ynab_helper.web; "
        "bad = [m for m in ('fastapi', 'starlette', 'uvicorn', 'jinja2') if m in sys.modules]; "
        "print(bad); sys.exit(1 if bad else 0)"
    )
    repo = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=repo, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
