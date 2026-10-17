"""Watch-only Bitcoin primitives: key parsing, address derivation and balance scanning.

Implements what the crypto sync needs to value a wallet from public data only
(DESIGN §6.6):

* Base58 / Base58Check with checksum verification;
* extended public keys (BIP32 serialisation, SLIP-132 version bytes) with strict
  validation — checksum, 78-byte payload, known *public mainnet* version, BIP32 depth
  rules, compressed point on secp256k1 — so a typo can never silently read as 0 BTC (F48);
* BIP32 public child derivation (CKDpub, non-hardened only);
* address encoders: P2PKH, P2SH-P2WPKH (BIP49) and P2WPKH (BIP84, bech32), plus
  bech32/bech32m decoding (BIP173/BIP350) to validate single addresses;
* :func:`scan_xpub`, which derives ``m/0/i`` and ``m/1/i`` for every script type implied
  by the key's version (F28) and stops after ``gap_limit`` consecutive unused addresses;
* :class:`BlockstreamClient` (Esplora ``/address/{a}``) with rate limiting, timeouts and
  retries via :mod:`ynab_helper.net` (F78).

Amounts are integers (satoshis). Nothing here touches YNAB.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import threading
import time
import urllib.parse
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from functools import cache
from typing import Any, Final, Literal

from ynab_helper import net
from ynab_helper.errors import ConfigError, ExternalServiceError, YnabHelperError

log = logging.getLogger(__name__)


class InvalidBitcoinInput(ValueError):
    """A key or address the user supplied is not usable; ``str(exc)`` is user-friendly."""


# ── RIPEMD160 (resolved once at import) ──────────────────────────────────────


def _resolve_ripemd160() -> Callable[[bytes], bytes] | None:
    """Pick a RIPEMD160 implementation once: pycryptodome, else OpenSSL via hashlib.

    OpenSSL 3 disables RIPEMD160 by default, which is why pycryptodome is a hard
    dependency of the image.
    """
    try:
        from Crypto.Hash import RIPEMD160
    except ImportError:  # pragma: no cover - pycryptodome is a pinned dependency
        pass
    else:
        return lambda data: RIPEMD160.new(data).digest()
    try:  # pragma: no cover - only reached without pycryptodome
        hashlib.new("ripemd160", b"")
    except ValueError:  # pragma: no cover
        return None
    return lambda data: hashlib.new("ripemd160", data).digest()  # pragma: no cover


_RIPEMD160 = _resolve_ripemd160()


def hash160(data: bytes) -> bytes:
    """``RIPEMD160(SHA256(data))``.

    Raises:
        ConfigError: no RIPEMD160 implementation is available (pycryptodome missing).
    """
    if _RIPEMD160 is None:
        raise ConfigError(
            "RIPEMD160 is unavailable: install pycryptodome",
            user_message="Bitcoin address derivation is unavailable on this server "
            "(pycryptodome is not installed).",
        )
    return _RIPEMD160(hashlib.sha256(data).digest())


def _sha256d(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


# ── Base58 / Base58Check ─────────────────────────────────────────────────────

_B58_ALPHABET: Final = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_INDEX: Final = {ch: i for i, ch in enumerate(_B58_ALPHABET)}
# Longest Base58 string we ever decode (an extended key is 111 chars); bounds the
# quadratic big-int work on hostile input.
_MAX_B58_LEN: Final = 128


def b58encode(data: bytes) -> str:
    """Base58-encode ``data`` (leading zero bytes become ``1``)."""
    n = int.from_bytes(data, "big")
    out: list[str] = []
    while n:
        n, rem = divmod(n, 58)
        out.append(_B58_ALPHABET[rem])
    pad = len(data) - len(data.lstrip(b"\0"))
    return "1" * pad + "".join(reversed(out))


def b58decode(text: str) -> bytes:
    """Decode a Base58 string.

    Raises:
        InvalidBitcoinInput: empty, too long, or a character outside the Base58 alphabet.
    """
    if not text:
        raise InvalidBitcoinInput("The value is empty.")
    if len(text) > _MAX_B58_LEN:
        raise InvalidBitcoinInput("The value is too long to be a Bitcoin key or address.")
    n = 0
    for ch in text:
        digit = _B58_INDEX.get(ch)
        if digit is None:
            raise InvalidBitcoinInput(
                f"Invalid character {ch!r}: Base58 never uses 0, O, I or l. Please check for typos."
            )
        n = n * 58 + digit
    pad = len(text) - len(text.lstrip("1"))
    return b"\0" * pad + n.to_bytes((n.bit_length() + 7) // 8, "big")


def b58check_encode(payload: bytes) -> str:
    """Base58Check: ``payload`` followed by the first 4 bytes of ``SHA256d(payload)``."""
    return b58encode(payload + _sha256d(payload)[:4])


def b58check_decode(text: str) -> bytes:
    """Decode Base58Check and verify the checksum; returns the payload without it.

    Raises:
        InvalidBitcoinInput: invalid characters, too short, or checksum mismatch.
    """
    raw = b58decode(text)
    if len(raw) < 5:
        raise InvalidBitcoinInput("The value is too short.")
    payload, checksum = raw[:-4], raw[-4:]
    if not hmac.compare_digest(_sha256d(payload)[:4], checksum):
        raise InvalidBitcoinInput(
            "Checksum mismatch: the value contains a typo or was copied incompletely."
        )
    return payload


# ── secp256k1 ────────────────────────────────────────────────────────────────

_P: Final = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
_N: Final = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
_G: Final = (
    0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798,
    0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8,
)

_Point = tuple[int, int] | None  # None is the point at infinity


def _point_add(p: _Point, q: _Point) -> _Point:
    """Affine point addition on secp256k1 (handles doubling and inverses)."""
    if p is None:
        return q
    if q is None:
        return p
    x1, y1 = p
    x2, y2 = q
    if x1 == x2:
        if (y1 + y2) % _P == 0:
            return None
        slope = 3 * x1 * x1 * pow(2 * y1, -1, _P) % _P
    else:
        slope = (y2 - y1) * pow(x2 - x1, -1, _P) % _P
    x3 = (slope * slope - x1 - x2) % _P
    return x3, (slope * (x1 - x3) - y1) % _P


@cache
def _g_doublings() -> tuple[tuple[int, int], ...]:
    """``(G, 2G, 4G, …, 2^255 G)``: turns k·G into ~128 additions and no doublings."""
    table: list[tuple[int, int]] = []
    point: _Point = _G
    for _ in range(256):
        assert point is not None  # G has prime order n > 2^255
        table.append(point)
        point = _point_add(point, point)
    return tuple(table)


def _mul_g(k: int) -> _Point:
    """Scalar multiplication ``k·G`` for ``0 < k < n``."""
    result: _Point = None
    for i, point in enumerate(_g_doublings()):
        if (k >> i) & 1:
            result = _point_add(result, point)
    return result


def _compress(point: tuple[int, int]) -> bytes:
    x, y = point
    return bytes([2 + (y & 1)]) + x.to_bytes(32, "big")


def _decompress(key: bytes) -> tuple[int, int]:
    """Decode a 33-byte compressed public key and verify it lies on secp256k1.

    Raises:
        InvalidBitcoinInput: wrong length/prefix, x out of range, or not on the curve.
    """
    if len(key) != 33 or key[0] not in (2, 3):
        raise InvalidBitcoinInput("The key does not contain a compressed public key.")
    x = int.from_bytes(key[1:], "big")
    if x >= _P:
        raise InvalidBitcoinInput("The public key is not a valid secp256k1 point.")
    y_squared = (pow(x, 3, _P) + 7) % _P
    y = pow(y_squared, (_P + 1) // 4, _P)  # p ≡ 3 (mod 4): this is the square root if any
    if y * y % _P != y_squared:
        raise InvalidBitcoinInput(
            "The public key is not on the secp256k1 curve — the value is corrupted or mistyped."
        )
    if (y & 1) != (key[0] & 1):
        y = _P - y
    return x, y


def is_on_curve(key: bytes) -> bool:
    """True if ``key`` is a valid compressed secp256k1 public key."""
    try:
        _decompress(key)
    except InvalidBitcoinInput:
        return False
    return True


# ── BIP32 public derivation ──────────────────────────────────────────────────

HARDENED: Final = 0x80000000


def ckd_pub(key: bytes, chain_code: bytes, index: int) -> tuple[bytes, bytes]:
    """BIP32 CKDpub: derive the non-hardened child ``index`` of a public key.

    Returns:
        ``(child compressed public key, child chain code)``.

    Raises:
        ValueError: hardened or negative index, or an invalid child (probability < 2^-127;
            BIP32 wallets skip such an index, which a watch-only scan cannot reproduce
            safely, so it is reported instead).
    """
    if not 0 <= index < HARDENED:
        raise ValueError("hardened derivation needs the private key")
    if len(chain_code) != 32:
        raise ValueError("chain code must be 32 bytes")
    digest = hmac.new(chain_code, key + index.to_bytes(4, "big"), hashlib.sha512).digest()
    tweak = int.from_bytes(digest[:32], "big")
    if tweak >= _N:
        raise ValueError(f"invalid BIP32 child at index {index}")
    child = _point_add(_mul_g(tweak), _decompress(key))
    if child is None:
        raise ValueError(f"invalid BIP32 child at index {index} (point at infinity)")
    return _compress(child), digest[32:]


# ── script types and SLIP-132 versions ───────────────────────────────────────


class ScriptType(StrEnum):
    """Single-signature output scripts derivable from an extended public key."""

    P2PKH = "p2pkh"  # legacy 1…  (BIP44)
    P2SH_P2WPKH = "p2sh-p2wpkh"  # nested segwit 3…  (BIP49)
    P2WPKH = "p2wpkh"  # native segwit bc1q…  (BIP84)


XpubKind = Literal["xpub", "ypub", "zpub"]

# SLIP-132. A plain xpub does not say which script the wallet uses (BIP44, BIP49 and
# BIP84 wallets all export "xpub" in some software), so all three are scanned and summed.
_PUBLIC_VERSIONS: Final[dict[int, tuple[XpubKind, tuple[ScriptType, ...]]]] = {
    0x0488B21E: ("xpub", (ScriptType.P2PKH, ScriptType.P2SH_P2WPKH, ScriptType.P2WPKH)),
    0x049D7CB2: ("ypub", (ScriptType.P2SH_P2WPKH,)),
    0x04B24746: ("zpub", (ScriptType.P2WPKH,)),
}
_PRIVATE_HINT: Final = (
    "This is a PRIVATE key ({kind}). Never share it with anyone — it controls your funds. "
    "Export the extended PUBLIC key (xpub/ypub/zpub) instead."
)
_REJECTED_VERSIONS: Final[dict[int, str]] = {
    0x0488ADE4: _PRIVATE_HINT.format(kind="xprv"),
    0x049D7878: _PRIVATE_HINT.format(kind="yprv"),
    0x04B2430C: _PRIVATE_HINT.format(kind="zprv"),
    0x0295B005: _PRIVATE_HINT.format(kind="Yprv"),
    0x02AA7A99: _PRIVATE_HINT.format(kind="Zprv"),
    0x04358394: _PRIVATE_HINT.format(kind="tprv"),
    0x044A4E28: _PRIVATE_HINT.format(kind="uprv"),
    0x045F18BC: _PRIVATE_HINT.format(kind="vprv"),
    0x024285B5: _PRIVATE_HINT.format(kind="Uprv"),
    0x02575048: _PRIVATE_HINT.format(kind="Vprv"),
    0x0295B43F: "Multisig keys (Ypub) are not supported. Use a single-signature xpub/ypub/zpub.",
    0x02AA7ED3: "Multisig keys (Zpub) are not supported. Use a single-signature xpub/ypub/zpub.",
    0x043587CF: "This is a testnet key (tpub). Only Bitcoin mainnet keys are supported.",
    0x044A5262: "This is a testnet key (upub). Only Bitcoin mainnet keys are supported.",
    0x045F1CF6: "This is a testnet key (vpub). Only Bitcoin mainnet keys are supported.",
    0x024289EF: "This is a testnet key (Upub). Only Bitcoin mainnet keys are supported.",
    0x02575483: "This is a testnet key (Vpub). Only Bitcoin mainnet keys are supported.",
}
_EXTENDED_KEY_PAYLOAD_LEN: Final = 78


@dataclass(frozen=True, slots=True, repr=False)
class ExtendedPubKey:
    """A validated BIP32 extended public key (mainnet, single-sig).

    Invariants (enforced by :func:`parse_extended_pubkey`): ``key`` is a compressed point
    on secp256k1, ``chain_code`` is 32 bytes, depth-0 keys have a zero parent fingerprint
    and child number, and ``script_types`` follows SLIP-132 for ``version_kind``.
    """

    version_kind: XpubKind
    depth: int
    chain_code: bytes
    key: bytes
    script_types: tuple[ScriptType, ...]
    version: int
    parent_fingerprint: bytes = b"\0\0\0\0"
    child_number: int = 0

    def __repr__(self) -> str:
        # The key reveals every address of the wallet: keep it out of logs and tracebacks.
        return f"ExtendedPubKey({self.version_kind}, depth={self.depth})"

    @property
    def fingerprint(self) -> bytes:
        """First 4 bytes of ``hash160(key)`` (the children's parent fingerprint)."""
        return hash160(self.key)[:4]

    def child(self, index: int) -> ExtendedPubKey:
        """Non-hardened child ``index`` as an extended key of the same version."""
        if self.depth >= 255:
            raise ValueError("maximum BIP32 depth reached")
        key, chain_code = ckd_pub(self.key, self.chain_code, index)
        return ExtendedPubKey(
            version_kind=self.version_kind,
            depth=self.depth + 1,
            chain_code=chain_code,
            key=key,
            script_types=self.script_types,
            version=self.version,
            parent_fingerprint=self.fingerprint,
            child_number=index,
        )

    def to_base58(self) -> str:
        """Serialise back to the Base58Check string (same version bytes)."""
        payload = (
            self.version.to_bytes(4, "big")
            + bytes([self.depth])
            + self.parent_fingerprint
            + self.child_number.to_bytes(4, "big")
            + self.chain_code
            + self.key
        )
        return b58check_encode(payload)


def parse_extended_pubkey(text: str) -> ExtendedPubKey:
    """Parse and fully validate an xpub/ypub/zpub string (F48).

    Raises:
        InvalidBitcoinInput: with a user-friendly message for checksum/typo, wrong
            length, private key, testnet, multisig, unknown version, invalid depth fields
            or a key that is not on the curve.
    """
    text = text.strip()
    payload = b58check_decode(text)
    if len(payload) != _EXTENDED_KEY_PAYLOAD_LEN:
        raise InvalidBitcoinInput(
            "This is not a complete extended public key (wrong length). "
            "Please copy the whole xpub/ypub/zpub."
        )
    version = int.from_bytes(payload[0:4], "big")
    if version in _REJECTED_VERSIONS:
        raise InvalidBitcoinInput(_REJECTED_VERSIONS[version])
    if version not in _PUBLIC_VERSIONS:
        raise InvalidBitcoinInput(
            "Unknown extended key version. Use an xpub, ypub or zpub from your wallet."
        )
    kind, script_types = _PUBLIC_VERSIONS[version]
    depth = payload[4]
    parent_fingerprint = payload[5:9]
    child_number = int.from_bytes(payload[9:13], "big")
    chain_code = payload[13:45]
    key = payload[45:78]
    if depth == 0 and (parent_fingerprint != b"\0\0\0\0" or child_number != 0):
        raise InvalidBitcoinInput(
            "Invalid extended key: a depth-0 key must not have a parent or child index."
        )
    if key[0] == 0:
        raise InvalidBitcoinInput(_PRIVATE_HINT.format(kind=f"private data in a {kind}"))
    _decompress(key)  # raises unless a compressed point on the curve
    return ExtendedPubKey(
        version_kind=kind,
        depth=depth,
        chain_code=chain_code,
        key=key,
        script_types=script_types,
        version=version,
        parent_fingerprint=parent_fingerprint,
        child_number=child_number,
    )


# ── bech32 / bech32m (BIP173, BIP350) ────────────────────────────────────────

_BECH32_CHARSET: Final = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_BECH32_INDEX: Final = {ch: i for i, ch in enumerate(_BECH32_CHARSET)}
_BECH32_GENERATORS: Final = (0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3)
_BECH32_MAX_LEN: Final = 90


class Bech32Variant(StrEnum):
    BECH32 = "bech32"  # BIP173, witness v0
    BECH32M = "bech32m"  # BIP350, witness v1+


_BECH32_CONSTANTS: Final = {Bech32Variant.BECH32: 1, Bech32Variant.BECH32M: 0x2BC830A3}


def _bech32_polymod(values: Sequence[int]) -> int:
    checksum = 1
    for value in values:
        top = checksum >> 25
        checksum = ((checksum & 0x1FFFFFF) << 5) ^ value
        for i, generator in enumerate(_BECH32_GENERATORS):
            if (top >> i) & 1:
                checksum ^= generator
    return checksum


def _bech32_hrp_expand(hrp: str) -> list[int]:
    return [ord(ch) >> 5 for ch in hrp] + [0] + [ord(ch) & 31 for ch in hrp]


def bech32_encode(hrp: str, data: Sequence[int], variant: Bech32Variant) -> str:
    """Encode 5-bit ``data`` under ``hrp`` with a bech32 or bech32m checksum."""
    values = _bech32_hrp_expand(hrp) + list(data)
    polymod = _bech32_polymod([*values, 0, 0, 0, 0, 0, 0]) ^ _BECH32_CONSTANTS[variant]
    checksum = [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]
    return hrp + "1" + "".join(_BECH32_CHARSET[d] for d in [*data, *checksum])


def bech32_decode(text: str) -> tuple[str, list[int], Bech32Variant]:
    """Decode and verify a bech32/bech32m string.

    Returns:
        ``(hrp in lower case, 5-bit data without checksum, variant)``.

    Raises:
        ValueError: mixed case, bad characters, length, separator or checksum.
    """
    if any(ord(ch) < 33 or ord(ch) > 126 for ch in text):
        raise ValueError("bech32: character out of range")
    if text.lower() != text and text.upper() != text:
        raise ValueError("bech32: mixed case")
    if len(text) > _BECH32_MAX_LEN:
        raise ValueError("bech32: too long")
    text = text.lower()
    sep = text.rfind("1")
    if sep < 1 or sep + 7 > len(text):
        raise ValueError("bech32: missing separator, empty prefix or short checksum")
    hrp = text[:sep]
    try:
        data = [_BECH32_INDEX[ch] for ch in text[sep + 1 :]]
    except KeyError:
        raise ValueError("bech32: invalid data character") from None
    constant = _bech32_polymod(_bech32_hrp_expand(hrp) + data)
    for variant, expected in _BECH32_CONSTANTS.items():
        if constant == expected:
            return hrp, data[:-6], variant
    raise ValueError("bech32: checksum mismatch")


def convertbits(data: Sequence[int], frombits: int, tobits: int, *, pad: bool) -> list[int]:
    """Regroup bits (BIP173 reference). Raises ValueError on invalid values or padding."""
    acc = 0
    bits = 0
    out: list[int] = []
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for value in data:
        if value < 0 or value >> frombits:
            raise ValueError("convertbits: value out of range")
        acc = ((acc << frombits) | value) & max_acc
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            out.append((acc >> bits) & maxv)
    if pad:
        if bits:
            out.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        raise ValueError("convertbits: invalid padding")
    return out


def decode_segwit_address(hrp: str, address: str) -> tuple[int, bytes]:
    """Decode a segwit address for ``hrp`` (BIP173/BIP350 rules).

    Returns:
        ``(witness version, witness program)``.

    Raises:
        ValueError: wrong prefix, checksum variant, witness version or program length.
    """
    got_hrp, data, variant = bech32_decode(address)
    if got_hrp != hrp:
        raise ValueError(f"segwit: expected prefix {hrp!r}")
    if not data:
        raise ValueError("segwit: empty data")
    version = data[0]
    if version > 16:
        raise ValueError("segwit: invalid witness version")
    program = bytes(convertbits(data[1:], 5, 8, pad=False))
    if not 2 <= len(program) <= 40:
        raise ValueError("segwit: invalid program length")
    if version == 0 and len(program) not in (20, 32):
        raise ValueError("segwit: invalid v0 program length")
    expected = Bech32Variant.BECH32 if version == 0 else Bech32Variant.BECH32M
    if variant is not expected:
        raise ValueError(f"segwit: witness v{version} must use {expected.value}")
    return version, program


def encode_segwit_address(hrp: str, version: int, program: bytes) -> str:
    """Encode a segwit address; the result is round-trip verified."""
    variant = Bech32Variant.BECH32 if version == 0 else Bech32Variant.BECH32M
    address = bech32_encode(hrp, [version, *convertbits(program, 8, 5, pad=True)], variant)
    if decode_segwit_address(hrp, address) != (version, bytes(program)):
        raise ValueError("segwit: encoding round-trip failed")
    return address


# ── address encoders ─────────────────────────────────────────────────────────

_P2PKH_VERSION: Final = 0x00
_P2SH_VERSION: Final = 0x05
_TESTNET_B58_VERSIONS: Final = frozenset({0x6F, 0xC4})
_WIF_VERSIONS: Final = frozenset({0x80, 0xEF})
_MAINNET_HRP: Final = "bc"
_TESTNET_ADDRESS: Final = "This is a testnet address. Only Bitcoin mainnet is supported."


def _require_compressed(pubkey: bytes) -> None:
    if len(pubkey) != 33 or pubkey[0] not in (2, 3):
        raise ValueError("expected a 33-byte compressed public key")


def p2pkh_address(pubkey: bytes) -> str:
    """Legacy ``1…`` address of a compressed public key."""
    _require_compressed(pubkey)
    return b58check_encode(bytes([_P2PKH_VERSION]) + hash160(pubkey))


def p2sh_p2wpkh_address(pubkey: bytes) -> str:
    """BIP49 nested-segwit ``3…`` address (P2SH wrapping ``0 <hash160(pubkey)>``)."""
    _require_compressed(pubkey)
    redeem_script = b"\x00\x14" + hash160(pubkey)
    return b58check_encode(bytes([_P2SH_VERSION]) + hash160(redeem_script))


def p2wpkh_address(pubkey: bytes) -> str:
    """BIP84 native-segwit ``bc1q…`` address."""
    _require_compressed(pubkey)
    return encode_segwit_address(_MAINNET_HRP, 0, hash160(pubkey))


_ENCODERS: Final[dict[ScriptType, Callable[[bytes], str]]] = {
    ScriptType.P2PKH: p2pkh_address,
    ScriptType.P2SH_P2WPKH: p2sh_p2wpkh_address,
    ScriptType.P2WPKH: p2wpkh_address,
}


def address_for(script_type: ScriptType, pubkey: bytes) -> str:
    """Encode ``pubkey`` as an address of ``script_type``."""
    return _ENCODERS[script_type](pubkey)


def derive_address(xpub: ExtendedPubKey, script_type: ScriptType, chain: int, index: int) -> str:
    """Address at ``m/chain/index`` below ``xpub`` for ``script_type``."""
    chain_key, chain_code = ckd_pub(xpub.key, xpub.chain_code, chain)
    child_key, _ = ckd_pub(chain_key, chain_code, index)
    return address_for(script_type, child_key)


# ── user input validation ────────────────────────────────────────────────────

BtcInput = tuple[Literal["xpub"], ExtendedPubKey] | tuple[Literal["address"], str]

# Longer than any address (P2WSH bech32 is 62 chars): must be an extended key.
_EXTENDED_KEY_MIN_LEN: Final = 91
_MAX_INPUT_LEN: Final = 128


def validate_btc_address(text: str) -> str:
    """Validate a single mainnet address and return it normalised.

    Accepts P2PKH (``1…``), P2SH (``3…``), segwit v0 (``bc1q…``, bech32) and taproot
    (``bc1p…``, bech32m). Bech32 addresses are returned in lower case.

    Raises:
        InvalidBitcoinInput: with a user-friendly message.
    """
    text = text.strip()
    lowered = text.lower()
    if lowered.startswith(("tb1", "bcrt1")):
        raise InvalidBitcoinInput(_TESTNET_ADDRESS)
    if lowered.startswith(_MAINNET_HRP + "1"):
        try:
            version, program = decode_segwit_address(_MAINNET_HRP, text)
        except ValueError:
            raise InvalidBitcoinInput(
                "This bc1 address is invalid (checksum or length). Please check for typos."
            ) from None
        if (version, len(program)) not in ((0, 20), (0, 32), (1, 32)):
            raise InvalidBitcoinInput(
                f"Segwit version {version} addresses of this length are not supported."
            )
        return lowered
    payload = _try_b58check(text)
    if payload is not None:
        if len(payload) == 21 and payload[0] in (_P2PKH_VERSION, _P2SH_VERSION):
            return text
        if len(payload) == 21 and payload[0] in _TESTNET_B58_VERSIONS:
            raise InvalidBitcoinInput(_TESTNET_ADDRESS)
        if len(payload) in (33, 34) and payload[0] in _WIF_VERSIONS:
            raise InvalidBitcoinInput(_PRIVATE_HINT.format(kind="WIF"))
    elif text[:1] in ("1", "3"):
        b58check_decode(text)  # raises the specific typo/checksum message
    raise InvalidBitcoinInput(
        "Not a Bitcoin address or extended public key. Send an address (1…, 3…, bc1…) "
        "or an xpub/ypub/zpub."
    )


def _try_b58check(text: str) -> bytes | None:
    try:
        return b58check_decode(text)
    except InvalidBitcoinInput:
        return None


def validate_btc_input(value: str) -> BtcInput:
    """Classify and validate what a user entered as their Bitcoin wallet.

    Returns:
        ``("xpub", ExtendedPubKey)`` or ``("address", normalised address)``.

    Raises:
        InvalidBitcoinInput: (a ``ValueError``) with a message safe to show the user.
    """
    if not isinstance(value, str) or not value.strip():
        raise InvalidBitcoinInput(
            "Please send a Bitcoin address or an extended public key (xpub/ypub/zpub)."
        )
    text = value.strip()
    if len(text) > _MAX_INPUT_LEN:
        raise InvalidBitcoinInput("The value is too long to be a Bitcoin key or address.")
    if len(text) >= _EXTENDED_KEY_MIN_LEN:
        return "xpub", parse_extended_pubkey(text)
    return "address", validate_btc_address(text)


# ── xpub scanning ────────────────────────────────────────────────────────────

AddressStatsFn = Callable[[str], tuple[int, int]]
"""``fetch(address) -> (balance_sats, tx_count)`` — e.g. :meth:`BlockstreamClient.address_stats`."""

DEFAULT_GAP_LIMIT: Final = 20
RECEIVE_CHAIN: Final = 0
CHANGE_CHAIN: Final = 1
#: Defence against an API that reports every address as used: a scan never runs away.
DEFAULT_MAX_ADDRESSES_PER_CHAIN: Final = 10_000


class ScanLimitExceeded(YnabHelperError):
    """More used addresses on one chain than the scanner is willing to walk."""


@dataclass(frozen=True, slots=True)
class ScriptTypeScan:
    """Totals of one script type (both chains)."""

    script_type: ScriptType
    balance_sats: int
    used_addresses: int
    scanned: int


@dataclass(frozen=True, slots=True)
class XpubScanResult:
    """Outcome of :func:`scan_xpub`.

    ``used_addresses == 0`` means no address of the wallet has ever received funds — the
    crypto sync treats that as suspicious when YNAB holds a non-zero balance.
    """

    balance_sats: int
    used_addresses: int
    scanned: int
    per_script_type: dict[ScriptType, ScriptTypeScan]


class _ChildCache:
    """Lazily derived child public keys of one chain, shared by all script types."""

    def __init__(self, xpub: ExtendedPubKey, chain: int) -> None:
        self._key, self._chain_code = ckd_pub(xpub.key, xpub.chain_code, chain)
        self._children: list[bytes] = []

    def __getitem__(self, index: int) -> bytes:
        while len(self._children) <= index:
            child, _ = ckd_pub(self._key, self._chain_code, len(self._children))
            self._children.append(child)
        return self._children[index]


def scan_xpub(
    xpub: ExtendedPubKey | str,
    fetch_address_stats: AddressStatsFn,
    gap_limit: int = DEFAULT_GAP_LIMIT,
    *,
    max_addresses_per_chain: int = DEFAULT_MAX_ADDRESSES_PER_CHAIN,
) -> XpubScanResult:
    """Sum the balance of every address of an HD wallet.

    For each script type of ``xpub`` (SLIP-132: xpub → P2PKH + P2SH-P2WPKH + P2WPKH,
    ypub → P2SH-P2WPKH, zpub → P2WPKH) and each chain (receive ``m/0/i``, change
    ``m/1/i``), addresses are queried in order until ``gap_limit`` consecutive addresses
    have no transactions. Any fetch error propagates: a partial sum is never returned.

    Raises:
        InvalidBitcoinInput: ``xpub`` given as an invalid string.
        ScanLimitExceeded: a chain has more than ``max_addresses_per_chain`` addresses.
        ValueError: ``gap_limit < 1`` or the stats function returned negative numbers.
    """
    if gap_limit < 1:
        raise ValueError("gap_limit must be >= 1")
    key = parse_extended_pubkey(xpub) if isinstance(xpub, str) else xpub
    chains = {chain: _ChildCache(key, chain) for chain in (RECEIVE_CHAIN, CHANGE_CHAIN)}
    per_type: dict[ScriptType, ScriptTypeScan] = {}
    for script_type in key.script_types:
        balance = used = scanned = 0
        for chain, children in chains.items():
            chain_balance, chain_used, chain_scanned = _scan_chain(
                script_type,
                chain,
                children,
                fetch_address_stats,
                gap_limit,
                max_addresses_per_chain,
            )
            balance += chain_balance
            used += chain_used
            scanned += chain_scanned
        per_type[script_type] = ScriptTypeScan(script_type, balance, used, scanned)
        log.debug(
            "xpub scan %s: %d used of %d scanned, %d sats",
            script_type.value,
            used,
            scanned,
            balance,
        )
    return XpubScanResult(
        balance_sats=sum(s.balance_sats for s in per_type.values()),
        used_addresses=sum(s.used_addresses for s in per_type.values()),
        scanned=sum(s.scanned for s in per_type.values()),
        per_script_type=per_type,
    )


def _scan_chain(
    script_type: ScriptType,
    chain: int,
    children: _ChildCache,
    fetch: AddressStatsFn,
    gap_limit: int,
    max_addresses: int,
) -> tuple[int, int, int]:
    """Walk one chain until ``gap_limit`` consecutive unused addresses; return totals."""
    balance = used = gap = index = 0
    while gap < gap_limit:
        if index >= max_addresses:
            raise ScanLimitExceeded(
                f"{script_type.value} chain {chain}: more than {max_addresses} addresses used",
                user_message="The Bitcoin wallet has too many used addresses to scan.",
            )
        address = address_for(script_type, children[index])
        sats, tx_count = fetch(address)
        if sats < 0 or tx_count < 0:
            raise ValueError("address stats must be non-negative")
        balance += sats
        if tx_count > 0:
            used += 1
            gap = 0
            log.debug("xpub scan %s m/%d/%d used: %d sats", script_type.value, chain, index, sats)
        else:
            gap += 1
        index += 1
    return balance, used, index


# ── Blockstream (Esplora) ────────────────────────────────────────────────────

DEFAULT_ESPLORA_URL: Final = "https://blockstream.info/api"
_STATS_FIELDS: Final = ("funded_txo_sum", "spent_txo_sum", "tx_count")


class BlockstreamClient:
    """Esplora address lookups with client-side pacing and net.py retries.

    ``min_interval`` spaces request *starts* (Blockstream's free tier is strict); unlike
    v1's unconditional sleep it only waits when calls come faster than that.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_ESPLORA_URL,
        *,
        request: Callable[..., Any] = net.request_json,
        min_interval: float = 0.25,
        timeout: float = 15.0,
        retries: int = 4,
        backoff: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._request = request
        self._min_interval = max(0.0, min_interval)
        self._timeout = timeout
        self._retries = retries
        self._backoff = backoff
        self._sleep = sleep
        self._clock = clock
        self._lock = threading.Lock()
        self._next_start = float("-inf")

    def address_stats(self, address: str) -> tuple[int, int]:
        """Return ``(balance_sats, tx_count)`` including unconfirmed (mempool) activity.

        Balance = funded − spent over chain + mempool, as in v1.

        Raises:
            ExternalServiceError: network/HTTP failure after retries, or a malformed reply.
        """
        self._throttle()
        data = self._request(
            "GET",
            f"{self._base_url}/address/{urllib.parse.quote(address, safe='')}",
            timeout=self._timeout,
            retries=self._retries,
            backoff=self._backoff,
            service="Blockstream",
            sleep=self._sleep,
        )
        return parse_address_stats(data, address)

    def _throttle(self) -> None:
        with self._lock:
            wait = self._next_start - self._clock()
            if wait > 0:
                self._sleep(wait)
            self._next_start = self._clock() + self._min_interval


def parse_address_stats(data: Any, address: str) -> tuple[int, int]:
    """Validate an Esplora ``/address/{a}`` reply and return ``(balance_sats, tx_count)``.

    Raises:
        ExternalServiceError: missing sections, non-integer or negative fields, a reply
            for a different address, or a negative balance.
    """

    def malformed(reason: str) -> ExternalServiceError:
        return ExternalServiceError(
            f"Blockstream returned malformed stats for {address[:12]}…: {reason}",
            service="Blockstream",
            retriable=True,
            user_message="Blockstream returned an unexpected response. Please try again later.",
        )

    if not isinstance(data, dict):
        raise malformed("not an object")
    reported = data.get("address")
    if reported is not None and reported != address:
        raise malformed("reply is for a different address")
    totals = dict.fromkeys(_STATS_FIELDS, 0)
    for section in ("chain_stats", "mempool_stats"):
        stats = data.get(section)
        if not isinstance(stats, dict):
            raise malformed(f"{section} missing")
        for name in _STATS_FIELDS:
            value = stats.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise malformed(f"{section}.{name}={value!r}")
            totals[name] += value
    balance = totals["funded_txo_sum"] - totals["spent_txo_sum"]
    if balance < 0:
        raise malformed("spent exceeds funded")
    if balance > 0 and totals["tx_count"] == 0:
        raise malformed("balance without transactions")
    return balance, totals["tx_count"]
