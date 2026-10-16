"""Independent BIP32/BIP39 private-key derivation used as a test oracle.

Deliberately shares no code with ynab_helper.balance.crypto.btc: plain double-and-add
with Fermat inverses, private (hardened-capable) derivation, and its own serialisation.
Public derivation results from the module under test must agree with
``pub(CKDpriv(k))`` computed here, and the published BIP vectors anchor both.
"""

from __future__ import annotations

import hashlib
import hmac

from Crypto.Hash import RIPEMD160

P = 2**256 - 2**32 - 977
N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
G = (
    0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798,
    0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8,
)
HARDENED = 0x80000000
B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

VERSIONS = {
    "xpub": 0x0488B21E,
    "xprv": 0x0488ADE4,
    "ypub": 0x049D7CB2,
    "yprv": 0x049D7878,
    "zpub": 0x04B24746,
    "zprv": 0x04B2430C,
    "Ypub": 0x0295B43F,
    "Zpub": 0x02AA7ED3,
    "tpub": 0x043587CF,
    "tprv": 0x04358394,
    "vpub": 0x045F1CF6,
}

ABANDON_MNEMONIC = " ".join(["abandon"] * 11 + ["about"])


def _inv(a: int) -> int:
    return pow(a, P - 2, P)


def _add(p, q):
    if p is None:
        return q
    if q is None:
        return p
    if p[0] == q[0] and (p[1] + q[1]) % P == 0:
        return None
    doubling = p == q
    s = (3 * p[0] * p[0] * _inv(2 * p[1]) if doubling else (q[1] - p[1]) * _inv(q[0] - p[0])) % P
    x = (s * s - p[0] - q[0]) % P
    return x, (s * (p[0] - x) - p[1]) % P


def _mul(k: int, point=G):
    result = None
    while k:
        if k & 1:
            result = _add(result, point)
        point = _add(point, point)
        k >>= 1
    return result


def pubkey(k: int) -> bytes:
    x, y = _mul(k)
    return bytes([2 + (y & 1)]) + x.to_bytes(32, "big")


def hash160(data: bytes) -> bytes:
    return RIPEMD160.new(hashlib.sha256(data).digest()).digest()


def b58check(payload: bytes) -> str:
    raw = payload + hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    n = int.from_bytes(raw, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = B58[r] + out
    return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + out


def seed_from_mnemonic(mnemonic: str, passphrase: str = "") -> bytes:
    return hashlib.pbkdf2_hmac("sha512", mnemonic.encode(), b"mnemonic" + passphrase.encode(), 2048)


class PrivNode:
    """A private BIP32 node with enough metadata to serialise it."""

    def __init__(self, k: int, chain: bytes, depth=0, fingerprint=b"\0" * 4, child=0):
        self.k, self.chain, self.depth, self.fingerprint, self.child_number = (
            k,
            chain,
            depth,
            fingerprint,
            child,
        )

    @classmethod
    def from_seed(cls, seed: bytes) -> PrivNode:
        digest = hmac.new(b"Bitcoin seed", seed, hashlib.sha512).digest()
        return cls(int.from_bytes(digest[:32], "big"), digest[32:])

    @property
    def pub(self) -> bytes:
        return pubkey(self.k)

    def child(self, index: int) -> PrivNode:
        if index >= HARDENED:
            data = b"\0" + self.k.to_bytes(32, "big") + index.to_bytes(4, "big")
        else:
            data = self.pub + index.to_bytes(4, "big")
        digest = hmac.new(self.chain, data, hashlib.sha512).digest()
        k = (int.from_bytes(digest[:32], "big") + self.k) % N
        return PrivNode(k, digest[32:], self.depth + 1, hash160(self.pub)[:4], index)

    def path(self, *indexes: int) -> PrivNode:
        node = self
        for index in indexes:
            node = node.child(index)
        return node

    def _payload(self, version: str, key: bytes) -> bytes:
        return (
            VERSIONS[version].to_bytes(4, "big")
            + bytes([self.depth])
            + self.fingerprint
            + self.child_number.to_bytes(4, "big")
            + self.chain
            + key
        )

    def serialize_pub(self, version: str = "xpub") -> str:
        return b58check(self._payload(version, self.pub))

    def serialize_prv(self, version: str = "xprv") -> str:
        return b58check(self._payload(version, b"\0" + self.k.to_bytes(32, "big")))


def h(index: int) -> int:
    return index | HARDENED


def account_node(purpose: int, mnemonic: str = ABANDON_MNEMONIC) -> PrivNode:
    """``m/purpose'/0'/0'`` of a BIP39 mnemonic (mainnet, account 0)."""
    return PrivNode.from_seed(seed_from_mnemonic(mnemonic)).path(h(purpose), h(0), h(0))


def p2pkh(pub: bytes) -> str:
    return b58check(b"\x00" + hash160(pub))


def p2sh_p2wpkh(pub: bytes) -> str:
    return b58check(b"\x05" + hash160(b"\x00\x14" + hash160(pub)))
