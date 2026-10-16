"""Tests for ynab_helper.balance.crypto.btc (DESIGN §6.6; findings F28, F48, F78)."""

from __future__ import annotations

import functools

import pytest

from tests.balance.crypto import bip32_oracle as oracle
from tests.net.fake_opener import FakeOpener, FakeResponse, SleepRecorder, json_response
from ynab_helper import net
from ynab_helper.balance.crypto import btc
from ynab_helper.balance.crypto.btc import InvalidBitcoinInput, ScriptType
from ynab_helper.errors import ConfigError, ExternalServiceError, YnabHelperError

# ── published vectors ────────────────────────────────────────────────────────

# BIP32 test vector 1 (seed 000102030405060708090a0b0c0d0e0f), public serialisations.
TV1_SEED = bytes.fromhex("000102030405060708090a0b0c0d0e0f")
TV1_M = "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265TMg7usUDFdp6W1EGMcet8"
TV1_M_XPRV = "xprv9s21ZrQH143K3QTDL4LXw2F7HEK3wJUD2nW2nRk4stbPy6cq3jPPqjiChkVvvNKmPGJxWUtg6LnF5kejMRNNU3TGtRBeJgk33yuGBxrMPHi"
TV1_M_0H = "xpub68Gmy5EdvgibQVfPdqkBBCHxA5htiqg55crXYuXoQRKfDBFA1WEjWgP6LHhwBZeNK1VTsfTFUHCdrfp1bgwQ9xv5ski8PX9rL2dZXvgGDnw"
TV1_M_0H_1 = "xpub6ASuArnXKPbfEwhqN6e3mwBcDTgzisQN1wXN9BJcM47sSikHjJf3UFHKkNAWbWMiGj7Wf5uMash7SyYq527Hqck2AxYysAA7xmALppuCkwQ"
TV1_M_0H_1_2H = "xpub6D4BDPcP2GT577Vvch3R8wDkScZWzQzMMUm3PWbmWvVJrZwQY4VUNgqFJPMM3No2dFDFGTsxxpG5uJh7n7epu4trkrX7x7DogT5Uv6fcLW5"
TV1_M_0H_1_2H_2 = "xpub6FHa3pjLCk84BayeJxFW2SP4XRrFd1JYnxeLeU8EqN3vDfZmbqBqaGJAyiLjTAwm6ZLRQUMv1ZACTj37sR62cfN7fe5JnJ7dh8zL4fiyLHV"
TV1_M_0H_1_2H_2_1E9 = "xpub6H1LXWLaKsWFhvm6RVpEL9P4KfRZSW7abD2ttkWP3SSQvnyA8FSVqNTEcYFgJS2UaFcxupHiYkro49S8yGasTvXEYBVPamhGW6cFJodrTHy"

# Mnemonic "abandon ×11 about", account 0 of BIP44 / BIP49 / BIP84.
BIP44_XPUB = "xpub6BosfCnifzxcFwrSzQiqu2DBVTshkCXacvNsWGYJVVhhawA7d4R5WSWGFNbi8Aw6ZRc1brxMyWMzG3DSSSSoekkudhUd9yLb6qx39T9nMdj"
BIP49_YPUB = "ypub6Ww3ibxVfGzLrAH1PNcjyAWenMTbbAosGNB6VvmSEgytSER9azLDWCxoJwW7Ke7icmizBMXrzBx9979FfaHxHcrArf3zbeJJJUZPf663zsP"
BIP84_ZPUB = "zpub6rFR7y4Q2AijBEqTUquhVz398htDFrtymD9xYYfG1m4wAcvPhXNfE3EfH1r1ADqtfSdVCToUG868RvUUkgDKf31mGDtKsAYz2oz2AGutZYs"

BIP44_RECEIVE_0 = "1LqBGSKuX5yYUonjxT5qGfpUsXKYYWeabA"
BIP44_RECEIVE_1 = "1Ak8PffB2meyfYnbXZR9EGfLfFZVpzJvQP"
BIP49_RECEIVE_0 = "37VucYSaXLCAsxYyAPfbSi9eh4iEcbShgf"
BIP49_RECEIVE_1 = "3LtMnn87fqUeHBUG414p9CWwnoV6E2pNKS"
BIP84_RECEIVE_0 = "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu"
BIP84_RECEIVE_1 = "bc1qnjg0jd8228aq7egyzacy8cys3knf9xvrerkf9g"
BIP84_CHANGE_0 = "bc1q8c6fshw2dlwun7ekn9qwf37cu2rn755upcp6el"
BIP84_RECEIVE_0_PUBKEY = "0330d54fd0dd420a6e5f8d3624f5f3482cae350f79d5f0753bf5beef9c2d91af3c"

G_COMPRESSED = bytes.fromhex("0279BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798")


def reserialize(xkey: str, version: str) -> str:
    """Same key material under different SLIP-132 version bytes (valid checksum)."""
    payload = btc.b58check_decode(xkey)
    return btc.b58check_encode(oracle.VERSIONS[version].to_bytes(4, "big") + payload[4:])


def with_payload(xkey: str, **changes: bytes) -> str:
    """Re-encode ``xkey`` with fields of the 78-byte payload replaced (valid checksum)."""
    payload = bytearray(btc.b58check_decode(xkey))
    offsets = {"version": (0, 4), "depth": (4, 5), "fingerprint": (5, 9), "child": (9, 13),
               "chain": (13, 45), "key": (45, 78)}  # fmt: skip
    for name, value in changes.items():
        start, end = offsets[name]
        assert len(value) == end - start
        payload[start:end] = value
    return btc.b58check_encode(bytes(payload))


# ── primitives ───────────────────────────────────────────────────────────────


def test_hash160_and_address_encoders_on_the_generator_point():
    assert btc.hash160(G_COMPRESSED).hex() == "751e76e8199196d454941c45d1b3a323f1433bd6"
    assert btc.p2pkh_address(G_COMPRESSED) == "1BgGZ9tcN4rm9KBzDn7KprQz87SZ26SAMH"
    assert btc.p2wpkh_address(G_COMPRESSED) == "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"
    assert btc.p2sh_p2wpkh_address(G_COMPRESSED) == oracle.p2sh_p2wpkh(G_COMPRESSED)


def test_address_encoders_require_compressed_keys():
    with pytest.raises(ValueError):
        btc.p2pkh_address(b"\x04" + bytes(64))
    with pytest.raises(ValueError):
        btc.p2wpkh_address(G_COMPRESSED[:-1])


def test_hash160_without_ripemd160_raises_a_clear_config_error(monkeypatch):
    monkeypatch.setattr(btc, "_RIPEMD160", None)
    with pytest.raises(ConfigError, match="pycryptodome"):
        btc.hash160(b"abc")


@pytest.mark.parametrize("data", [b"\0", b"\0\0\x01", bytes(range(40)), b"\xff" * 33])
def test_base58_round_trip(data):
    assert btc.b58decode(btc.b58encode(data)) == data
    assert btc.b58check_decode(btc.b58check_encode(data)) == data


def test_base58_rejects_empty_and_oversized_input():
    with pytest.raises(InvalidBitcoinInput, match="empty"):
        btc.b58decode("")
    with pytest.raises(InvalidBitcoinInput, match="too long"):
        btc.b58decode("2" * 129)
    with pytest.raises(InvalidBitcoinInput, match="too short"):
        btc.b58check_decode(btc.b58encode(b"\0\0\0\0"))


def test_base58_rejects_characters_outside_the_alphabet():
    for bad in ("0", "O", "I", "l", " ", "+"):
        with pytest.raises(InvalidBitcoinInput, match="Base58"):
            btc.b58decode("1abc" + bad)


def test_base58check_detects_corruption():
    encoded = btc.b58check_encode(b"\x00" + bytes(20))
    corrupted = encoded[:-1] + ("2" if encoded[-1] != "2" else "3")
    with pytest.raises(InvalidBitcoinInput, match="Checksum"):
        btc.b58check_decode(corrupted)


def test_is_on_curve():
    assert btc.is_on_curve(G_COMPRESSED)
    assert btc.is_on_curve(bytes.fromhex(BIP84_RECEIVE_0_PUBKEY))
    assert not btc.is_on_curve(bytes.fromhex("02" + "00" * 31 + "07"))  # BIP32 TV5
    assert not btc.is_on_curve(b"\x04" + G_COMPRESSED[1:])
    assert not btc.is_on_curve(b"\x02" + b"\xff" * 32)  # x >= p


# ── BIP32 public derivation ──────────────────────────────────────────────────


def test_bip32_tv1_parse_round_trip():
    for xpub in (TV1_M, TV1_M_0H, TV1_M_0H_1, TV1_M_0H_1_2H, TV1_M_0H_1_2H_2, TV1_M_0H_1_2H_2_1E9):
        assert btc.parse_extended_pubkey(xpub).to_base58() == xpub


def test_bip32_tv1_public_derivation_of_non_hardened_children():
    assert btc.parse_extended_pubkey(TV1_M_0H).child(1).to_base58() == TV1_M_0H_1
    m_0h_1_2h_2 = btc.parse_extended_pubkey(TV1_M_0H_1_2H).child(2)
    assert m_0h_1_2h_2.to_base58() == TV1_M_0H_1_2H_2
    assert m_0h_1_2h_2.child(1_000_000_000).to_base58() == TV1_M_0H_1_2H_2_1E9


def test_bip32_tv1_oracle_agrees_with_published_master():
    master = oracle.PrivNode.from_seed(TV1_SEED)
    assert master.serialize_pub() == TV1_M
    assert master.serialize_prv() == TV1_M_XPRV
    assert master.child(oracle.h(0)).serialize_pub() == TV1_M_0H


@pytest.mark.parametrize("index", [0, 1, 7, 2**31 - 1])
def test_ckd_pub_matches_private_derivation(index):
    node = oracle.account_node(84)
    xpub = btc.parse_extended_pubkey(node.serialize_pub("zpub"))
    key, chain_code = btc.ckd_pub(xpub.key, xpub.chain_code, index)
    child = node.child(index)
    assert key == child.pub
    assert chain_code == child.chain


@pytest.mark.parametrize("index", [-1, 2**31, 2**32])
def test_ckd_pub_rejects_hardened_or_out_of_range_indexes(index):
    xpub = btc.parse_extended_pubkey(TV1_M)
    with pytest.raises(ValueError, match="hardened"):
        btc.ckd_pub(xpub.key, xpub.chain_code, index)


def test_bip84_zpub_addresses():
    zpub = btc.parse_extended_pubkey(BIP84_ZPUB)
    assert zpub.depth == 3
    assert btc.derive_address(zpub, ScriptType.P2WPKH, 0, 0) == BIP84_RECEIVE_0
    assert btc.derive_address(zpub, ScriptType.P2WPKH, 0, 1) == BIP84_RECEIVE_1
    assert btc.derive_address(zpub, ScriptType.P2WPKH, 1, 0) == BIP84_CHANGE_0
    receive = zpub.child(0).child(0)
    assert receive.key.hex() == BIP84_RECEIVE_0_PUBKEY


def test_bip49_ypub_addresses():
    ypub = btc.parse_extended_pubkey(BIP49_YPUB)
    assert btc.derive_address(ypub, ScriptType.P2SH_P2WPKH, 0, 0) == BIP49_RECEIVE_0
    assert btc.derive_address(ypub, ScriptType.P2SH_P2WPKH, 0, 1) == BIP49_RECEIVE_1


def test_bip44_xpub_addresses():
    xpub = btc.parse_extended_pubkey(BIP44_XPUB)
    assert btc.derive_address(xpub, ScriptType.P2PKH, 0, 0) == BIP44_RECEIVE_0
    assert btc.derive_address(xpub, ScriptType.P2PKH, 0, 1) == BIP44_RECEIVE_1


@pytest.mark.parametrize(("purpose", "version"), [(44, "xpub"), (49, "ypub"), (84, "zpub")])
def test_published_account_keys_match_the_oracle(purpose, version):
    published = {44: BIP44_XPUB, 49: BIP49_YPUB, 84: BIP84_ZPUB}[purpose]
    assert oracle.account_node(purpose).serialize_pub(version) == published


def test_extended_key_repr_does_not_leak_key_material():
    xpub = btc.parse_extended_pubkey(BIP84_ZPUB)
    text = repr(xpub)
    assert text == "ExtendedPubKey(zpub, depth=3)"
    assert xpub.key.hex() not in text
    assert BIP84_ZPUB[:20] not in text


# ── SLIP-132 script type selection (F28) ─────────────────────────────────────


def test_F28_script_types_follow_slip132_versions():
    assert btc.parse_extended_pubkey(BIP44_XPUB).script_types == (
        ScriptType.P2PKH,
        ScriptType.P2SH_P2WPKH,
        ScriptType.P2WPKH,
    )
    assert btc.parse_extended_pubkey(BIP49_YPUB).script_types == (ScriptType.P2SH_P2WPKH,)
    assert btc.parse_extended_pubkey(BIP84_ZPUB).script_types == (ScriptType.P2WPKH,)
    assert btc.parse_extended_pubkey(BIP49_YPUB).version_kind == "ypub"


def test_F28_ypub_scan_finds_nested_segwit_funds():
    """v1 scanned every key as bc1q, so a funded ypub read as 0 BTC."""
    funded = {BIP49_RECEIVE_0: (50_000_000, 2)}
    queried: list[str] = []

    def stats(address: str) -> tuple[int, int]:
        queried.append(address)
        return funded.get(address, (0, 0))

    result = btc.scan_xpub(BIP49_YPUB, stats, gap_limit=2)
    assert result.balance_sats == 50_000_000
    assert result.used_addresses == 1
    assert all(a.startswith("3") for a in queried)
    assert set(result.per_script_type) == {ScriptType.P2SH_P2WPKH}


def test_F28_plain_xpub_scans_and_sums_all_three_script_types():
    # The BIP49 account key exported under the plain "xpub" version (common in wallets).
    xpub = reserialize(BIP49_YPUB, "xpub")
    node = oracle.account_node(49)
    receive0 = node.path(0, 0).pub
    change0 = node.path(1, 0).pub
    funded = {
        oracle.p2pkh(receive0): (1_000, 1),
        BIP49_RECEIVE_0: (20_000, 3),
        btc.p2wpkh_address(change0): (300_000, 1),
    }
    result = btc.scan_xpub(xpub, lambda a: funded.get(a, (0, 0)), gap_limit=2)
    assert result.balance_sats == 321_000
    assert result.used_addresses == 3
    per = result.per_script_type
    assert list(per) == [ScriptType.P2PKH, ScriptType.P2SH_P2WPKH, ScriptType.P2WPKH]
    assert per[ScriptType.P2PKH].balance_sats == 1_000
    assert per[ScriptType.P2SH_P2WPKH].balance_sats == 20_000
    assert per[ScriptType.P2WPKH].balance_sats == 300_000
    # Each script type: receive used at 0 → 1 + 2 scanned, change: 2 (+1 if used at 0).
    assert per[ScriptType.P2PKH].scanned == 3 + 2
    assert per[ScriptType.P2WPKH].scanned == 2 + 3
    assert result.scanned == sum(s.scanned for s in per.values())


# ── extended key validation (F48) ────────────────────────────────────────────


def test_F48_every_single_character_typo_is_rejected():
    """v1 ignored the checksum, so a typo derived valid-looking empty addresses → 0 BTC."""
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    for pos in range(len(BIP84_ZPUB)):
        original = BIP84_ZPUB[pos]
        replacement = alphabet[(alphabet.index(original) + 1) % 58]
        mutated = BIP84_ZPUB[:pos] + replacement + BIP84_ZPUB[pos + 1 :]
        with pytest.raises(InvalidBitcoinInput):
            btc.parse_extended_pubkey(mutated)


@pytest.mark.parametrize(
    "mutated",
    [BIP84_ZPUB[:-1], BIP84_ZPUB[:-5], BIP84_ZPUB + "a", BIP84_ZPUB[:50] + BIP84_ZPUB[51:]],
    ids=["truncated-1", "truncated-5", "extra-char", "missing-middle"],
)
def test_F48_incomplete_or_extended_strings_are_rejected(mutated):
    with pytest.raises(InvalidBitcoinInput):
        btc.parse_extended_pubkey(mutated)


def test_F48_invalid_character_gets_a_friendly_message():
    with pytest.raises(InvalidBitcoinInput, match="0, O, I or l"):
        btc.parse_extended_pubkey(BIP84_ZPUB[:30] + "0" + BIP84_ZPUB[31:])


def test_F48_off_curve_key_with_valid_checksum_is_rejected():
    bad = with_payload(BIP84_ZPUB, key=bytes.fromhex("02" + "00" * 31 + "07"))
    with pytest.raises(InvalidBitcoinInput, match="curve"):
        btc.parse_extended_pubkey(bad)


@pytest.mark.parametrize("prefix", [b"\x04", b"\x01", b"\x05"])
def test_F48_non_compressed_key_prefixes_are_rejected(prefix):
    key = btc.b58check_decode(BIP84_ZPUB)[45:78]
    with pytest.raises(InvalidBitcoinInput):
        btc.parse_extended_pubkey(with_payload(BIP84_ZPUB, key=prefix + key[1:]))


def test_F48_private_key_material_under_a_public_version_is_rejected():
    bad = with_payload(BIP84_ZPUB, key=b"\x00" + bytes(31) + b"\x01")
    with pytest.raises(InvalidBitcoinInput, match="PRIVATE"):
        btc.parse_extended_pubkey(bad)


@pytest.mark.parametrize(
    "changes",
    [{"fingerprint": b"\0\0\0\x01"}, {"child": b"\0\0\0\x01"}],
    ids=["fingerprint", "child-number"],
)
def test_F48_depth_zero_with_parent_data_is_rejected(changes):
    bad = with_payload(TV1_M, **changes)
    with pytest.raises(InvalidBitcoinInput, match="depth-0"):
        btc.parse_extended_pubkey(bad)


@pytest.mark.parametrize("length", [77, 79])
def test_F48_wrong_payload_length_is_rejected(length):
    payload = btc.b58check_decode(BIP84_ZPUB)
    payload = payload[:length] if length < 78 else payload + b"\0"
    with pytest.raises(InvalidBitcoinInput, match="length"):
        btc.parse_extended_pubkey(btc.b58check_encode(payload))


def test_F48_unknown_version_is_rejected():
    bad = with_payload(BIP84_ZPUB, version=b"\x01\x02\x03\x04")
    with pytest.raises(InvalidBitcoinInput, match="Unknown extended key version"):
        btc.parse_extended_pubkey(bad)


def test_xprv_is_rejected_with_a_warning():
    with pytest.raises(InvalidBitcoinInput, match="PRIVATE key \\(xprv\\)"):
        btc.parse_extended_pubkey(TV1_M_XPRV)


@pytest.mark.parametrize("version", ["yprv", "zprv", "tprv"])
def test_other_private_versions_are_rejected(version):
    node = oracle.account_node(84)
    with pytest.raises(InvalidBitcoinInput, match="PRIVATE"):
        btc.parse_extended_pubkey(node.serialize_prv(version))


@pytest.mark.parametrize(
    ("version", "match"),
    [("tpub", "testnet"), ("vpub", "testnet"), ("Ypub", "Multisig"), ("Zpub", "Multisig")],
)
def test_testnet_and_multisig_versions_are_rejected(version, match):
    with pytest.raises(InvalidBitcoinInput, match=match):
        btc.parse_extended_pubkey(reserialize(BIP84_ZPUB, version))


def test_invalid_bitcoin_input_is_a_value_error():
    assert issubclass(InvalidBitcoinInput, ValueError)


# ── bech32 / bech32m vectors (BIP173, BIP350) ────────────────────────────────

BECH32_VALID = [
    "A12UEL5L",
    "a12uel5l",
    "an83characterlonghumanreadablepartthatcontainsthenumber1andtheexcludedcharactersbio1tt5tgs",
    "abcdef1qpzry9x8gf2tvdw0s3jn54khce6mua7lmqqqxw",
    "11" + "q" * 82 + "c8247j",
    "split1checkupstagehandshakeupstreamerranterredcaperred2y9e3w",
    "?1ezyfcl",
]
BECH32M_VALID = [
    "A1LQFN3A",
    "a1lqfn3a",
    "an83characterlonghumanreadablepartthatcontainsthetheexcludedcharactersbioandnumber11sg7hg6",
    "abcdef1l7aum6echk45nj3s0wdvt2fg8x9yrzpqzd3ryx",
    "11" + "l" * 82 + "ludsr8",
    "split1checkupstagehandshakeupstreamerranterredcaperredlc445v",
    "?1v759aa",
]
BECH32_INVALID = [
    "\x201nwldj5",  # HRP character out of range
    "\x7f1axkwrx",
    "\x801eym55h",
    "an84characterslonghumanreadablepartthatcontainsthenumber1andtheexcludedcharactersbio1569pvx",
    "pzry9x0s0muk",  # no separator
    "1pzry9x0s0muk",  # empty HRP
    "x1b4n0q5v",  # invalid data character
    "li1dgmt3",  # too short checksum
    "de1lg7wt\xff",  # invalid character in checksum
    "A1G7SGD8",  # checksum computed with uppercase HRP
    "10a06t8",  # empty HRP
    "1qzzfhee",  # empty HRP
]


@pytest.mark.parametrize("text", BECH32_VALID)
def test_bip173_valid_bech32(text):
    hrp, data, variant = btc.bech32_decode(text)
    assert variant is btc.Bech32Variant.BECH32
    assert btc.bech32_encode(hrp, data, variant) == text.lower()


@pytest.mark.parametrize("text", BECH32M_VALID)
def test_bip350_valid_bech32m(text):
    hrp, data, variant = btc.bech32_decode(text)
    assert variant is btc.Bech32Variant.BECH32M
    assert btc.bech32_encode(hrp, data, variant) == text.lower()


@pytest.mark.parametrize("text", BECH32_INVALID)
def test_bip173_invalid_bech32(text):
    with pytest.raises(ValueError):
        btc.bech32_decode(text)


SEGWIT_VALID = [
    ("BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4", "bc", 0, "751e76e8199196d454941c45d1b3a323f1433bd6"),
    ("tb1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3q0sl5k7", "tb", 0,
     "1863143c14c5166804bd19203356da136c985678cd4d27a1b8c6329604903262"),
    ("bc1pw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7kt5nd6y", "bc", 1,
     "751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d454941c45d1b3a323f1433bd6"),
    ("BC1SW50QGDZ25J", "bc", 16, "751e"),
    ("bc1zw508d6qejxtdg4y5r3zarvaryvaxxpcs", "bc", 2, "751e76e8199196d454941c45d1b3a323"),
    ("tb1pqqqqp399et2xygdj5xreqhjjvcmzhxw4aywxecjdzew6hylgvsesf3hn0c", "tb", 1,
     "000000c4a5cad46221b2a187905e5266362b99d5e91c6ce24d165dab93e86433"),
    ("bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0", "bc", 1,
     "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"),
]  # fmt: skip

SEGWIT_INVALID = [
    ("tc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vq5zuyut", "bc"),  # wrong hrp
    ("bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqh2y7hd", "bc"),  # v1 with bech32
    ("tb1z0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqglt7rf", "tb"),  # v2 with bech32
    ("BC1S0XLXVLHEMJA6C4DQV22UAPCTQUPFHLXM9H8Z3K2E72Q4K9HCZ7VQ54WELL", "bc"),  # v16 with bech32
    ("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kemeawh", "bc"),  # v0 with bech32m
    ("tb1q0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vq24jc47", "tb"),  # v0 with bech32m
    ("bc1p38j9r5y49hruaue7wxjce0updqjuyyx0kh56v8s25huc6995vvpql3jow4", "bc"),  # bad char
    ("BC130XLXVLHEMJA6C4DQV22UAPCTQUPFHLXM9H8Z3K2E72Q4K9HCZ7VQ7ZWS8R", "bc"),  # version 17
    ("bc1pw5dgrnzv", "bc"),  # program too short
    ("bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7v8n0nx0muaewav253zgeav", "bc"),
    ("BC1QR508D6QEJXTDG4Y5R3ZARVARYV98GJ9P", "bc"),  # v0 program of 16 bytes
    ("tb1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vq47Zagq", "tb"),  # mixed case
    ("bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7v07qwwzcrf", "bc"),  # padding > 4 bits
    ("tb1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vpggkg4j", "tb"),  # non-zero padding
    ("bc1gmk9yu", "bc"),  # empty data
]


@pytest.mark.parametrize(("address", "hrp", "version", "program"), SEGWIT_VALID)
def test_segwit_address_vectors(address, hrp, version, program):
    assert btc.decode_segwit_address(hrp, address) == (version, bytes.fromhex(program))
    assert btc.encode_segwit_address(hrp, version, bytes.fromhex(program)) == address.lower()


@pytest.mark.parametrize(("address", "hrp"), SEGWIT_INVALID)
def test_segwit_invalid_address_vectors(address, hrp):
    with pytest.raises(ValueError):
        btc.decode_segwit_address(hrp, address)


# ── user input validation ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2", "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"),
        ("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"),
        (BIP84_RECEIVE_0, BIP84_RECEIVE_0),
        (
            "  BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4\n",
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
        ),
        (
            "bc1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3qccfmv3",
            "bc1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3qccfmv3",
        ),
        (
            "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0",
            "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0",
        ),
    ],
)
def test_validate_btc_input_accepts_mainnet_addresses(text, expected):
    assert btc.validate_btc_input(text) == ("address", expected)


@pytest.mark.parametrize("text", [BIP44_XPUB, BIP49_YPUB, BIP84_ZPUB, f" {BIP84_ZPUB}\n"])
def test_validate_btc_input_accepts_extended_public_keys(text):
    kind, value = btc.validate_btc_input(text)
    assert kind == "xpub"
    assert isinstance(value, btc.ExtendedPubKey)
    assert value.to_base58() == text.strip()


def _testnet_p2pkh() -> str:
    return btc.b58check_encode(b"\x6f" + bytes(range(20)))


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("", "Please send"),
        ("   ", "Please send"),
        ("tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx", "testnet"),
        ("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t5", "invalid"),  # checksum typo
        ("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kemeawh", "invalid"),  # v0 + bech32m
        ("bc1zw508d6qejxtdg4y5r3zarvaryvaxxpcs", "not supported"),  # witness v2
        ("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN3", "Checksum"),
        ("5HueCGU8rMjxEXxiPuD5BDku4MkFqeZyd4dZ1jvhTVqvbTLvyTJ", "PRIVATE"),  # WIF
        ("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed", "Not a Bitcoin address"),
        ("hello", "Not a Bitcoin address"),
        (TV1_M_XPRV, "PRIVATE"),
        ("x" * 200, "too long"),
    ],
)
def test_validate_btc_input_rejects_with_friendly_messages(text, match):
    with pytest.raises(InvalidBitcoinInput, match=match):
        btc.validate_btc_input(text)


def test_validate_btc_input_rejects_testnet_base58():
    with pytest.raises(InvalidBitcoinInput, match="testnet"):
        btc.validate_btc_input(_testnet_p2pkh())


def test_validate_btc_input_rejects_non_strings():
    with pytest.raises(InvalidBitcoinInput):
        btc.validate_btc_input(None)  # type: ignore[arg-type]


# ── xpub scanning ────────────────────────────────────────────────────────────


class FakeChain:
    """Address stats keyed by address; records the query order."""

    def __init__(self, funded: dict[str, tuple[int, int]] | None = None) -> None:
        self.funded = funded or {}
        self.queried: list[str] = []

    def __call__(self, address: str) -> tuple[int, int]:
        self.queried.append(address)
        return self.funded.get(address, (0, 0))


def zpub_address(chain: int, index: int) -> str:
    return btc.derive_address(
        btc.parse_extended_pubkey(BIP84_ZPUB), ScriptType.P2WPKH, chain, index
    )


def test_scan_gap_resets_on_every_used_address():
    used = {zpub_address(0, i): (1_000 * (i + 1), 1) for i in (0, 2, 5)}
    chain = FakeChain(used)
    result = btc.scan_xpub(BIP84_ZPUB, chain, gap_limit=3)
    # receive: 0 used, 1, 2 used, 3, 4, 5 used, 6, 7, 8 → stop (9 queried); change: 0..2.
    assert chain.queried == [zpub_address(0, i) for i in range(9)] + [
        zpub_address(1, i) for i in range(3)
    ]
    assert result.scanned == 12
    assert result.used_addresses == 3
    assert result.balance_sats == 1_000 + 3_000 + 6_000


def test_scan_does_not_look_beyond_the_gap_limit():
    chain = FakeChain({zpub_address(0, 4): (5_000, 1)})
    result = btc.scan_xpub(BIP84_ZPUB, chain, gap_limit=3)
    assert zpub_address(0, 4) not in chain.queried
    assert result.used_addresses == 0
    assert result.balance_sats == 0
    assert result.scanned == 6


def test_scan_counts_change_chain_and_mempool_style_zero_balances():
    funded = {zpub_address(1, 0): (0, 2), zpub_address(1, 1): (70_000, 1)}
    result = btc.scan_xpub(BIP84_ZPUB, FakeChain(funded), gap_limit=2)
    assert result.used_addresses == 2
    assert result.balance_sats == 70_000
    assert result.per_script_type[ScriptType.P2WPKH].scanned == result.scanned == 2 + 4


def test_scan_with_default_gap_limit_scans_twenty_per_chain():
    chain = FakeChain()
    result = btc.scan_xpub(btc.parse_extended_pubkey(BIP84_ZPUB), chain)
    assert result.scanned == 40
    assert result.used_addresses == 0
    assert len(set(chain.queried)) == 40


def test_scan_fetch_errors_propagate_without_partial_result():
    calls = 0

    def flaky(address: str) -> tuple[int, int]:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise ExternalServiceError("boom", service="Blockstream", retriable=True)
        return (10, 1)

    with pytest.raises(ExternalServiceError):
        btc.scan_xpub(BIP84_ZPUB, flaky, gap_limit=2)


def test_scan_limit_guards_against_runaway_scans():
    with pytest.raises(btc.ScanLimitExceeded) as info:
        btc.scan_xpub(BIP84_ZPUB, lambda a: (1, 1), gap_limit=2, max_addresses_per_chain=5)
    assert isinstance(info.value, YnabHelperError)


def test_scan_rejects_invalid_arguments():
    with pytest.raises(ValueError):
        btc.scan_xpub(BIP84_ZPUB, FakeChain(), gap_limit=0)
    with pytest.raises(InvalidBitcoinInput):
        btc.scan_xpub(BIP84_ZPUB[:-1], FakeChain())
    with pytest.raises(ValueError, match="non-negative"):
        btc.scan_xpub(BIP84_ZPUB, lambda a: (-1, 1), gap_limit=1)


# ── Blockstream client ───────────────────────────────────────────────────────


class FakeRequest:
    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls: list[tuple[tuple, dict]] = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def esplora(address, chain=(0, 0, 0), mempool=(0, 0, 0)):
    def stats(values):
        return dict(zip(("funded_txo_sum", "spent_txo_sum", "tx_count"), values, strict=True))

    return {"address": address, "chain_stats": stats(chain), "mempool_stats": stats(mempool)}


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def make_client(request, **kwargs):
    sleep = kwargs.pop("sleep", SleepRecorder())
    return btc.BlockstreamClient(
        request=request, sleep=sleep, clock=kwargs.pop("clock", FakeClock()), **kwargs
    ), sleep


def test_blockstream_address_stats_sums_chain_and_mempool():
    reply = esplora(BIP84_RECEIVE_0, chain=(150_000, 50_000, 3), mempool=(1_000, 0, 1))
    request = FakeRequest(reply)
    client, _ = make_client(request, base_url="https://esplora.example/api/")
    assert client.address_stats(BIP84_RECEIVE_0) == (101_000, 4)
    (args, kwargs) = request.calls[0]
    assert args == ("GET", f"https://esplora.example/api/address/{BIP84_RECEIVE_0}")
    assert kwargs["timeout"] == 15.0
    assert kwargs["retries"] == 4
    assert kwargs["service"] == "Blockstream"


def test_blockstream_mempool_spend_of_confirmed_coins():
    reply = esplora(BIP84_RECEIVE_0, chain=(100, 0, 1), mempool=(0, 100, 1))
    client, _ = make_client(FakeRequest(reply))
    assert client.address_stats(BIP84_RECEIVE_0) == (0, 2)


@pytest.mark.parametrize(
    "reply",
    [
        None,
        [],
        {"chain_stats": {"funded_txo_sum": 1, "spent_txo_sum": 0, "tx_count": 1}},
        esplora(BIP84_RECEIVE_0, chain=(-1, 0, 1)),
        esplora(BIP84_RECEIVE_0, chain=(True, 0, 1)),
        esplora(BIP84_RECEIVE_0, chain=(1.5, 0, 1)),
        esplora(BIP84_RECEIVE_0, chain=("100", 0, 1)),
        esplora(BIP84_RECEIVE_1, chain=(100, 0, 1)),
        esplora(BIP84_RECEIVE_0, chain=(100, 200, 2)),
        esplora(BIP84_RECEIVE_0, chain=(100, 0, 0)),
    ],
    ids=[
        "null",
        "list",
        "mempool-missing",
        "negative",
        "bool",
        "float",
        "string",
        "other-address",
        "overspent",
        "balance-without-tx",
    ],
)
def test_blockstream_malformed_replies_raise_instead_of_reading_zero(reply):
    client, _ = make_client(FakeRequest(reply))
    with pytest.raises(ExternalServiceError) as info:
        client.address_stats(BIP84_RECEIVE_0)
    assert info.value.service == "Blockstream"


def test_blockstream_requests_are_paced_by_min_interval():
    clock = FakeClock()
    sleep = SleepRecorder()

    def advancing_sleep(seconds: float) -> None:
        sleep(seconds)
        clock.now += seconds

    request = FakeRequest(*[esplora(BIP84_RECEIVE_0) for _ in range(3)])
    client = btc.BlockstreamClient(
        request=request, min_interval=0.25, sleep=advancing_sleep, clock=clock
    )
    client.address_stats(BIP84_RECEIVE_0)  # first call never waits
    clock.now += 0.1
    client.address_stats(BIP84_RECEIVE_0)  # 0.1 s later → waits the remaining 0.15 s
    clock.now += 1.0
    client.address_stats(BIP84_RECEIVE_0)  # long after → no wait
    assert sleep.calls == [pytest.approx(0.15)]


def test_blockstream_errors_propagate():
    error = ExternalServiceError("down", service="Blockstream", retriable=True)
    client, _ = make_client(FakeRequest(error))
    with pytest.raises(ExternalServiceError, match="down"):
        client.address_stats(BIP84_RECEIVE_0)


def test_F78_blockstream_read_timeout_is_retried_and_the_scan_continues():
    """A TimeoutError while reading the body used to abort the whole xpub scan."""
    opener = FakeOpener(
        FakeResponse(read_error=TimeoutError("The read operation timed out")),
        json_response(esplora(BIP84_RECEIVE_0, chain=(5_000, 0, 1))),
    )
    sleep = SleepRecorder()
    request = functools.partial(net.request_json, opener=opener)
    client = btc.BlockstreamClient(request=request, sleep=sleep, clock=FakeClock())
    assert client.address_stats(BIP84_RECEIVE_0) == (5_000, 1)
    assert len(opener.calls) == 2
    assert len(sleep.calls) == 1  # one backoff wait, no pacing wait
