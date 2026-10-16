"""Tests for ynab_helper.revolut.files: safe upload storage (F19/F83/F84) and CLI discovery."""

from __future__ import annotations

import hashlib
import os
import stat
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.revolut.csvbuild import HEADER, make_csv, row
from ynab_helper.errors import CsvError, YnabHelperError
from ynab_helper.revolut import files, parser
from ynab_helper.revolut.files import (
    KEEP_STORED,
    STORED_NAME_RE,
    find_latest_revolut_csv,
    is_revolut_csv,
    latest_stored,
    list_stored,
    prune_stored,
    sanitize_display_name,
    store_upload,
)
from ynab_helper.revolut.parser import parse_export


def statement(tag: str = "a") -> bytes:
    """A valid statement whose content identifies it."""
    return make_csv(row(desc=f"payee-{tag}"))


def snapshot(root: Path) -> dict[str, bytes]:
    """Every regular file under ``root`` → content (symlinks recorded, not followed)."""
    result: dict[str, bytes] = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            result[str(path.relative_to(root))] = b"<symlink>"
        elif path.is_file():
            result[str(path.relative_to(root))] = path.read_bytes()
    return result


@pytest.fixture
def stamps(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Make utc_stamp() return strictly increasing stamps (one per call)."""
    issued: list[str] = []

    def fake_stamp() -> str:
        stamp = f"20261016T{len(issued):06d}Z"
        issued.append(stamp)
        return stamp

    monkeypatch.setattr(files, "utc_stamp", fake_stamp)
    yield issued


# ─── store_upload ─────────────────────────────────────────────────────────────


def test_store_upload_writes_atomically_under_a_generated_name(tmp_path: Path) -> None:
    csv_dir = tmp_path / "csv_cache" / "100"
    content = statement()
    stored = store_upload(csv_dir, content, "account-statement_2026-10-01_2026-10-16_en.csv")

    assert stored.path.parent == csv_dir
    assert STORED_NAME_RE.match(stored.path.name)
    assert stored.path.read_bytes() == content
    assert stored.sha256 == hashlib.sha256(content).hexdigest()
    assert stored.size == len(content)
    assert stored.original_name == "account-statement_2026-10-01_2026-10-16_en.csv"
    assert stat.S_IMODE(stored.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(csv_dir.stat().st_mode) & 0o077 == 0
    assert sorted(os.listdir(csv_dir)) == [stored.path.name]  # no temp leftovers


def test_store_upload_accepts_bytearray_and_none_name(tmp_path: Path) -> None:
    stored = store_upload(tmp_path, bytearray(statement()), None)
    assert stored.original_name is None
    assert stored.path.read_bytes() == statement()


def test_stored_file_parses_with_original_name_metadata(tmp_path: Path) -> None:
    stored = store_upload(
        tmp_path, statement(), "../account-statement_2026-10-01_2026-10-16_en_x.csv"
    )
    export = parse_export(stored.path, filename=stored.original_name)
    assert export.filename_window == ("2026-10-01", "2026-10-16")
    assert export.sha256 == stored.sha256


TRAVERSAL_NAMES = [
    "../100/revolut_evil.csv",
    "../../x.csv",
    "../../../../../../etc/cron.d/evil.csv",
    "/etc/passwd.csv",
    "..\\..\\windows.csv",
    "C:\\Users\\victim\\x.csv",
    "x/../../y.csv",
    "evil\x00.csv",
    "evil.csv\x00../../z",
    "a" * 10_000 + ".csv",
    "",
    ".",
    "..",
    "/",
    "\u202eevil.csv",
    "line\nbreak.csv",
    "~/.ssh/authorized_keys.csv",
    "%2e%2e%2fencoded.csv",
]


def test_F19_traversal_names_never_reach_the_filesystem(tmp_path: Path) -> None:
    data_dir = tmp_path / "bot_data"
    victim_dir = data_dir / "csv_cache" / "100"
    victim_dir.mkdir(parents=True)
    (victim_dir / "legit.csv").write_bytes(statement("victim"))
    attacker_dir = data_dir / "csv_cache" / "200"
    absolute = tmp_path / "absolute.csv"
    before = snapshot(tmp_path)

    for name in [*TRAVERSAL_NAMES, str(absolute)]:
        stored = store_upload(attacker_dir, statement("attacker"), name)
        assert stored.path.parent == attacker_dir
        assert STORED_NAME_RE.match(stored.path.name)
        assert stored.path.resolve().parent == attacker_dir.resolve()
        shown = stored.original_name
        if shown is not None:
            assert "/" not in shown and "\\" not in shown and "\x00" not in shown
            assert "\n" not in shown and "\u202e" not in shown
            assert len(shown) <= files.DISPLAY_NAME_MAX

    after = snapshot(tmp_path)
    changed = {k for k in before.keys() | after.keys() if before.get(k) != after.get(k)}
    assert changed and all(k.startswith("bot_data/csv_cache/200/") for k in changed)
    assert not absolute.exists()
    assert (victim_dir / "legit.csv").read_bytes() == statement("victim")
    assert len(list_stored(attacker_dir)) == KEEP_STORED


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"date,amount,payee\n2026-01-01,1,x\n",
        b"\x89PNG\r\n\x1a\n",
        make_csv(row()).decode().encode("utf-16"),
        make_csv(row(), header=[h for h in HEADER if h != "State"]),
    ],
)
def test_F84_invalid_upload_does_not_touch_the_cache(tmp_path: Path, content: bytes) -> None:
    good = store_upload(tmp_path, statement("good"), "good.csv")
    before = snapshot(tmp_path)
    with pytest.raises(CsvError) as info:
        store_upload(tmp_path, content, "account-statement_2026-01-01_2026-10-16_x.csv")
    assert info.value.user_message
    assert snapshot(tmp_path) == before
    assert latest_stored(tmp_path) == good.path


_UNPARSEABLE_UPLOADS = {
    "non_utf8_after_header": make_csv(row()) + "Café".encode("latin-1") + b"\n",
    "mixed_currency": make_csv(row(currency="CHF"), row(currency="EUR")),
    "invalid_currency": make_csv(row(currency="X" * 1000)),
    "duplicate_column": make_csv(header=[*HEADER, "Amount"]),
    "field_limit": make_csv(row(desc="x" * 200_000)),
    "too_many_columns": make_csv(header=[*HEADER, *(f"c{i}" for i in range(100))]),
}


@pytest.mark.parametrize("kind", sorted(_UNPARSEABLE_UPLOADS))
def test_F84_header_valid_but_unparseable_upload_is_not_stored(
    tmp_path: Path, stamps: list[str], kind: str
) -> None:
    """These pass the header sniff; v2 parses before writing, so none of them is stored
    and none can evict a good statement (five in a row used to empty the cache)."""
    content = _UNPARSEABLE_UPLOADS[kind]
    assert parser.looks_like_revolut_csv(content)
    good = store_upload(tmp_path, statement("good"), "good.csv")
    before = snapshot(tmp_path)
    for _ in range(KEEP_STORED + 1):
        with pytest.raises(CsvError) as info:
            store_upload(tmp_path, content, "account-statement_2026-01-01_2026-10-16_x.csv")
        assert info.value.user_message
    assert snapshot(tmp_path) == before
    assert latest_stored(tmp_path) == good.path


def test_F84_protected_statement_survives_refused_uploads(
    tmp_path: Path, stamps: list[str]
) -> None:
    """A statement that parses but is refused later (here: no tracked rows) is stored and
    counts toward retention; callers protect the newest imported statement."""
    good = store_upload(tmp_path, statement("good"), "good.csv")
    savings_only = make_csv(row(product="Savings"))
    for _ in range(KEEP_STORED + 2):
        stored = store_upload(tmp_path, savings_only, None, protect=[good.path])
        assert stored.export.rows == [] and stored.export.other_products == {"Savings": 1}
    assert good.path.read_bytes() == statement("good")
    assert len(list_stored(tmp_path)) == KEEP_STORED + 1
    # Without protection the oldest statement goes once five newer ones exist.
    store_upload(tmp_path, savings_only, None)
    assert not good.path.exists()
    assert len(list_stored(tmp_path)) == KEEP_STORED


def test_store_upload_returns_the_parse_of_the_stored_bytes(tmp_path: Path) -> None:
    stored = store_upload(
        tmp_path, statement("x"), "account-statement_2026-10-01_2026-10-16_en.csv"
    )
    again = parse_export(stored.path, filename=stored.original_name)
    assert stored.export.rows == again.rows
    assert stored.export.sha256 == again.sha256 == stored.sha256
    assert stored.export.filename_window == ("2026-10-01", "2026-10-16")
    assert "export" not in repr(stored)


def test_prune_protect_accepts_one_or_many_paths_in_any_spelling(
    tmp_path: Path, stamps: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    s0, s1, s2, s3 = (store_upload(tmp_path, statement(str(i)), None).path for i in range(4))
    monkeypatch.chdir(tmp_path.parent)
    relative = Path(tmp_path.name) / s0.name
    # keep=1 keeps the newest (s3); s0 and s1 are protected, spelled differently.
    assert prune_stored(tmp_path, keep=1, protect=[relative, str(s1)]) == [s2]
    assert list_stored(tmp_path) == [s3, s1, s0]
    assert prune_stored(tmp_path, keep=1, protect=s0) == [s1]  # a single path
    assert prune_stored(tmp_path, keep=1, protect=None) == [s0]
    assert list_stored(tmp_path) == [s3]


def test_F19_guard_failure_does_not_leak_the_server_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the generated name ever stops matching (a bug), the user sees a generic message;
    the server directory stays in the log."""
    csv_dir = tmp_path / "srv" / "csv_cache" / "100"
    for bad_stamp in ("../escape", "2026-10-16 12:00"):
        monkeypatch.setattr(files, "utc_stamp", lambda stamp=bad_stamp: stamp)
        with pytest.raises(YnabHelperError) as info:
            store_upload(csv_dir, statement(), "x.csv")
        assert info.value.user_message == "The statement could not be saved on the server."
        assert str(tmp_path) not in info.value.user_message
    assert snapshot(tmp_path) == {}  # nothing written anywhere (directories only)


def test_F84_oversized_upload_is_rejected_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = statement()
    monkeypatch.setattr(files, "MAX_CSV_BYTES", len(content) - 1)
    with pytest.raises(CsvError) as info:
        store_upload(tmp_path / "dir", content, "x.csv")
    assert info.value.user_message == "The statement is larger than 25 MB."
    assert not (tmp_path / "dir").exists()


def test_upload_size_limit_matches_the_parser() -> None:
    assert files.MAX_CSV_BYTES == parser.MAX_CSV_BYTES == 25 * 1024 * 1024


@pytest.mark.parametrize("failing", ["fsync", "replace", "write"])
def test_F83_failed_write_leaves_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failing: str
) -> None:
    good = store_upload(tmp_path, statement("good"), "good.csv")
    before = snapshot(tmp_path)

    def boom(*args: object, **kwargs: object) -> None:
        raise OSError(28, "No space left on device")

    if failing == "write":
        real_fdopen = os.fdopen

        class FailingFile:
            def __init__(self, fd: int, *a: object, **k: object) -> None:
                self._fh = real_fdopen(fd, "wb")

            def __enter__(self) -> FailingFile:
                return self

            def __exit__(self, *exc: object) -> None:
                self._fh.close()

            def write(self, data: bytes) -> int:
                self._fh.write(data[: len(data) // 2])  # half written, then the disk is full
                raise OSError(28, "No space left on device")

        monkeypatch.setattr(files.os, "fdopen", FailingFile)
    else:
        monkeypatch.setattr(files.os, failing, boom)

    with pytest.raises(YnabHelperError) as info:
        store_upload(tmp_path, statement("new"), "new.csv")
    assert not isinstance(info.value, CsvError)
    assert info.value.user_message == "The statement could not be saved on the server."
    assert snapshot(tmp_path) == before  # neither the final file nor the temp file exists
    assert latest_stored(tmp_path) == good.path


def test_F83_final_name_appears_only_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = statement("complete")
    real_replace = os.replace
    seen: list[tuple[str, str]] = []

    def checking_replace(src: str | Path, dst: str | Path) -> None:
        src, dst = Path(src), Path(dst)
        assert not dst.exists()  # readers cannot see the final name yet
        assert src.read_bytes() == content  # the temp file is complete and synced
        assert src.parent == dst.parent  # same directory → atomic rename
        seen.append((src.name, dst.name))
        real_replace(src, dst)

    monkeypatch.setattr(files.os, "replace", checking_replace)
    stored = store_upload(tmp_path, content, "x.csv")
    ((tmp_name, final_name),) = seen
    assert tmp_name.startswith(".") and final_name == stored.path.name
    assert latest_stored(tmp_path) == stored.path


def test_store_upload_into_a_file_path_fails_cleanly(tmp_path: Path) -> None:
    not_a_dir = tmp_path / "file"
    not_a_dir.write_text("x")
    with pytest.raises(YnabHelperError):
        store_upload(not_a_dir, statement(), "x.csv")


def test_store_upload_rejects_bad_keep(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        store_upload(tmp_path, statement(), "x.csv", keep=0)
    with pytest.raises(ValueError):
        prune_stored(tmp_path, keep=0)


def test_name_collisions_are_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(files, "utc_stamp", lambda: "20261016T120000Z")
    tokens = iter(["aaaaaaaa", "aaaaaaaa", "bbbbbbbb"])
    real_token_hex = files.secrets.token_hex
    monkeypatch.setattr(
        files.secrets, "token_hex", lambda n: next(tokens) if n == 4 else real_token_hex(n)
    )
    first = store_upload(tmp_path, statement("1"), None)
    second = store_upload(tmp_path, statement("2"), None)
    assert first.path.name == "20261016T120000Z_aaaaaaaa.csv"
    assert second.path.name == "20261016T120000Z_bbbbbbbb.csv"
    assert first.path.read_bytes() == statement("1")


# ─── Retention and ordering ──────────────────────────────────────────────────


def test_retention_keeps_the_newest_five(tmp_path: Path, stamps: list[str]) -> None:
    stored = [store_upload(tmp_path, statement(str(i)), f"{i}.csv") for i in range(7)]
    remaining = list_stored(tmp_path)
    assert remaining == [s.path for s in reversed(stored[2:])]
    assert [p.read_bytes() for p in remaining] == [statement(str(i)) for i in range(6, 1, -1)]
    assert latest_stored(tmp_path) == stored[-1].path


def test_retention_treats_v1_files_as_older_and_ignores_other_entries(
    tmp_path: Path, stamps: list[str]
) -> None:
    legacy = tmp_path / "account-statement_2026-01-01_2026-10-01_en_x.csv"
    legacy.write_bytes(statement("legacy"))
    future = time.time() + 86_400
    os.utime(legacy, (future, future))  # even a "newer" mtime ranks below v2 files
    keepers = {
        ".hidden.csv": b"x",
        "notes.txt": b"x",
        "README": b"x",
    }
    for name, data in keepers.items():
        (tmp_path / name).write_bytes(data)
    (tmp_path / "subdir.csv").mkdir()
    outside = tmp_path.parent / f"{tmp_path.name}-outside.csv"
    outside.write_bytes(statement("outside"))
    (tmp_path / "link.csv").symlink_to(outside)

    assert latest_stored(tmp_path) == legacy  # before any v2 upload
    first = store_upload(tmp_path, statement("v2-0"), None)
    assert latest_stored(tmp_path) == first.path
    for i in range(1, 4):
        store_upload(tmp_path, statement(f"v2-{i}"), None)
    assert legacy.exists()  # 4 v2 files + legacy = 5
    store_upload(tmp_path, statement("v2-4"), None)
    assert not legacy.exists()  # the 6th statement pushes the v1 file out

    for name, data in keepers.items():
        assert (tmp_path / name).read_bytes() == data
    assert (tmp_path / "subdir.csv").is_dir()
    assert (tmp_path / "link.csv").is_symlink() and outside.exists()
    assert len(list_stored(tmp_path)) == KEEP_STORED


def test_just_written_file_is_never_pruned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """With a clock that went backwards, the new file ranks oldest; it is still kept."""
    for i in range(KEEP_STORED):
        (tmp_path / f"20991231T00000{i}Z_0000000{i}.csv").write_bytes(statement(str(i)))
    monkeypatch.setattr(files, "utc_stamp", lambda: "20261016T120000Z")
    stored = store_upload(tmp_path, statement("new"), None)
    assert stored.path.exists()
    assert len(list_stored(tmp_path)) == KEEP_STORED + 1


def test_ordering_is_by_stamp_then_mtime(tmp_path: Path) -> None:
    old_stamp = tmp_path / "20261015T000000Z_ffffffff.csv"
    new_stamp = tmp_path / "20261016T000000Z_00000000.csv"
    same_a = tmp_path / "20261017T000000Z_aaaaaaaa.csv"
    same_b = tmp_path / "20261017T000000Z_bbbbbbbb.csv"
    for path in (old_stamp, new_stamp, same_a, same_b):
        path.write_bytes(statement())
    os.utime(old_stamp, (2_000_000_000, 2_000_000_000))  # newest mtime, oldest stamp
    os.utime(new_stamp, (1_000_000_000, 1_000_000_000))
    os.utime(same_a, (1_500_000_100, 1_500_000_100))  # same stamp: newer mtime wins
    os.utime(same_b, (1_500_000_000, 1_500_000_000))
    assert list_stored(tmp_path) == [same_a, same_b, new_stamp, old_stamp]


def test_prune_removes_only_stale_temp_files(tmp_path: Path) -> None:
    stale = tmp_path / ".upload-0123456789abcdef.tmp"
    fresh = tmp_path / ".upload-fedcba9876543210.tmp"
    other_hidden = tmp_path / ".something.tmp"
    for path in (stale, fresh, other_hidden):
        path.write_bytes(b"partial")
    two_hours_ago = time.time() - 7200
    os.utime(stale, (two_hours_ago, two_hours_ago))
    os.utime(other_hidden, (two_hours_ago, two_hours_ago))
    removed = prune_stored(tmp_path)
    assert removed == [stale]
    assert fresh.exists() and other_hidden.exists()


@pytest.mark.parametrize("kind", ["missing", "file", "empty"])
def test_latest_stored_without_statements(tmp_path: Path, kind: str) -> None:
    target = tmp_path / "dir"
    if kind == "file":
        target.write_text("x")
    elif kind == "empty":
        target.mkdir()
    assert latest_stored(target) is None
    assert list_stored(target) == []
    if kind != "file":
        assert prune_stored(target) == []


# ─── sanitize_display_name ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (None, None),
        (
            "account-statement_2026-01-01_2026-10-16_en_x.csv",
            "account-statement_2026-01-01_2026-10-16_en_x.csv",
        ),
        ("../../x.csv", "x.csv"),
        ("/etc/passwd", "passwd"),
        ("C:\\Users\\a\\b.csv", "b.csv"),
        ("a\x00b.csv", "ab.csv"),
        ("\u202eevil\u200b.csv", "evil.csv"),
        ("  spaced \t  name .csv  ", "spaced name .csv"),
        ("line\nbreak.csv", "linebreak.csv"),
        ("\u2028para\u2029sep.csv", "para sep.csv"),
        ("Ünïcödé 💳.csv", "Ünïcödé 💳.csv"),
        ("Cafe\u0301.csv", "Café.csv"),  # NFC
        ("", None),
        ("   ", None),
        (".", None),
        ("..", None),
        ("dir/", None),
        ("\x00\x01", None),
    ],
)
def test_sanitize_display_name(name: str | None, expected: str | None) -> None:
    assert sanitize_display_name(name) == expected


def test_sanitize_display_name_truncates_keeping_the_extension() -> None:
    long_csv = sanitize_display_name("x" * 300 + ".csv")
    assert long_csv is not None and len(long_csv) == files.DISPLAY_NAME_MAX
    assert long_csv.endswith("….csv")
    no_ext = sanitize_display_name("y" * 300)
    assert no_ext is not None and len(no_ext) == files.DISPLAY_NAME_MAX
    assert no_ext.endswith("…")
    long_ext = sanitize_display_name("z" * 300 + "." + "e" * 20)
    assert long_ext is not None and len(long_ext) == files.DISPLAY_NAME_MAX
    assert sanitize_display_name("abc.csv", max_length=8) == "abc.csv"
    with pytest.raises(ValueError):
        sanitize_display_name("abc", max_length=3)


# ─── CLI discovery ───────────────────────────────────────────────────────────


def _touch(path: Path, data: bytes, mtime: float) -> Path:
    path.write_bytes(data)
    os.utime(path, (mtime, mtime))
    return path


def test_find_latest_ranks_by_export_to_date_then_mtime(tmp_path: Path) -> None:
    older_export = _touch(
        tmp_path / "account-statement_2026-01-01_2026-09-30_en_a.csv", statement(), 3_000
    )
    newest_export = _touch(
        tmp_path / "account-statement_2026-06-01_2026-10-16_en_b.csv", statement(), 1_000
    )
    same_day_newer = _touch(
        tmp_path / "ACCOUNT-STATEMENT_2026-01-01_2026-10-16_EN_C.CSV", statement(), 2_000
    )
    undated = _touch(tmp_path / "export.csv", statement(), 9_000)
    assert find_latest_revolut_csv(tmp_path) == same_day_newer
    same_day_newer.unlink()
    assert find_latest_revolut_csv(tmp_path) == newest_export
    newest_export.unlink()
    older_export.unlink()
    assert find_latest_revolut_csv(tmp_path) == undated


def test_find_latest_ignores_non_revolut_files(tmp_path: Path) -> None:
    _touch(tmp_path / "revolut_notes.csv", b"just,some\nnotes,here\n", 9_000)
    _touch(tmp_path / "other.csv", b"date,amount\n", 9_000)
    _touch(tmp_path / "statement.txt", statement(), 9_000)
    (tmp_path / "account-statement_2026-01-01_2026-10-16_en_d.csv").mkdir()
    assert find_latest_revolut_csv(tmp_path) is None
    # A dated name without the trailing "_…" part only qualifies by content.
    sniffed = _touch(tmp_path / "account-statement_2026-01-01_2026-10-16.csv", statement(), 1)
    assert find_latest_revolut_csv(tmp_path) == sniffed


def test_find_latest_handles_missing_dirs_and_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert find_latest_revolut_csv(tmp_path / "missing") is None
    a_file = _touch(tmp_path / "f.csv", statement(), 1)
    assert find_latest_revolut_csv(a_file) is None
    downloads = tmp_path / "home" / "Downloads"
    downloads.mkdir(parents=True)
    target = _touch(downloads / "account-statement_2026-01-01_2026-10-16_en_e.csv", b"x", 1)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    assert find_latest_revolut_csv("~/Downloads") == target


def test_is_revolut_csv(tmp_path: Path) -> None:
    assert is_revolut_csv(_touch(tmp_path / "a.csv", statement(), 1))
    assert is_revolut_csv(_touch(tmp_path / "B.CSV", statement(), 1))
    assert not is_revolut_csv(_touch(tmp_path / "a.txt", statement(), 1))
    assert not is_revolut_csv(_touch(tmp_path / "revolut.csv", b"not a statement", 1))
    assert not is_revolut_csv(tmp_path / "missing.csv")
    (tmp_path / "dir.csv").mkdir()
    assert not is_revolut_csv(tmp_path / "dir.csv")


def test_parse_filename_window_is_shared_with_the_parser() -> None:
    assert files.parse_filename_window is parser.parse_filename_window
