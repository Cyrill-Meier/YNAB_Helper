# Test fixtures: synthetic Revolut exports

Every file here is **synthetic** — invented merchants, people and amounts, never real
account data. The regular fixtures mimic the format of a real Revolut account statement
as verified on production (docs/DESIGN.md §2.9):

* header `Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance`;
* rows ordered by **Completed Date ascending**;
* PENDING and REVERTED rows have an empty Completed Date and Balance and are
  **interleaved** at their Started Date (not appended at the end);
* Started Date is therefore **not monotonic** (card payments complete a day or two later);
* Balance is the running balance per product/currency after each COMPLETED row
  (Amount − Fee).

`tests/revolut/test_fixtures.py` checks these properties for every regular fixture.
`tests/revolut/test_parser_golden.py` parses each one with the v2 parser and with the frozen
v1 parser (`tests/legacy/v1_revolut_to_ynab.py`) and requires byte-identical import_ids,
amounts, dates, memos and payees; it also pins the expected keys (`FROZEN`), so a fixture
change must update that table.

| File | What it exercises |
|---|---|
| `export_basic.csv` | Typical CHF month: top-up, card payments, transfers (in/out), exchange, refund, ATM, quoted descriptions with comma and quotes, two same-amount same-day pairs (SBB −3.60, Starbucks −4.50), one REVERTED hold, PENDING rows interleaved, a PENDING and a COMPLETED Coop −8.40 on the same day (the pending gets occurrence 1). |
| `export_tz_shift.csv` | `export_basic.csv` with every timestamp rendered **+9 h** (phone in another timezone). The Starbucks −4.50 of 10-08 23:30 moves to 10-09 and takes the key `YNAB:-4500:2026-10-09:1` that belonged to another coffee in the basic export (F34). Balances and order are unchanged. |
| `export_pending_then_completed_a.csv` | Earlier export of a period: three PENDING rows (hotel hold −150.00, Migros −45.30, Amazon −89.99). |
| `export_pending_then_completed_b.csv` | Later export of the same period: Migros completed at the same amount and **moved** (Completed Date order), Amazon **settled at −90.41** (FX re-quote), the hotel hold is **REVERTED**, and a new Coop pending appears. |
| `export_same_amount_same_day_a.csv` | C01 setup on 2026-10-05: SBB −3.60 PENDING 08:10 (key `…:1`) and COMPLETED 17:40 (`…:2`); three Starbucks −4.50 at 08:05 / 12:30 (PENDING) / 16:45 (`…:1/2/3`). |
| `export_same_amount_same_day_b.csv` | Same day later: the 08:10 SBB settled at −3.80, so the 17:40 ride is renumbered to `YNAB:-3600:2026-10-05:1` — the pending's old key (C01); the 12:30 coffee completed the next day and moved behind 16:45, so keys `…:2` and `…:3` swap. |
| `export_fees.csv` | Fee handling and the v1 memo text `Fee: {float}`: `1.25`, `0.37`, `1.0`, `2` → `Fee: 2.0`, `0.005`, `0.0005`, empty and `0`/`0.00` fees (no memo part), a PENDING row with fee (`(pending) \| Fee: 0.2`), an empty Description (payee falls back to Type), banker's rounding of the float formula. |
| `export_other_products.csv` | `Savings` and `Deposit` product rows interleaved with `Current` rows (transfers to/from pockets, interest, a 0.00 pocket-closing row at the bottom). Other products are reported, never imported, and never consume occurrence numbers (the two Coop −100.00 rows keep `:1`/`:2` regardless of the +100.00 Savings rows). |
| `export_mixed_currency.csv` | `Current` rows in CHF **and** EUR → `CsvError` (F14). |
| `export_eur.csv` | A EUR account export saved with a **UTF-8 BOM and CRLF** line endings; one quoted description contains a comma and an embedded CRLF (normalised to `\n`, as v1 did). |
| `export_declined.csv` | DECLINED and FAILED rows (skipped and reported, F37) followed by same-amount rows: the skipped rows still consume an occurrence (`YNAB:-250000:2026-10-02:2`), as in v1; a REVERTED transfer followed by its re-sent COMPLETED twin. |
| `export_legacy_no_time.csv` | Old export format with dates only (`YYYY-MM-DD`, no time) in Started/Completed Date; a same-day duplicate pair; an empty Description. |
| `export_bad_rows.csv` | Hand-written, deliberately irregular (excluded from the ordering checks): empty/invalid/pre-1970 Started Date, invalid/empty/`nan`/`±inf`/`1e15` amounts, an `inf` fee, an unparseable fee (kept, fee 0 + warning, as v1), unknown and empty states, an invalid Balance and Completed Date (kept + warning), a truncated row, a short row missing only Balance (kept, as v1), a lower-case state, an untracked product row full of garbage, and a DECLINED row consuming an occurrence before a same-amount PENDING row (`…:3`). |

To change a regular fixture, keep the ordering rules above (the fixture tests will fail
otherwise), then regenerate the `FROZEN` table in `tests/revolut/test_parser_golden.py`
and confirm the oracle test still passes.

The bytes matter: `export_eur.csv` must keep its BOM and CRLF line endings, and every
other fixture is plain LF without a BOM. `tests/revolut/test_fixtures.py` checks both, so
line-ending normalisation (`core.autocrlf`, an editor, a `.gitattributes` rule) fails
loudly instead of silently dropping the BOM/CRLF coverage. These files should be marked
`-text` in `.gitattributes`.
