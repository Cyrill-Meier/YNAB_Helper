"""Repair helpers that work on a freshly synced YNAB mirror (DESIGN §6.7).

* :mod:`.cleanup` — strip stale ``(pending)`` memo markers from cleared transactions;
* :mod:`.dedupe` — find importer-created transactions the ledger does not account for,
  and delete a confirmed selection of them.

Submodules are imported explicitly by their users.
"""
