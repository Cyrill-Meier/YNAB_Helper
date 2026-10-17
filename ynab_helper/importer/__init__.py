"""Revolut → YNAB importer: data model, pure planner and executor (docs/DESIGN.md §6).

* :mod:`.models` — :class:`PlanOptions`, :class:`PlannedAction`, :class:`Plan`,
  :class:`ImportResult` and friends.
* :mod:`.planner` — :func:`plan_import` (pure), :func:`compute_allow_sweep`, and the per-match
  logic the executor reuses (:func:`actions_for_match`, :func:`resolve_duplicate`).
* :mod:`.executor` — ``apply_plan``. Import it from ``ynab_helper.importer.executor``: the
  package does not import the executor (which needs the YNAB client, the mirror sync and
  SQLite), so ``import ynab_helper.importer`` loads only the data model and the pure planner.
"""

from __future__ import annotations

from ynab_helper.importer.models import (
    ActionKind,
    Change,
    CsvState,
    Failure,
    ImportResult,
    LedgerEntry,
    LedgerStatus,
    MatchInfo,
    MatchMethod,
    MatchOutcome,
    Plan,
    PlannedAction,
    PlanOptions,
    PlanSkip,
    Removal,
    TxRef,
    entry_update_fields,
)
from ynab_helper.importer.planner import (
    ImportIdAllocator,
    PlanContext,
    actions_for_match,
    compute_allow_sweep,
    plan_import,
    resolve_duplicate,
)

__all__ = [
    "ActionKind",
    "Change",
    "CsvState",
    "Failure",
    "ImportIdAllocator",
    "ImportResult",
    "LedgerEntry",
    "LedgerStatus",
    "MatchInfo",
    "MatchMethod",
    "MatchOutcome",
    "Plan",
    "PlanContext",
    "PlanOptions",
    "PlanSkip",
    "PlannedAction",
    "Removal",
    "TxRef",
    "actions_for_match",
    "compute_allow_sweep",
    "entry_update_fields",
    "plan_import",
    "resolve_duplicate",
]
