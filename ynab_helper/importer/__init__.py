"""Revolut → YNAB importer: data model, pure planner and executor (docs/DESIGN.md §6).

* :mod:`.models` — :class:`PlanOptions`, :class:`PlannedAction`, :class:`Plan`,
  :class:`ImportResult` and friends.
* :mod:`.planner` — :func:`plan_import` (pure), :func:`compute_allow_sweep`, and the per-match
  logic the executor reuses (:func:`actions_for_match`, :func:`resolve_duplicate`).
* :mod:`.executor` — ``apply_plan`` (loaded lazily, so the planner can be used on its own).
"""

from __future__ import annotations

from typing import Any

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


def __getattr__(name: str) -> Any:
    # The executor imports the YNAB client and the storage layer; keep it out of the
    # planner's import path and load it only when asked for.
    if name == "apply_plan":
        from ynab_helper.importer.executor import apply_plan

        return apply_plan
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
