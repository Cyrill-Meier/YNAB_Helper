"""Telegram bot (docs/DESIGN.md §8).

Modules:

* :mod:`.telegram` — :class:`~.telegram.TelegramClient` over :mod:`ynab_helper.net`
  (typed errors, HTML messages chunked by UTF-16 code units, file downloads);
* :mod:`.render` — HTML formatters for every service result (everything escaped);
* :mod:`.auth` — the single authorization gate for messages *and* callback queries;
* :mod:`.commands` — the command registry behind dispatch, ``/help`` and ``setMyCommands``;
* :mod:`.handlers` — one module per feature (onboarding, import, reconcile, dedupe, …);
* :mod:`.app` — the runtime: poll loop, offset, backoff, heartbeat, workers, shutdown;
* :mod:`.main` — the entry point used by ``python3 revolut_ynab_bot.py`` and the container.

This ``__init__`` imports nothing so ``import ynab_helper.bot`` stays cheap.
"""
