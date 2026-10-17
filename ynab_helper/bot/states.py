"""User states (``users.state``) grouped the way the bot's gate and handlers use them."""

from __future__ import annotations

from typing import Final

PENDING: Final = "pending"
DENIED: Final = "denied"
READY: Final = "ready"
AWAITING_TOKEN: Final = "awaiting_token"
AWAITING_BUDGET: Final = "awaiting_budget"
AWAITING_ACCOUNT: Final = "awaiting_account"
AWAITING_CRYPTO_ACCOUNT: Final = "awaiting_crypto_account"
AWAITING_CRYPTO_BTC: Final = "awaiting_crypto_btc"
AWAITING_CRYPTO_ETH: Final = "awaiting_crypto_eth"

#: Fully onboarded.
READY_STATES: Final = frozenset({READY})
#: Main onboarding (token → budget → account).
SETUP_STATES: Final = frozenset({AWAITING_TOKEN, AWAITING_BUDGET, AWAITING_ACCOUNT})
#: Crypto onboarding (tracking account → BTC → ETH); entered from ``ready``.
CRYPTO_STATES: Final = frozenset(
    {AWAITING_CRYPTO_ACCOUNT, AWAITING_CRYPTO_BTC, AWAITING_CRYPTO_ETH}
)
#: Approved by the admin (may use the bot in some way).
APPROVED_STATES: Final = READY_STATES | SETUP_STATES | CRYPTO_STATES
#: Not (or no longer) allowed to use the bot.
BLOCKED_STATES: Final = frozenset({PENDING, DENIED})
