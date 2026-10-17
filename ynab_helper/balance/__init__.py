"""Balance valuation and adjustments: reconcile, crypto and IBKR (DESIGN §6.6).

Submodules are imported explicitly by their users; this package deliberately has no
eager imports so that importing one valuation source never pulls in the others.
"""
