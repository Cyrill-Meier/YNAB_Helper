"""Web UI (docs/DESIGN.md §9): FastAPI app, middleware, auth routes and analytics.

This ``__init__`` deliberately imports nothing, so lightweight users (the bot's startup
code and the Docker health check) can import :mod:`ynab_helper.web.config` without
loading FastAPI.
"""
