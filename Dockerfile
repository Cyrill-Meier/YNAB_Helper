FROM python:3.12-slim

LABEL org.opencontainers.image.source=https://github.com/Cyrill-Meier/YNAB_Helper

WORKDIR /app

# Native deps:
#   pycryptodome — RIPEMD160 for BTC xpub derivation (Debian bookworm
#                  ships OpenSSL 3 which disables it in default hashlib)
#   fastapi      — web UI backend
#   uvicorn      — ASGI server (no [standard] extras: we don't need
#                  websockets/httptools and they bring 30 MB of wheels)
#   jinja2       — server-rendered templates
#   itsdangerous — signed cookies (FastAPI's SessionMiddleware backend)
#   python-multipart — multipart/form-data parsing for CSV upload
RUN pip install --no-cache-dir \
    pycryptodome==3.20.0 \
    fastapi==0.115.4 \
    "uvicorn==0.32.0" \
    jinja2==3.1.4 \
    itsdangerous==2.2.0 \
    python-multipart==0.0.17

# Copy bot scripts plus the web package (templates + static + server).
COPY revolut_to_ynab.py revolut_ynab_bot.py ./
COPY web/ ./web/

# Build metadata (populated by GitHub Actions; defaults here for local builds)
ARG BUILD_SHA=local
ARG BUILD_DATE=local
ENV BUILD_SHA=${BUILD_SHA}
ENV BUILD_DATE=${BUILD_DATE}
LABEL org.opencontainers.image.revision=${BUILD_SHA}
LABEL org.opencontainers.image.created=${BUILD_DATE}

# bot_data/ and .env are mounted as volumes at runtime
VOLUME ["/app/bot_data"]

# Web UI listens on 8080 inside the container; expose for the cloudflared
# sidecar (or any reverse proxy) to forward to.
EXPOSE 8080

CMD ["python3", "revolut_ynab_bot.py"]
