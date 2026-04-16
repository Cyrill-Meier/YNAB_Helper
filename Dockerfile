FROM python:3.12-slim

LABEL org.opencontainers.image.source=https://github.com/Cyrill-Meier/YNAB_Helper

WORKDIR /app

# RIPEMD160 is required for deriving BTC addresses from an xpub (/crypto flow).
# Debian bookworm + OpenSSL 3 disables it in the default hashlib provider,
# so we install pycryptodome as a pure-Python fallback.
RUN pip install --no-cache-dir pycryptodome==3.20.0

# Copy only the scripts we need (no .env, no data)
COPY revolut_to_ynab.py revolut_ynab_bot.py ./

# Build metadata (populated by GitHub Actions; defaults here for local builds)
ARG BUILD_SHA=local
ARG BUILD_DATE=local
ENV BUILD_SHA=${BUILD_SHA}
ENV BUILD_DATE=${BUILD_DATE}
LABEL org.opencontainers.image.revision=${BUILD_SHA}
LABEL org.opencontainers.image.created=${BUILD_DATE}

# bot_data/ and .env are mounted as volumes at runtime
VOLUME ["/app/bot_data"]

CMD ["python3", "revolut_ynab_bot.py"]
