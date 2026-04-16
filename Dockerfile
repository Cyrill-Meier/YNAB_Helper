FROM python:3.12-slim

LABEL org.opencontainers.image.source=https://github.com/Cyrill-Meier/YNAB_Helper

WORKDIR /app

# Copy only the scripts we need (no .env, no data)
COPY revolut_to_ynab.py revolut_ynab_bot.py ./

# No pip dependencies — stdlib only

# bot_data/ and .env are mounted as volumes at runtime
VOLUME ["/app/bot_data"]

CMD ["python3", "revolut_ynab_bot.py"]
