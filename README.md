# Binance Pump Scanner v7.6 — Option B

No VPS required.

Architecture:
GitHub Actions -> Binance WebSocket -> local synchronized order book -> 250-second live scan -> Telegram + latest.json -> Streamlit Community Cloud.

GitHub Actions starts a fresh worker every 5 minutes. Each run keeps a WebSocket connection for about 250 seconds, leaving a small margin before the next scheduled run.

This is NOT continuous 24/7 WebSocket operation. Scheduled GitHub Actions can be delayed, so it is lower latency than pure REST polling but less reliable/timely than a VPS.

## GitHub setup

1. Create a GitHub repository.
2. Upload all files.
3. Go to Settings -> Secrets and variables -> Actions.
4. Add:
   TELEGRAM_BOT_TOKEN
   TELEGRAM_CHAT_ID
5. Go to Actions and enable workflows.
6. Run "Binance Pump Scanner v7.6" manually once to test.

The scheduled workflow uses:
cron: "*/5 * * * *"

## Streamlit

Deploy `app.py` from the GitHub repository on Streamlit Community Cloud.

Community Cloud can deploy directly from GitHub and updates when repository files change.

## Important

GitHub Actions is the scanner runtime here. Streamlit is the dashboard only.

Because GitHub scheduled workflows can be delayed, do not interpret the dashboard as a guaranteed 5-second live feed. During each active job, the worker makes decisions every 5 seconds; between jobs there can be a gap.

The scanner is decision support, not automatic trading advice.
