# ClearMap Screenshot Service

Standalone screenshot broadcaster for [clearmap.co.il](https://clearmap.co.il).

Watches Firebase Realtime Database for active alert changes, captures map screenshots
via Playwright/Chromium, overlays the logo and alert legend, and broadcasts to a
Telegram channel.

## Why separate?

The main brain service (`clear-map-backend`) runs on Koyeb with limited memory.
Chromium requires ~200-500MB RAM to render screenshots, which causes OOM crashes
on constrained instances. This service runs on a separate machine with more memory.

## Setup

### 1. Clone and install

```bash
git clone <this-repo>
cd clearmap-screenshots
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure

Copy `config.env.example` to `config.env` and fill in:

```bash
cp config.env.example config.env
```

Required:
- `CLEARMAP_BOT_TOKEN` — Telegram bot token
- `FIREBASE_SERVICE_ACCOUNT_JSON` — Firebase service account credentials (or place `serviceAccountKey.json` in project root)

Optional:
- `TELEGRAM_CHANNEL_ID` — defaults to the main ClearMap channel
- `SCREENSHOT_COOLDOWN` — minimum seconds between broadcasts (default: 120)
- `SCREENSHOT_BATCH_DELAY` — sliding window to batch rapid alerts (default: 10)

### 3. Run

```bash
python main.py
```

### Docker

```bash
docker build -t clearmap-screenshots .
docker run -d \
  --name clearmap-screenshots \
  -v $(pwd)/config.env:/app/config.env:ro \
  -v $(pwd)/serviceAccountKey.json:/app/serviceAccountKey.json:ro \
  clearmap-screenshots
```

## How it works

1. Connects to Firebase and listens for changes to `public_state/active_alerts`
2. When new primary alerts are detected, starts a 10-second sliding window
3. After the window expires (and cooldown has passed), captures a screenshot of clearmap.co.il
4. Overlays the ClearMap logo and an active-alerts legend bar
5. Broadcasts the final image to the configured Telegram channel with a Hebrew caption
