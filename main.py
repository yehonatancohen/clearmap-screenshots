"""
ClearMap Screenshot Service — Standalone alert screenshot broadcaster.

Watches Firebase Realtime Database for active alert changes,
captures screenshots of clearmap.co.il via Playwright, and
broadcasts them to a Telegram channel.

This runs on a separate machine from the brain service (which runs on Koyeb)
so that Chromium has enough memory to operate reliably.

Usage:
    python main.py

Requires: playwright, Pillow, firebase-admin, requests
One-time setup: playwright install chromium
"""

import base64
import json
import logging
import os
import sys
import time
import threading
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import firebase_admin
import requests
from firebase_admin import credentials, db

# ── Config ──────────────────────────────────────────────────────────────────

def _load_config_env() -> dict[str, str]:
    """Read key=value pairs from config.env."""
    result = {}
    for p in (Path(__file__).parent / "config.env", Path("/app/config.env")):
        if p.exists():
            for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                result[key.strip()] = val.strip()
            break
    return result


_cfg = _load_config_env()

TELEGRAM_BOT_TOKEN = os.environ.get("CLEARMAP_BOT_TOKEN", "") or _cfg.get("CLEARMAP_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "") or _cfg.get("TELEGRAM_CHANNEL_ID", "-1003879479829")
SCREENSHOT_URL = os.environ.get("SCREENSHOT_URL", "") or _cfg.get("SCREENSHOT_URL", "https://clearmap.co.il")
SCREENSHOT_COOLDOWN = int(os.environ.get("SCREENSHOT_COOLDOWN", "") or _cfg.get("SCREENSHOT_COOLDOWN", "120"))
BATCH_DELAY = int(os.environ.get("SCREENSHOT_BATCH_DELAY", "") or _cfg.get("SCREENSHOT_BATCH_DELAY", "10"))
FIREBASE_DB_URL = "https://clear-map-f20d0-default-rtdb.europe-west1.firebasedatabase.app/"
FIREBASE_NODE = "/public_state/active_alerts"

VIEWPORT_SIZE = 900
OUTPUT_DIR = Path(__file__).parent / "screenshots"
LOGO_DIR = Path(__file__).parent / "public"

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("screenshot-service")

# ── Firebase Init ───────────────────────────────────────────────────────────

def init_firebase():
    """Initialize Firebase Admin SDK."""
    sa_json = None
    for key in ("FIREBASE_SERVICE_ACCOUNT_JSON", "FIREBASE_SERVICE_ACCOUNT", "SERVICE_ACCOUNT_JSON"):
        val = os.environ.get(key) or _cfg.get(key)
        if val:
            sa_json = val
            break

    sa_file = Path(__file__).parent / "serviceAccountKey.json"

    if sa_json:
        try:
            if sa_json.strip().startswith("{"):
                cert_dict = json.loads(sa_json)
            else:
                cert_dict = json.loads(base64.b64decode(sa_json).decode("utf-8"))
            cred = credentials.Certificate(cert_dict)
            log.info("Loaded service account from env var.")
        except Exception as e:
            raise RuntimeError(f"Failed to parse service account JSON: {e}") from e
    elif sa_file.exists():
        cred = credentials.Certificate(str(sa_file))
        log.info("Loaded service account from %s", sa_file)
    else:
        raise RuntimeError(
            "Service account key not found! "
            "Set FIREBASE_SERVICE_ACCOUNT_JSON env var or provide serviceAccountKey.json"
        )

    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})
    log.info("Firebase initialized → %s", FIREBASE_DB_URL)


# ── Screenshot Capture ──────────────────────────────────────────────────────

def capture_screenshot(url: str, output_path: Path, theme: str = "dark", size: int = 1080) -> Path:
    """Capture a screenshot of the map using Playwright, overlay logo + legend."""
    from playwright.sync_api import sync_playwright
    from screenshot_overlay import overlay_screenshot

    chromium_args = [
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--disable-extensions",
        "--mute-audio",
        "--disable-background-networking",
        "--disable-site-isolation-trials",
        "--disable-renderer-backgrounding",
        "--disable-background-timer-throttling",
        "--js-flags=--max-old-space-size=512",
    ]

    screenshot_url = f"{url}?screenshot=true" if "?" not in url else f"{url}&screenshot=true"

    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        browser = None
        try:
            log.info("📸 Attempt %d/%d — launching browser...", attempt, MAX_RETRIES)
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=chromium_args)
                context = browser.new_context(
                    viewport={"width": VIEWPORT_SIZE, "height": VIEWPORT_SIZE},
                    device_scale_factor=1.5,
                )
                page = context.new_page()

                page.goto(screenshot_url, wait_until="load", timeout=45000)
                page.wait_for_selector(".leaflet-container", timeout=15000)
                time.sleep(3)  # Let map tiles render

                # Hide UI overlays — keep only map + polygons
                page.evaluate("""
                    const style = document.createElement('style');
                    style.textContent = `
                        .glass-overlay,
                        [class*="absolute top-3"],
                        [class*="absolute bottom-4"],
                        [class*="absolute top-14"],
                        [class*="absolute top-16"],
                        [class*="absolute bottom-16"],
                        [class*="z-[1000]"],
                        [class*="z-[1001]"],
                        [class*="z-[1002]"],
                        [class*="z-[2000]"] {
                            display: none !important;
                        }
                        .leaflet-control-container {
                            display: none !important;
                        }
                    `;
                    document.head.appendChild(style);
                """)
                time.sleep(0.5)

                raw_path = output_path.parent / "raw_capture.png"
                page.screenshot(path=str(raw_path), full_page=False)
                browser.close()

            # Overlay logo + legend using Pillow (lightweight, no browser needed)
            overlay_screenshot(raw_path, output_path, size=size, theme=theme)
            raw_path.unlink(missing_ok=True)

            log.info("📸 Screenshot saved: %s (%.1f KB)", output_path, output_path.stat().st_size / 1024)
            return output_path

        except Exception as e:
            log.error("📸 Attempt %d failed: %s", attempt, e)
            if browser:
                try:
                    browser.close()
                except Exception:
                    pass
            if attempt < MAX_RETRIES:
                wait_secs = attempt * 3
                log.info("📸 Retrying in %ds...", wait_secs)
                time.sleep(wait_secs)
            else:
                raise RuntimeError(f"All {MAX_RETRIES} screenshot attempts failed") from e


# ── Caption Builder ─────────────────────────────────────────────────────────

_STATUS_EMOJI = {
    "alert": "🔴", "uav": "🟣", "terrorist": "🔶",
    "pre_alert": "🟠", "after_alert": "⚫",
}
_STATUS_LABEL = {
    "alert": "התרעות ירי רקטות וטילים", "uav": "התרעות חדירת כלי טיס עוין",
    "terrorist": "חדירת מחבלים", "pre_alert": "התרעות מוקדמות",
    "after_alert": "להישאר בממ\"ד",
}

# Import district metadata for grouping
try:
    from district_to_areas import DISTRICT_AREAS
except ImportError:
    DISTRICT_AREAS = {}


def _build_caption(alerts_data: dict) -> str:
    """Build a Hebrew caption from the Firebase active_alerts snapshot."""
    if not alerts_data:
        return "אין התרעות פעילות"

    # Build reverse mapping from city -> region
    city_to_region = {}
    for region, cities_list in DISTRICT_AREAS.items():
        for c in cities_list:
            city_to_region[c] = region

    # Group by status
    groups: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for _key, alert in alerts_data.items():
        if not isinstance(alert, dict):
            continue
        # Skip test alerts
        if alert.get("is_test") or alert.get("test") or alert.get("isTest"):
            continue
        status = alert.get("status", "alert")
        city_he = alert.get("city_name_he", "")
        ts = alert.get("timestamp", 0) / 1000  # JS ms → Python seconds
        groups[status].append((city_he, ts))

    lines = []
    for status in ("alert", "uav", "terrorist", "pre_alert", "after_alert"):
        cities = groups.get(status)
        if not cities:
            continue
        emoji = _STATUS_EMOJI.get(status, "❓")
        label = _STATUS_LABEL.get(status, status)

        if status == "after_alert":
            lines.append(f"{emoji} {len(cities)} מקומות להישאר במרחב מוגן:")
        else:
            lines.append(f"{emoji} {len(cities)} {label}:")

        if len(cities) > 8 and DISTRICT_AREAS:
            reg_dict: dict[str, float] = {}
            for city_name, ts in cities:
                reg = city_to_region.get(city_name, city_name)
                if reg not in reg_dict or ts > reg_dict[reg]:
                    reg_dict[reg] = ts
            for reg, ts in sorted(reg_dict.items(), key=lambda x: x[1], reverse=True):
                t = datetime.fromtimestamp(ts).strftime("%H:%M")
                lines.append(f"  • {reg} ({t})")
        else:
            cities.sort(key=lambda x: x[1], reverse=True)
            for city_name, ts in cities:
                t = datetime.fromtimestamp(ts).strftime("%H:%M")
                lines.append(f"  • {city_name} ({t})")

    lines.append("")
    lines.append("🗺 clearmap.co.il")

    caption = "\n".join(lines)

    # Telegram caption limit
    if len(caption) > 1024:
        summary_parts = []
        for status in ("alert", "uav", "terrorist", "pre_alert", "after_alert"):
            cities = groups.get(status)
            if not cities:
                continue
            emoji = _STATUS_EMOJI.get(status, "❓")
            label = _STATUS_LABEL.get(status, status)
            if status == "after_alert":
                summary_parts.append(f"{emoji} {len(cities)} מקומות להישאר במרחב מוגן")
            else:
                summary_parts.append(f"{emoji} {len(cities)} {label}")
        caption = " | ".join(summary_parts) + "\n\n🗺 clearmap.co.il"

    return caption


# ── Telegram Sender ─────────────────────────────────────────────────────────

def send_photo_to_channel(photo_path: Path, caption: str) -> bool:
    """Send a photo to the Telegram channel."""
    try:
        with open(photo_path, "rb") as f:
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": TELEGRAM_CHANNEL_ID, "caption": caption},
                files={"photo": ("alert.png", f, "image/png")},
                timeout=30,
            )
        if resp.ok:
            log.info("📸 Broadcast sent to channel %s", TELEGRAM_CHANNEL_ID)
            return True
        else:
            log.error("📸 sendPhoto failed: %s", resp.text[:200])
            return False
    except Exception as e:
        log.error("📸 sendPhoto error: %s", e)
        return False


# ── Alert Change Detection (Firebase Listener) ─────────────────────────────

def _is_primary_alert(status: str) -> bool:
    """Returns True if the status is a primary (non-passive) alert type."""
    return status in ("alert", "uav", "terrorist", "pre_alert")


def main():
    log.info("=== ClearMap Screenshot Service starting ===")

    if not TELEGRAM_BOT_TOKEN:
        log.error("CLEARMAP_BOT_TOKEN not set! Exiting.")
        sys.exit(1)

    init_firebase()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # State tracking
    last_screenshot_time = 0.0
    sliding_window_end = 0.0
    pending_screenshot = False
    previous_primary_ids: set[str] = set()
    latest_snapshot: dict = {}
    snapshot_lock = threading.Lock()

    def on_alerts_change(event):
        """Firebase listener callback — fires on every change to active_alerts."""
        nonlocal pending_screenshot, sliding_window_end, previous_primary_ids, latest_snapshot

        data = event.data
        if event.path != "/" or data is None:
            # Sub-key update or deletion — fetch full snapshot
            ref = db.reference(FIREBASE_NODE)
            data = ref.get() or {}

        with snapshot_lock:
            latest_snapshot = data if isinstance(data, dict) else {}

        # Check if there are new primary alerts
        current_primary_ids = set()
        if isinstance(data, dict):
            for key, alert in data.items():
                if isinstance(alert, dict) and _is_primary_alert(alert.get("status", "")):
                    # Skip test alerts
                    if alert.get("is_test") or alert.get("test") or alert.get("isTest"):
                        continue
                    current_primary_ids.add(key)

        new_primaries = current_primary_ids - previous_primary_ids
        previous_primary_ids = current_primary_ids

        if new_primaries:
            now = time.time()
            sliding_window_end = now + BATCH_DELAY
            pending_screenshot = True
            log.info("🔔 %d new primary alert(s) detected! Sliding window set to %ds.",
                     len(new_primaries), BATCH_DELAY)

    # Start Firebase listener
    log.info("👂 Listening for alert changes on Firebase: %s", FIREBASE_NODE)
    ref = db.reference(FIREBASE_NODE)
    ref.listen(on_alerts_change)

    log.info("📸 Broadcast config: channel=%s cooldown=%ds batch_delay=%ds",
             TELEGRAM_CHANNEL_ID, SCREENSHOT_COOLDOWN, BATCH_DELAY)

    # Main loop — handles batching and cooldown
    while True:
        try:
            if pending_screenshot:
                now = time.time()

                if now >= sliding_window_end:
                    time_since_last = now - last_screenshot_time
                    if time_since_last >= SCREENSHOT_COOLDOWN:
                        with snapshot_lock:
                            current_data = dict(latest_snapshot)

                        if current_data:
                            # Filter out test alerts to see if there's anything real to show
                            real_alerts = {
                                k: v for k, v in current_data.items()
                                if isinstance(v, dict) and not (v.get("is_test") or v.get("test") or v.get("isTest"))
                            }

                            if not real_alerts:
                                log.info("📸 No non-test alerts active — skipping screenshot.")
                                pending_screenshot = False
                                continue

                            log.info("📸 Cooldown & batch window elapsed — capturing screenshot for %d alerts",
                                     len(real_alerts))
                            try:
                                final_path = OUTPUT_DIR / "broadcast_latest.png"
                                capture_screenshot(SCREENSHOT_URL, final_path)
                                caption = _build_caption(current_data)
                                log.info("📸 Caption: %s", caption)
                                send_photo_to_channel(final_path, caption)
                                last_screenshot_time = time.time()
                            except Exception as e:
                                log.error("📸 Screenshot/broadcast failed: %s", e)
                        else:
                            log.info("📸 Alerts cleared before screenshot — cancelling.")

                        pending_screenshot = False
                    else:
                        remaining = SCREENSHOT_COOLDOWN - time_since_last
                        if int(remaining) % 30 == 0:
                            log.info("📸 Waiting on cooldown: %.0fs remaining", remaining)
                else:
                    remaining = sliding_window_end - now
                    if remaining > 8 or int(remaining) % 3 == 0:
                        log.info("📸 Batching alerts... %.1fs left in sliding window", remaining)

        except Exception as e:
            log.error("Unexpected error: %s", e, exc_info=True)

        time.sleep(1.5)


if __name__ == "__main__":
    main()
