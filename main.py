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
import itertools
import json
import logging
import math
import os
import sys
import time
import shutil
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import firebase_admin
import requests
import telebot
from telebot import types
from PIL import Image
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
# When set, routes ALL messages (including is_test alerts) only to this channel.
# Use for end-to-end testing without hitting real subscribers.
TEST_CHANNEL_ID = os.environ.get("TEST_CHANNEL_ID", "-5197151796") or _cfg.get("TEST_CHANNEL_ID", "-5197151796")
SCREENSHOT_URL = os.environ.get("SCREENSHOT_URL", "") or _cfg.get("SCREENSHOT_URL", "https://www.clearmap.co.il/broadcast?uav=true&ellipse=true&theme=dark")
SCREENSHOT_COOLDOWN = int(os.environ.get("SCREENSHOT_COOLDOWN", "") or _cfg.get("SCREENSHOT_COOLDOWN", "120"))
BATCH_DELAY = int(os.environ.get("SCREENSHOT_BATCH_DELAY", "") or _cfg.get("SCREENSHOT_BATCH_DELAY", "10"))
# If new alerts arrive within this many seconds of the last broadcast,
# edit the existing Telegram message instead of sending a new one.
EDIT_WINDOW = int(os.environ.get("SCREENSHOT_EDIT_WINDOW", "") or _cfg.get("SCREENSHOT_EDIT_WINDOW", "300"))
GEO_CLUSTER_KM = float(os.environ.get("GEO_CLUSTER_KM", "") or _cfg.get("GEO_CLUSTER_KM", "50"))
POLYGONS_FILE_DEFAULT = Path(__file__).parent.parent / "clear-map-backend" / "polygons.json"
FIREBASE_DB_URL = "https://clear-map-f20d0-default-rtdb.europe-west1.firebasedatabase.app/"
FIREBASE_NODE = "/public_state/active_alerts"
FIREBASE_SUBSCRIBERS = "/public_state/subscribers"

VIEWPORT_SIZE = 900
OUTPUT_DIR = Path(__file__).parent / "screenshots"
LOGO_DIR = Path(__file__).parent / "public"
CUSTOM_LOGOS_DIR = LOGO_DIR / "custom_logos"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CUSTOM_LOGOS_DIR.mkdir(parents=True, exist_ok=True)

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

def capture_screenshot(url: str, output_path: Path, theme: str = "dark",
                       size: int = 1080, custom_logo_path: Path | None = None,
                       init_script: str | None = None) -> tuple[Path, Path]:
    """
    Capture a screenshot of the map using Playwright, overlay logo + legend.
    Returns (final_output_path, raw_capture_path).
    """
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
                if init_script:
                    context.add_init_script(init_script)
                page = context.new_page()

                page.goto(screenshot_url, wait_until="load", timeout=45000)
                page.wait_for_selector(".leaflet-container", timeout=15000)

                # Wait for Leaflet tile images to finish loading.
                # Tiles are fetched asynchronously after page load, so a fixed
                # sleep is unreliable. Poll until all visible tile <img> elements
                # report complete=true, with a 12s timeout before giving up.
                try:
                    page.wait_for_function(
                        """() => {
                            const tiles = document.querySelectorAll('img.leaflet-tile');
                            return tiles.length > 0 &&
                                   Array.from(tiles).every(t => t.complete && t.naturalWidth > 0);
                        }""",
                        timeout=12000,
                    )
                except Exception:
                    log.warning("📸 Tile load wait timed out — taking screenshot anyway")
                time.sleep(0.5)  # Brief pause for final paint

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

                raw_path = output_path.parent / f"raw_capture_{int(time.time())}.png"
                page.screenshot(path=str(raw_path), full_page=False)
                browser.close()

            # Overlay logo + legend using Pillow (lightweight, no browser needed)
            overlay_screenshot(raw_path, output_path, size=size, theme=theme, 
                               custom_logo_path=custom_logo_path)
            
            log.info("📸 Screenshot saved: %s (%.1f KB)", output_path, output_path.stat().st_size / 1024)
            return output_path, raw_path

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

# ── Geographic Group State ───────────────────────────────────────────────────

_group_id_counter = itertools.count(1)


@dataclass
class GroupState:
    group_id: int
    city_names: set[str]  # current set of city_name_he in this group
    # status -> frozenset of cities at time of last broadcast (for diff detection)
    last_broadcast_snapshot: dict[str, frozenset[str]]
    message_ids: dict[str, int]  # chat_id -> telegram message_id for this group
    last_broadcast_time: float


def _build_caption(alerts_data: dict, city_filter: set[str] | None = None) -> str:
    """
    Build a Hebrew caption from the Firebase active_alerts snapshot.
    If city_filter is provided, only include alerts for those cities.
    """
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
        # Skip test alerts unless TEST_CHANNEL_ID is configured
        if not TEST_CHANNEL_ID and (alert.get("is_test") or alert.get("test") or alert.get("isTest")):
            continue
        city_he = alert.get("city_name_he", "")
        if city_filter is not None and city_he not in city_filter:
            continue
        status = alert.get("status", "alert")
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
    lines.append("לצפייה במפה החיה ועדכונים בזמן אמת:")
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
        caption = " | ".join(summary_parts) + "\n\nלצפייה במפה החיה ועדכונים בזמן אמת:\n🗺 clearmap.co.il"

    return caption


# ── Telegram Subscriber Management ──────────────────────────────────────────

def get_subscribers_data() -> dict:
    """Fetch all subscriber data from Firebase."""
    try:
        ref = db.reference(FIREBASE_SUBSCRIBERS)
        return ref.get() or {}
    except Exception as e:
        log.error("Failed to fetch subscribers: %s", e)
        return {}

def get_subscribers() -> list[str]:
    """Fetch the list of subscriber chat IDs from Firebase."""
    data = get_subscribers_data()
    return [str(chat_id) for chat_id in data.keys()]


def add_subscriber(chat_id: int, chat_title: str = ""):
    """Save a new chat_id to the Firebase subscribers list."""
    try:
        ref = db.reference(f"{FIREBASE_SUBSCRIBERS}/{chat_id}")
        ref.set({
            "added_at": int(time.time()),
            "title": chat_title
        })
        log.info("👤 New subscriber added: %s (%s)", chat_id, chat_title)
    except Exception as e:
        log.error("Failed to add subscriber %s: %s", chat_id, e)


def remove_subscriber(chat_id: str):
    """Remove a chat_id from the Firebase subscribers list."""
    try:
        ref = db.reference(f"{FIREBASE_SUBSCRIBERS}/{chat_id}")
        ref.delete()
        log.info("👤 Subscriber removed: %s", chat_id)
    except Exception as e:
        log.error("Failed to remove subscriber %s: %s", chat_id, e)


def telegram_listener():
    """Background thread to manage bot interactions in private chats."""
    if not TELEGRAM_BOT_TOKEN:
        return

    bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
    
    # user_id -> {"action": "waiting_for_logo", "channel_id": "..."}
    user_states = {}

    def is_user_admin(chat_id: str, user_id: int) -> bool:
        """Check if user_id is an admin of chat_id."""
        try:
            member = bot.get_chat_member(chat_id, user_id)
            return member.status in ("administrator", "creator")
        except Exception:
            return False

    @bot.message_handler(commands=['start', 'help', 'עזרה'], chat_types=['private'])
    def handle_start(message):
        text = (
            "👋 *שלום! אני הבוט של ClearMap המיועד להפצת התרעות לערוצי טלגרם.* 🗺️\n\n"
            "כדי להתחיל לעבוד איתי, בצע את השלבים הבאים:\n"
            "1️⃣ **הוסף אותי לערוץ שלך** - הוסף את הבוט כמשתמש רגיל ולאחר מכן הגדר אותו כ**מנהל** (Administrator).\n"
            "2️⃣ **נהל את ההגדרות כאן** - חזור לצ'אט הפרטי הזה ושלח את הפקודה /manage.\n\n"
            "*מה אפשר לעשות כאן?*\n"
            "✅ **ניהול ערוצים** - תמיכה בניהול של מספר ערוצים במקביל.\n"
            "🖼️ **לוגו מותאם אישית** - העלאת לוגו שיופיע בפינה השמאלית העליונה של כל צילום מסך בערוץ שלך.\n"
            "👁️ **תצוגה מקדימה** - תוכל לראות בדיוק איך הלוגו משתלב על המפה לפני האישור הסופי.\n"
            "🗑️ **הסרת לוגו/ערוץ** - שליטה מלאה בהגדרות או הפסקת השירות לערוץ ספציפי.\n\n"
            "*פקודות זמינות:*\n"
            "/manage - בחירת ערוץ וניהול הלוגו\n"
            "/help - הצגת הסבר זה שוב\n"
            "/cancel - ביטול פעולה נוכחית (למשל בזמן העלאת לוגו)\n\n"
            "לחץ על /manage כדי להתחיל!"
        )
        bot.send_message(message.chat.id, text, parse_mode="Markdown")

    @bot.message_handler(commands=['manage'], chat_types=['private'])
    def handle_manage(message):
        user_id = message.from_user.id
        subs = get_subscribers_data()
        
        managed_channels = []
        for chat_id, data in subs.items():
            if is_user_admin(chat_id, user_id):
                managed_channels.append((chat_id, data.get("title", chat_id)))
        
        if not managed_channels:
            text = (
                "❌ *לא נמצאו ערוצים בניהולך.*\n\n"
                "וודא ש:\n"
                "1. הוספת את הבוט לערוץ.\n"
                "2. הגדרת את הבוט כ**מנהל** (Administrator) בערוץ.\n"
                "3. אתה בעצמך מנהל בערוץ זה."
            )
            bot.send_message(message.chat.id, text, parse_mode="Markdown")
            return

        markup = types.InlineKeyboardMarkup()
        for cid, title in managed_channels:
            markup.add(types.InlineKeyboardButton(f"📺 {title}", callback_data=f"manage_{cid}"))
        
        bot.send_message(message.chat.id, "👇 *בחר את הערוץ שברצונך לנהל:*", reply_markup=markup, parse_mode="Markdown")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("manage_"))
    def callback_manage_channel(call):
        channel_id = call.data.split("_")[1]
        user_id = call.from_user.id
        
        if not is_user_admin(channel_id, user_id):
            bot.answer_callback_query(call.id, "⚠️ אינך מנהל בערוץ זה יותר.", show_alert=True)
            return

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🖼️ הגדרת לוגו חדש", callback_data=f"setlogo_{channel_id}"))
        markup.add(types.InlineKeyboardButton("🗑️ הסרת לוגו קיים", callback_data=f"removelogo_{channel_id}"))
        markup.add(types.InlineKeyboardButton("🚫 הפסקת התרעות לערוץ זה", callback_data=f"unregister_{channel_id}"))
        markup.add(types.InlineKeyboardButton("🔙 חזרה לרשימה", callback_data="back_to_list"))
        
        bot.edit_message_text(f"⚙️ *ניהול הגדרות עבור הערוץ:*\n`{channel_id}`", 
                             call.message.chat.id, call.message.message_id, 
                             reply_markup=markup, parse_mode="Markdown")

    @bot.callback_query_handler(func=lambda call: call.data == "back_to_list")
    def callback_back_to_list(call):
        handle_manage(call.message)
        bot.delete_message(call.message.chat.id, call.message.message_id)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("setlogo_"))
    def callback_set_logo(call):
        channel_id = call.data.split("_")[1]
        user_states[call.from_user.id] = {"action": "waiting_for_logo", "channel_id": channel_id}
        
        text = (
            "🖼️ *העלאת לוגו לערוץ*\n\n"
            "אנא שלח כעת את קובץ התמונה שברצונך להציג (PNG או JPG).\n"
            "💡 *טיפ:* כדי לשמור על רקע שקוף, מומלץ לשלוח את הלוגו כ**קובץ** (File/Document) ולא כתמונה רגילה.\n\n"
            "💡 _לביטול שלח /cancel_"
        )
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="Markdown")

    @bot.message_handler(content_types=['photo', 'document'], chat_types=['private'])
    def handle_logo_upload(message):
        user_id = message.from_user.id
        state = user_states.get(user_id)
        
        if not state or state.get("action") != "waiting_for_logo":
            return

        channel_id = state["channel_id"]
        
        # Determine file_id based on content type
        if message.content_type == 'photo':
            file_id = message.photo[-1].file_id
        else:
            if not message.document.mime_type or not message.document.mime_type.startswith('image/'):
                bot.reply_to(message, "❌ הקובץ ששלחת אינו תמונה נתמכת. אנא שלח קובץ תמונה (PNG/JPG).")
                return
            file_id = message.document.file_id

        # Download file
        file_info = bot.get_file(file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        temp_logo_path = OUTPUT_DIR / f"temp_logo_{user_id}.png"
        with open(temp_logo_path, "wb") as f:
            f.write(downloaded_file)
        
        # Generate Preview
        bot.send_message(message.chat.id, "מייצר תצוגה מקדימה, אנא המתן...")
        
        try:
            from screenshot_overlay import overlay_screenshot
            # Use a dummy background or current raw if exists
            raw_files = list(OUTPUT_DIR.glob("raw_capture_*.png"))
            
            if not raw_files:
                # No raw capture found, use a dark placeholder image
                preview_path = OUTPUT_DIR / f"preview_{user_id}.png"
                placeholder = Image.new("RGBA", (VIEWPORT_SIZE, VIEWPORT_SIZE), (30, 30, 40, 255))
                placeholder_raw = OUTPUT_DIR / f"placeholder_raw_{user_id}.png"
                placeholder.save(placeholder_raw)
                overlay_screenshot(placeholder_raw, preview_path, custom_logo_path=temp_logo_path)
                placeholder_raw.unlink(missing_ok=True)
                
                with open(preview_path, "rb") as f:
                    bot.send_photo(message.chat.id, f, caption="שימו לב: לא נמצא צילום מפה עדכני, זהו רקע לדוגמה. האם לאשר את הלוגו?")
            else:
                preview_path = OUTPUT_DIR / f"preview_{user_id}.png"
                overlay_screenshot(raw_files[-1], preview_path, custom_logo_path=temp_logo_path)
                with open(preview_path, "rb") as f:
                    bot.send_photo(message.chat.id, f, caption="כך יראה הצילום עם הלוגו שלך. לאשר?")
            
            user_states[user_id]["action"] = "confirm_logo"
            user_states[user_id]["temp_path"] = str(temp_logo_path)
            
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("✅ אשר", callback_data="confirmlogo_yes"))
            markup.add(types.InlineKeyboardButton("❌ בטל", callback_data="confirmlogo_no"))
            bot.send_message(message.chat.id, "האם לאשר את הלוגו?", reply_markup=markup)
            
        except Exception as e:
            log.error("Preview generation failed: %s", e)
            bot.send_message(message.chat.id, "נכשלה יצירת תצוגה מקדימה. וודא שהקובץ תקין.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirmlogo_"))
    def callback_confirm_logo(call):
        user_id = call.from_user.id
        state = user_states.get(user_id)
        if not state or state.get("action") != "confirm_logo":
            return
        
        if call.data == "confirmlogo_yes":
            channel_id = state["channel_id"]
            temp_path = Path(state["temp_path"])
            final_path = CUSTOM_LOGOS_DIR / f"{channel_id}.png"
            shutil.move(str(temp_path), str(final_path))
            bot.edit_message_text("✅ הלוגו עודכן בהצלחה!", call.message.chat.id, call.message.message_id)
        else:
            if "temp_path" in state:
                Path(state["temp_path"]).unlink(missing_ok=True)
            bot.edit_message_text("❌ הפעולה בוטלה.", call.message.chat.id, call.message.message_id)
        
        user_states.pop(user_id, None)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("removelogo_"))
    def callback_remove_logo(call):
        channel_id = call.data.split("_")[1]
        for ext in (".png", ".jpg", ".jpeg"):
            p = CUSTOM_LOGOS_DIR / f"{channel_id}{ext}"
            p.unlink(missing_ok=True)
        bot.edit_message_text("✅ הלוגו הוסר.", call.message.chat.id, call.message.message_id)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("unregister_"))
    def callback_unregister(call):
        channel_id = call.data.split("_")[1]
        remove_subscriber(channel_id)
        bot.edit_message_text(f"✅ ערוץ {channel_id} הוסר מהמערכת.", call.message.chat.id, call.message.message_id)

    @bot.message_handler(commands=['cancel'], chat_types=['private'])
    def handle_cancel(message):
        user_id = message.from_user.id
        if user_id in user_states:
            state = user_states.pop(user_id)
            if "temp_path" in state:
                Path(state["temp_path"]).unlink(missing_ok=True)
            bot.reply_to(message, "הפעולה בוטלה.")
        else:
            bot.reply_to(message, "אין פעולה פעילה לביטול.")

    # Auto-register when added to a channel
    @bot.my_chat_member_handler()
    def handle_my_chat_member_update(update):
        new = update.new_chat_member
        chat_id = update.chat.id
        chat_title = update.chat.title or "Channel"
        log.info("🔄 Bot status update in %s (%s): %s", update.chat.type, chat_id, new.status)

        if new.status in ("administrator", "member"):
            if update.chat.type in ("channel", "group", "supergroup"):
                add_subscriber(chat_id, chat_title)
                
                # Send a welcome message to the channel
                welcome_text = (
                    "👋 *הבוט של ClearMap חובר בהצלחה!*\n\n"
                    "כדי להגדיר לוגו מותאם אישית ולנהל את ההגדרות, "
                    "אנא פנו אל הבוט ב**צ'אט פרטי** ושלחו את הפקודה /manage."
                )
                try:
                    bot.send_message(chat_id, welcome_text, parse_mode="Markdown")
                except Exception as e:
                    log.error("Could not send welcome message to %s: %s", chat_id, e)
        elif new.status in ("left", "kicked"):
            remove_subscriber(str(chat_id))

    # Fallback manual registration for channels/groups
    @bot.message_handler(commands=['register', 'reg'], chat_types=['channel', 'group', 'supergroup'])
    def handle_manual_register(message):
        chat_id = message.chat.id
        chat_title = message.chat.title or "Channel"
        add_subscriber(chat_id, chat_title)
        bot.reply_to(message, "✅ הערוץ נרשם במערכת בהצלחה! ניתן לנהל אותו כעת מהצ'אט הפרטי עם הבוט.")

    log.info("👂 Telegram private-chat manager started (polling)...")
    while True:
        try:
            # Explicitly allow my_chat_member updates
            bot.polling(none_stop=True, interval=3, timeout=20, 
                        allowed_updates=["message", "callback_query", "my_chat_member", "chat_member"])
        except Exception as e:
            log.error("Telegram polling error: %s", e)
            time.sleep(10)



# ── Telegram Sender ─────────────────────────────────────────────────────────

def broadcast_to_subscribers(
    caption: str,
    theme: str = "dark",
    prev_message_ids: dict[str, int] | None = None,
) -> dict[str, int]:
    """
    Capture a raw screenshot once, then overlay and broadcast
    custom-branded screenshots to each registered subscriber.

    If prev_message_ids is provided, edits those messages in-place
    (editMessageMedia) instead of sending new ones.

    Returns a dict of {chat_id: message_id} for the sent/edited messages
    so the caller can pass them back for future edits.
    """
    from screenshot_overlay import overlay_screenshot

    sent_ids: dict[str, int] = {}
    if prev_message_ids is None:
        prev_message_ids = {}

    subscribers = get_subscribers()
    if TELEGRAM_CHANNEL_ID and TELEGRAM_CHANNEL_ID not in subscribers:
        subscribers.append(TELEGRAM_CHANNEL_ID)

    if not subscribers:
        log.warning("📸 No subscribers found for broadcast!")
        return sent_ids

    # 1. Capture Raw Screenshot (no overlays yet)
    try:
        dummy_path = OUTPUT_DIR / "dummy.png"
        _, raw_file = capture_screenshot(SCREENSHOT_URL, dummy_path, theme=theme)
    except Exception as e:
        log.error("📸 Failed to capture base screenshot: %s", e)
        return sent_ids

    # Group subscribers by custom logo
    logo_groups = defaultdict(list)
    for chat_id in subscribers:
        found_logo = None
        for ext in (".png", ".jpg", ".jpeg"):
            p = CUSTOM_LOGOS_DIR / f"{chat_id}{ext}"
            if p.exists():
                found_logo = p
                break
        logo_groups[found_logo].append(chat_id)

    is_edit = len(prev_message_ids) > 0
    log.info("📸 %s to %d subscribers in %d logo groups...",
             "Editing" if is_edit else "Broadcasting",
             len(subscribers), len(logo_groups))

    for logo_path, chat_ids in logo_groups.items():
        group_name = logo_path.name if logo_path else "default"
        final_path = OUTPUT_DIR / f"broadcast_{group_name}_{int(time.time())}.png"

        try:
            overlay_screenshot(raw_file, final_path, theme=theme, custom_logo_path=logo_path)

            for chat_id in chat_ids:
                prev_msg_id = prev_message_ids.get(chat_id)
                try:
                    if prev_msg_id:
                        # Edit the existing message with updated screenshot + caption
                        with open(final_path, "rb") as f:
                            media_payload = json.dumps({
                                "type": "photo",
                                "media": "attach://photo",
                                "caption": caption,
                            })
                            resp = requests.post(
                                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageMedia",
                                data={
                                    "chat_id": chat_id,
                                    "message_id": prev_msg_id,
                                    "media": media_payload,
                                },
                                files={"photo": (f"alert_{group_name}.png", f, "image/png")},
                                timeout=30,
                            )
                        if resp.ok:
                            sent_ids[chat_id] = prev_msg_id
                            log.info("   ✏️  Edited msg %s in %s (%s)", prev_msg_id, chat_id, group_name)
                        else:
                            # Edit failed (message deleted, too old, etc.) — fall back to new send
                            log.warning("   ⚠️  Edit failed for %s (msg %s): %s — sending new",
                                        chat_id, prev_msg_id, resp.text[:80])
                            prev_msg_id = None  # trigger fallback below

                    if not prev_msg_id:
                        # Send a new message
                        with open(final_path, "rb") as f:
                            resp = requests.post(
                                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                                data={"chat_id": chat_id, "caption": caption},
                                files={"photo": (f"alert_{group_name}.png", f, "image/png")},
                                timeout=30,
                            )
                        if resp.ok:
                            msg_id = resp.json().get("result", {}).get("message_id")
                            if msg_id:
                                sent_ids[chat_id] = msg_id
                            log.info("   ✅ Sent to %s (%s)", chat_id, group_name)
                        elif resp.status_code == 403:
                            log.warning("   ❌ Forbidden from %s — removing", chat_id)
                            remove_subscriber(chat_id)
                        else:
                            log.error("   ❌ Failed for %s: %s", chat_id, resp.text[:100])

                except Exception as e:
                    log.error("   ❌ Error sending to %s: %s", chat_id, e)

            final_path.unlink(missing_ok=True)
        except Exception as e:
            log.error("📸 Failed to overlay/send for group %s: %s", group_name, e)

    raw_file.unlink(missing_ok=True)
    dummy_path.unlink(missing_ok=True)

    return sent_ids




# ── Alert Change Detection (Firebase Listener) ─────────────────────────────

def _is_primary_alert(status: str) -> bool:
    """Returns True if the status is a primary (non-passive) alert type."""
    return status in ("alert", "uav", "terrorist", "pre_alert")


# ── Geographic Clustering ────────────────────────────────────────────────────

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _compute_centroid(polygon_coords: list) -> tuple[float, float]:
    """Average of all boundary points → (lat, lng)."""
    n = len(polygon_coords)
    if n == 0:
        return (0.0, 0.0)
    lat_sum = sum(p[0] for p in polygon_coords)
    lng_sum = sum(p[1] for p in polygon_coords)
    return (lat_sum / n, lng_sum / n)


def _load_centroids() -> dict[str, tuple[float, float]]:
    """
    Load city centroids from polygons.json.
    Returns dict[city_name_he -> (lat, lon)].
    Falls back to empty dict if file not found (clustering disabled).
    """
    polygons_path = Path(os.environ.get("POLYGONS_FILE", "") or str(POLYGONS_FILE_DEFAULT))
    if not polygons_path.exists():
        log.warning("polygons.json not found at %s — geo-clustering disabled (all alerts in one group)", polygons_path)
        return {}
    try:
        with open(polygons_path, encoding="utf-8") as f:
            data = json.load(f)
        centroids = {
            name: _compute_centroid(entry["polygon"])
            for name, entry in data.items()
            if entry.get("polygon")
        }
        log.info("🗺  Loaded centroids for %d cities from %s", len(centroids), polygons_path)
        return centroids
    except Exception as e:
        log.warning("Failed to load polygons.json: %s — geo-clustering disabled", e)
        return {}


def _cluster_cities(
    city_names: list[str],
    centroids: dict[str, tuple[float, float]],
    threshold_km: float = GEO_CLUSTER_KM,
) -> list[set[str]]:
    """
    Partition city_names into geographic clusters using BFS on a proximity graph.
    Two cities are adjacent if haversine distance <= threshold_km.
    Cities missing from centroids are each placed in their own singleton cluster.
    If centroids dict is empty, returns a single cluster containing all cities.
    """
    if not city_names:
        return []

    # Fallback: no centroid data → single group
    if not centroids:
        return [set(city_names)]

    known = [c for c in city_names if c in centroids]
    unknown = [c for c in city_names if c not in centroids]

    # BFS connected-component clustering on known cities
    visited: set[str] = set()
    clusters: list[set[str]] = []

    for start in known:
        if start in visited:
            continue
        cluster: set[str] = set()
        queue = [start]
        while queue:
            city = queue.pop()
            if city in visited:
                continue
            visited.add(city)
            cluster.add(city)
            lat1, lon1 = centroids[city]
            for other in known:
                if other not in visited:
                    lat2, lon2 = centroids[other]
                    if _haversine_km(lat1, lon1, lat2, lon2) <= threshold_km:
                        queue.append(other)
        clusters.append(cluster)

    # Each unknown city gets its own singleton cluster
    for city in unknown:
        clusters.append({city})

    return clusters


# ── Group Matching & Diff ────────────────────────────────────────────────────

def _match_clusters_to_states(
    new_clusters: list[set[str]],
    existing_states: dict[int, GroupState],
) -> tuple[dict[int, tuple[set[str], GroupState]], list[set[str]], list[GroupState]]:
    """
    Greedy max-overlap matching of new clusters to existing GroupState objects.
    Each cluster/state is assigned to at most one match.

    Returns:
      matched:          {cluster_idx -> (cluster_set, state)}
      unmatched_new:    clusters with no matching existing state (brand new groups)
      orphaned_states:  states with no matching new cluster (alerts cleared)
    """
    # Build all (overlap, cluster_idx, state_id) triples
    triples: list[tuple[int, int, int]] = []
    for ci, cluster in enumerate(new_clusters):
        for sid, state in existing_states.items():
            overlap = len(cluster & state.city_names)
            if overlap > 0:
                triples.append((overlap, ci, sid))

    triples.sort(key=lambda x: x[0], reverse=True)

    assigned_clusters: set[int] = set()
    assigned_states: set[int] = set()
    matched: dict[int, tuple[set[str], GroupState]] = {}

    for _overlap, ci, sid in triples:
        if ci not in assigned_clusters and sid not in assigned_states:
            matched[ci] = (new_clusters[ci], existing_states[sid])
            assigned_clusters.add(ci)
            assigned_states.add(sid)

    unmatched_new = [new_clusters[ci] for ci in range(len(new_clusters)) if ci not in assigned_clusters]
    orphaned_states = [existing_states[sid] for sid in existing_states if sid not in assigned_states]

    return matched, unmatched_new, orphaned_states


_STATUS_PRIORITY: dict[str, int] = {"pre_alert": 1, "alert": 2, "uav": 2, "terrorist": 2}


def _compute_group_diff(
    city_by_status: dict[str, str],
    state: GroupState,
) -> tuple[set[str], dict[str, set[str]]]:
    """
    Compare current city_by_status against the group's last_broadcast_snapshot.

    Returns:
      new_same_cities:  cities added at same or new status (not upgraded from pre_alert)
      upgraded:         dict[new_status -> set[city]] for cities that moved from pre_alert
                        to a higher-priority status (alert/uav/terrorist)
    """
    all_snapshot_cities: set[str] = set()
    for cities_set in state.last_broadcast_snapshot.values():
        all_snapshot_cities.update(cities_set)

    new_same: set[str] = set()
    upgraded: dict[str, set[str]] = defaultdict(set)

    for city, new_status in city_by_status.items():
        if city not in all_snapshot_cities:
            # Brand-new city in this group
            new_same.add(city)
        else:
            # City was in snapshot — check if status increased
            old_status = None
            for snap_status, snap_cities in state.last_broadcast_snapshot.items():
                if city in snap_cities:
                    old_status = snap_status
                    break
            if old_status and old_status != new_status:
                old_prio = _STATUS_PRIORITY.get(old_status, 0)
                new_prio = _STATUS_PRIORITY.get(new_status, 0)
                if new_prio > old_prio:
                    upgraded[new_status].add(city)

    return new_same, dict(upgraded)


# ── Per-Group Broadcast Helpers ──────────────────────────────────────────────

def _compute_cluster_view(
    city_names: set[str],
    centroids: dict[str, tuple[float, float]],
) -> tuple[float | None, float | None, int | None]:
    """
    Compute the best map center (lat, lon) and zoom level for a geographic cluster.
    Returns (None, None, None) when no centroids are known → caller uses default full-Israel view.
    """
    known = [centroids[c] for c in city_names if c in centroids]
    if not known:
        return None, None, None

    lats = [p[0] for p in known]
    lons = [p[1] for p in known]
    center_lat = (min(lats) + max(lats)) / 2.0
    center_lon = (min(lons) + max(lons)) / 2.0

    lat_span_km = (max(lats) - min(lats)) * 111.0
    lon_span_km = (max(lons) - min(lons)) * 111.0 * math.cos(math.radians(center_lat))
    max_span_km = max(lat_span_km, lon_span_km, 1.0)

    # 50% padding so context around the cluster is visible
    padded_km = max_span_km * 1.5
    # At zoom 8 roughly 400 km is visible in a 900 px viewport; each zoom step halves coverage
    zoom = max(7, min(13, round(8.5 + math.log2(400.0 / padded_km))))

    return center_lat, center_lon, zoom


def _capture_raw_screenshot(
    theme: str = "dark",
    lat: float | None = None,
    lon: float | None = None,
    zoom: int | None = None,
) -> Path | None:
    """
    Capture one raw (no-overlay) screenshot of the map.
    When lat/lon/zoom are provided the map is centred on that location.
    In test mode (TEST_CHANNEL_ID set), injects window.__showTestAlerts=true
    into the browser context so the frontend renders is_test alerts — without
    making test alerts visible to real website visitors.
    Returns the raw PNG path, or None on failure.
    """
    try:
        url = SCREENSHOT_URL
        if lat is not None and lon is not None and zoom is not None:
            url += f"&lat={lat:.5f}&lon={lon:.5f}&zoom={zoom}"
        init_script = "window.__showTestAlerts = true;" if TEST_CHANNEL_ID else None
        dummy_path = OUTPUT_DIR / f"dummy_{int(time.time())}.png"
        _, raw_file = capture_screenshot(url, dummy_path, theme=theme, init_script=init_script)
        dummy_path.unlink(missing_ok=True)
        return raw_file
    except Exception as e:
        log.error("Screenshot capture failed: %s", e)
        return None


def _send_group_to_subscribers(
    caption: str,
    raw_path: Path,
    theme: str = "dark",
    prev_message_ids: dict[str, int] | None = None,
) -> dict[str, int]:
    """
    Overlay and send (or edit) a screenshot to all subscribers for one geographic group.
    Reuses a pre-captured raw_path instead of launching a new browser.
    Returns {chat_id: message_id} for all successful sends/edits.
    """
    from screenshot_overlay import overlay_screenshot

    sent_ids: dict[str, int] = {}
    if prev_message_ids is None:
        prev_message_ids = {}

    if TEST_CHANNEL_ID:
        subscribers = [TEST_CHANNEL_ID]
        log.info("📸 TEST_CHANNEL_ID set — routing to test channel only: %s", TEST_CHANNEL_ID)
    else:
        subscribers = get_subscribers()
        if TELEGRAM_CHANNEL_ID and TELEGRAM_CHANNEL_ID not in subscribers:
            subscribers.append(TELEGRAM_CHANNEL_ID)

    if not subscribers:
        log.warning("📸 No subscribers for group broadcast!")
        return sent_ids

    # Group subscribers by custom logo path
    logo_groups: dict[Path | None, list[str]] = defaultdict(list)
    for chat_id in subscribers:
        found_logo = None
        for ext in (".png", ".jpg", ".jpeg"):
            p = CUSTOM_LOGOS_DIR / f"{chat_id}{ext}"
            if p.exists():
                found_logo = p
                break
        logo_groups[found_logo].append(chat_id)

    is_edit = len(prev_message_ids) > 0
    log.info("📸 %s group to %d subscriber(s) in %d logo group(s)...",
             "Editing" if is_edit else "Sending", len(subscribers), len(logo_groups))

    for logo_path, chat_ids in logo_groups.items():
        group_name = logo_path.name if logo_path else "default"
        final_path = OUTPUT_DIR / f"broadcast_{group_name}_{int(time.time())}.png"

        try:
            overlay_screenshot(raw_path, final_path, theme=theme, custom_logo_path=logo_path)

            for chat_id in chat_ids:
                prev_msg_id = prev_message_ids.get(chat_id)
                try:
                    if prev_msg_id:
                        with open(final_path, "rb") as f:
                            media_payload = json.dumps({
                                "type": "photo",
                                "media": "attach://photo",
                                "caption": caption,
                            })
                            resp = requests.post(
                                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageMedia",
                                data={
                                    "chat_id": chat_id,
                                    "message_id": prev_msg_id,
                                    "media": media_payload,
                                },
                                files={"photo": (f"alert_{group_name}.png", f, "image/png")},
                                timeout=30,
                            )
                        if resp.ok:
                            sent_ids[chat_id] = prev_msg_id
                            log.info("   ✏️  Edited msg %s in %s (%s)", prev_msg_id, chat_id, group_name)
                        else:
                            log.warning("   ⚠️  Edit failed for %s (msg %s): %s — sending new",
                                        chat_id, prev_msg_id, resp.text[:80])
                            prev_msg_id = None

                    if not prev_msg_id:
                        with open(final_path, "rb") as f:
                            resp = requests.post(
                                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                                data={"chat_id": chat_id, "caption": caption},
                                files={"photo": (f"alert_{group_name}.png", f, "image/png")},
                                timeout=30,
                            )
                        if resp.ok:
                            msg_id = resp.json().get("result", {}).get("message_id")
                            if msg_id:
                                sent_ids[chat_id] = msg_id
                            log.info("   ✅ Sent to %s (%s)", chat_id, group_name)
                        elif resp.status_code == 403:
                            log.warning("   ❌ Forbidden from %s — removing", chat_id)
                            remove_subscriber(chat_id)
                        else:
                            log.error("   ❌ Failed for %s: %s", chat_id, resp.text[:100])

                except Exception as e:
                    log.error("   ❌ Error sending to %s: %s", chat_id, e)

            final_path.unlink(missing_ok=True)
        except Exception as e:
            log.error("📸 Failed to overlay/send for logo group %s: %s", group_name, e)

    return sent_ids


def broadcast_all_groups(
    active_groups: dict[int, GroupState],
    alerts_data: dict,
    centroids: dict[str, tuple[float, float]],
    theme: str = "dark",
) -> None:
    """
    Geographic-aware broadcast: sends separate Telegram messages per contiguous
    cluster of alerted cities. Edits existing messages when only new cities of the
    same status join a group; sends new messages on status upgrades or new clusters.
    Mutates active_groups in-place.
    """
    now = time.time()

    # Build city_by_status for all active primary alerts.
    # In test mode (TEST_CHANNEL_ID set), include is_test alerts too.
    city_by_status: dict[str, str] = {}
    for _key, alert in alerts_data.items():
        if not isinstance(alert, dict):
            continue
        if not TEST_CHANNEL_ID and (alert.get("is_test") or alert.get("test") or alert.get("isTest")):
            continue
        status = alert.get("status", "")
        city_he = alert.get("city_name_he", "")
        if city_he and _is_primary_alert(status):
            city_by_status[city_he] = status

    if not city_by_status:
        log.info("📸 No primary alerts — skipping broadcast_all_groups")
        return

    # Cluster active cities geographically
    new_clusters = _cluster_cities(list(city_by_status.keys()), centroids)
    log.info("📸 %d alert cities → %d geographic cluster(s)", len(city_by_status), len(new_clusters))

    # Match clusters to existing group states
    matched, unmatched_new, orphaned = _match_clusters_to_states(new_clusters, active_groups)

    # Process matched clusters — each gets its own screenshot centred on the cluster
    for ci, (cluster_cities, state) in matched.items():
        cluster_city_by_status = {c: city_by_status[c] for c in cluster_cities}
        new_same, upgraded = _compute_group_diff(cluster_city_by_status, state)

        new_snapshot = {s: frozenset(cs) for s, cs in
                        _group_cities_by_status(cluster_city_by_status).items()}

        if upgraded:
            # Status upgrades → NEW message for only the upgraded cities
            upgraded_all: set[str] = set()
            for cities_set in upgraded.values():
                upgraded_all.update(cities_set)

            caption = _build_caption(alerts_data, city_filter=upgraded_all)
            lat, lon, zoom = _compute_cluster_view(cluster_cities, centroids)
            log.info("📸 Group %d: status upgrade in %d city/cities → new message (zoom=%s)",
                     state.group_id, len(upgraded_all), zoom)
            raw_path = _capture_raw_screenshot(theme, lat, lon, zoom)
            if raw_path:
                new_ids = _send_group_to_subscribers(caption, raw_path, theme, prev_message_ids=None)
                raw_path.unlink(missing_ok=True)
            else:
                new_ids = {}

            state.city_names = cluster_cities
            state.last_broadcast_snapshot = new_snapshot
            state.message_ids = new_ids
            state.last_broadcast_time = now

        elif new_same:
            # New cities at same status → EDIT if within EDIT_WINDOW, else NEW
            within_edit = (now - state.last_broadcast_time) < EDIT_WINDOW and bool(state.message_ids)
            caption = _build_caption(alerts_data, city_filter=cluster_cities)
            lat, lon, zoom = _compute_cluster_view(cluster_cities, centroids)

            if within_edit:
                log.info("📸 Group %d: %d new same-status city/cities → edit (%.0fs < %ds, zoom=%s)",
                         state.group_id, len(new_same),
                         now - state.last_broadcast_time, EDIT_WINDOW, zoom)
                raw_path = _capture_raw_screenshot(theme, lat, lon, zoom)
                if raw_path:
                    new_ids = _send_group_to_subscribers(caption, raw_path, theme,
                                                         prev_message_ids=state.message_ids)
                    raw_path.unlink(missing_ok=True)
                else:
                    new_ids = {}
            else:
                log.info("📸 Group %d: %d new same-status city/cities → new message (zoom=%s)",
                         state.group_id, len(new_same), zoom)
                raw_path = _capture_raw_screenshot(theme, lat, lon, zoom)
                if raw_path:
                    new_ids = _send_group_to_subscribers(caption, raw_path, theme,
                                                         prev_message_ids=None)
                    raw_path.unlink(missing_ok=True)
                else:
                    new_ids = {}

            state.city_names = cluster_cities
            state.last_broadcast_snapshot = new_snapshot
            state.message_ids = new_ids
            state.last_broadcast_time = now

        else:
            # No actionable change — update city_names in case cluster shape shifted
            state.city_names = cluster_cities

    # Process brand-new clusters
    for cluster_cities in unmatched_new:
        cluster_city_by_status = {c: city_by_status[c] for c in cluster_cities}
        caption = _build_caption(alerts_data, city_filter=cluster_cities)
        lat, lon, zoom = _compute_cluster_view(cluster_cities, centroids)
        log.info("📸 New geographic group of %d city/cities → new message (zoom=%s)",
                 len(cluster_cities), zoom)
        raw_path = _capture_raw_screenshot(theme, lat, lon, zoom)
        if raw_path:
            new_ids = _send_group_to_subscribers(caption, raw_path, theme, prev_message_ids=None)
            raw_path.unlink(missing_ok=True)
        else:
            new_ids = {}

        new_snapshot = {s: frozenset(cs) for s, cs in
                        _group_cities_by_status(cluster_city_by_status).items()}
        group_id = next(_group_id_counter)
        active_groups[group_id] = GroupState(
            group_id=group_id,
            city_names=cluster_cities,
            last_broadcast_snapshot=new_snapshot,
            message_ids=new_ids,
            last_broadcast_time=now,
        )

    # Remove orphaned states (alerts cleared for those areas)
    for state in orphaned:
        active_groups.pop(state.group_id, None)
        log.info("📸 Removed orphaned group %d (alerts cleared)", state.group_id)


def _group_cities_by_status(city_by_status: dict[str, str]) -> dict[str, set[str]]:
    """Helper: invert city->status to status->set[city]."""
    result: dict[str, set[str]] = defaultdict(set)
    for city, status in city_by_status.items():
        result[status].add(city)
    return dict(result)


def main():
    log.info("=== ClearMap Screenshot Service starting ===")

    if not TELEGRAM_BOT_TOKEN:
        log.error("CLEARMAP_BOT_TOKEN not set! Exiting.")
        sys.exit(1)

    init_firebase()

    # State tracking
    last_screenshot_time = 0.0
    sliding_window_end = 0.0
    pending_screenshot = False
    # Track (key, status) pairs so status upgrades (e.g. pre_alert → alert) are detected
    previous_primary_pairs: set[tuple[str, str]] = set()
    latest_snapshot: dict = {}
    snapshot_lock = threading.Lock()
    # Per-geographic-group state: group_id -> GroupState
    active_groups: dict[int, GroupState] = {}
    centroids = _load_centroids()

    def on_alerts_change(event):
        """Firebase listener callback — fires on every change to active_alerts."""
        nonlocal pending_screenshot, sliding_window_end, previous_primary_pairs, latest_snapshot

        # Always fetch full snapshot to avoid issues with Firebase patch events
        # at path "/" that only contain changed entries.
        data = event.data
        if event.path != "/" or data is None or getattr(event, "event_type", None) == "patch":
            ref = db.reference(FIREBASE_NODE)
            data = ref.get() or {}

        with snapshot_lock:
            latest_snapshot = data if isinstance(data, dict) else {}

        # Build (key, status) pairs so we detect both new alerts AND status upgrades
        current_primary_pairs = set()
        if isinstance(data, dict):
            for key, alert in data.items():
                if isinstance(alert, dict):
                    status = alert.get("status", "")
                    if _is_primary_alert(status):
                        # Skip test alerts unless TEST_CHANNEL_ID is configured
                        if not TEST_CHANNEL_ID and (alert.get("is_test") or alert.get("test") or alert.get("isTest")):
                            continue
                        current_primary_pairs.add((key, status))

        new_or_changed = current_primary_pairs - previous_primary_pairs
        previous_primary_pairs = current_primary_pairs

        if new_or_changed:
            now = time.time()
            sliding_window_end = now + BATCH_DELAY
            pending_screenshot = True
            log.info("🔔 %d new/upgraded primary alert(s) detected! Sliding window set to %ds.",
                     len(new_or_changed), BATCH_DELAY)

    # Start Firebase listener
    log.info("👂 Listening for alert changes on Firebase: %s", FIREBASE_NODE)
    ref = db.reference(FIREBASE_NODE)
    ref.listen(on_alerts_change)

    # Start Telegram command listener
    threading.Thread(target=telegram_listener, daemon=True).start()

    log.info("📸 Broadcast config: cooldown=%ds batch_delay=%ds edit_window=%ds cluster_km=%.0f",
             SCREENSHOT_COOLDOWN, BATCH_DELAY, EDIT_WINDOW, GEO_CLUSTER_KM)
    if TEST_CHANNEL_ID:
        log.warning("TEST MODE active: routing all messages (incl. is_test alerts) to %s only", TEST_CHANNEL_ID)

    # Main loop — handles batching and cooldown
    while True:
        try:
            if pending_screenshot:
                now = time.time()

                if now >= sliding_window_end:
                    time_since_last = now - last_screenshot_time
                    # Allow through if cooldown elapsed OR if any group has a live editable message
                    can_edit = any(gs.message_ids for gs in active_groups.values())
                    if time_since_last >= SCREENSHOT_COOLDOWN or can_edit:
                        with snapshot_lock:
                            current_data = dict(latest_snapshot)

                        if current_data:
                            # Filter out test alerts to see if there's anything real to show
                            # (In test mode, treat is_test alerts as real)
                            real_alerts = {
                                k: v for k, v in current_data.items()
                                if isinstance(v, dict) and (
                                    TEST_CHANNEL_ID or
                                    not (v.get("is_test") or v.get("test") or v.get("isTest"))
                                )
                            }

                            if not real_alerts:
                                log.info("📸 No non-test alerts active — skipping screenshot.")
                                pending_screenshot = False
                                continue

                            # Clear the flag BEFORE the blocking broadcast so that any new
                            # alerts arriving during capture/send can re-arm it correctly.
                            pending_screenshot = False
                            log.info("📸 Cooldown & batch window elapsed — broadcasting for %d alerts",
                                     len(real_alerts))
                            try:
                                broadcast_all_groups(active_groups, current_data, centroids)
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
