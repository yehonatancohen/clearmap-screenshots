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
SCREENSHOT_URL = os.environ.get("SCREENSHOT_URL", "") or _cfg.get("SCREENSHOT_URL", "https://www.clearmap.co.il/broadcast?uav=true&ellipse=true")
SCREENSHOT_COOLDOWN = int(os.environ.get("SCREENSHOT_COOLDOWN", "") or _cfg.get("SCREENSHOT_COOLDOWN", "120"))
BATCH_DELAY = int(os.environ.get("SCREENSHOT_BATCH_DELAY", "") or _cfg.get("SCREENSHOT_BATCH_DELAY", "10"))
FIREBASE_DB_URL = "https://clear-map-f20d0-default-rtdb.europe-west1.firebasedatabase.app/"
FIREBASE_NODE = "/public_state/active_alerts"
FIREBASE_SUBSCRIBERS = "/public_state/subscribers"

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

def capture_screenshot(url: str, output_path: Path, theme: str = "dark", 
                       size: int = 1080, custom_logo_path: Path | None = None) -> tuple[Path, Path]:
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

def get_subscribers() -> list[str]:
    """Fetch the list of subscriber chat IDs from Firebase."""
    try:
        ref = db.reference(FIREBASE_SUBSCRIBERS)
        subs = ref.get()
        if isinstance(subs, dict):
            return [str(chat_id) for chat_id in subs.keys()]
        return []
    except Exception as e:
        log.error("Failed to fetch subscribers: %s", e)
        return []


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
            temp_path.replace(final_path)
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

def broadcast_to_subscribers(caption: str, theme: str = "dark"):
    """
    Capture a raw screenshot once, then overlay and broadcast 
    custom-branded screenshots to each registered subscriber.
    """
    from screenshot_overlay import overlay_screenshot

    subscribers = get_subscribers()
    if TELEGRAM_CHANNEL_ID and TELEGRAM_CHANNEL_ID not in subscribers:
        subscribers.append(TELEGRAM_CHANNEL_ID)

    if not subscribers:
        log.warning("📸 No subscribers found for broadcast!")
        return

    # 1. Capture Raw Screenshot (no overlays yet)
    try:
        # We use a temporary dummy path for capture_screenshot's final output
        # and it returns the raw path as well.
        dummy_path = OUTPUT_DIR / "dummy.png"
        _, raw_file = capture_screenshot(SCREENSHOT_URL, dummy_path, theme=theme)
    except Exception as e:
        log.error("📸 Failed to capture base screenshot: %s", e)
        return

    # Group subscribers by custom logo
    logo_groups = defaultdict(list)
    for chat_id in subscribers:
        # Try different extensions
        found_logo = None
        for ext in (".png", ".jpg", ".jpeg"):
            p = CUSTOM_LOGOS_DIR / f"{chat_id}{ext}"
            if p.exists():
                found_logo = p
                break
        logo_groups[found_logo].append(chat_id)

    log.info("📸 Broadcasting to %d subscribers in %d logo groups...", 
             len(subscribers), len(logo_groups))

    # For each group, generate an overlaid screenshot and send
    for logo_path, chat_ids in logo_groups.items():
        group_name = logo_path.name if logo_path else "default"
        final_path = OUTPUT_DIR / f"broadcast_{group_name}_{int(time.time())}.png"
        
        try:
            overlay_screenshot(raw_file, final_path, theme=theme, custom_logo_path=logo_path)
            
            for chat_id in chat_ids:
                try:
                    with open(final_path, "rb") as f:
                        resp = requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                            data={"chat_id": chat_id, "caption": caption},
                            files={"photo": (f"alert_{group_name}.png", f, "image/png")},
                            timeout=30,
                        )
                    if resp.ok:
                        log.info("   ✅ Sent to %s (%s)", chat_id, group_name)
                    elif resp.status_code == 403:
                        log.warning("   ❌ Forbidden from %s — removing", chat_id)
                        remove_subscriber(chat_id)
                    else:
                        log.error("   ❌ Failed for %s: %s", chat_id, resp.text[:100])
                except Exception as e:
                    log.error("   ❌ Error sending to %s: %s", chat_id, e)
            
            # Clean up group-specific screenshot
            final_path.unlink(missing_ok=True)
        except Exception as e:
            log.error("📸 Failed to overlay/send for group %s: %s", group_name, e)

    # Clean up raw file and dummy
    raw_file.unlink(missing_ok=True)
    dummy_path.unlink(missing_ok=True)




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

    # Start Telegram command listener
    threading.Thread(target=telegram_listener, daemon=True).start()

    log.info("📸 Broadcast config: cooldown=%ds batch_delay=%ds",
             SCREENSHOT_COOLDOWN, BATCH_DELAY)

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
                                caption = _build_caption(current_data)
                                log.info("📸 Caption: %s", caption)
                                broadcast_to_subscribers(caption)
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
