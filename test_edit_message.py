"""
Quick test for the Telegram send → edit flow.

Sends a test image to the channel, waits a few seconds,
then edits the same message with a new image + caption.

Usage:
    python test_edit_message.py

Reads CLEARMAP_BOT_TOKEN and TELEGRAM_CHANNEL_ID from config.env
(same as main.py).
"""

import json
import time
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

# ── Config (reuse main.py's loader) ────────────────────────────────────────

def _load_config_env() -> dict[str, str]:
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

import os, requests

_cfg = _load_config_env()
BOT_TOKEN = os.environ.get("CLEARMAP_BOT_TOKEN", "") or _cfg.get("CLEARMAP_BOT_TOKEN", "")
CHANNEL_ID = "-5197151796"  # Test channel only

API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ── Helpers ────────────────────────────────────────────────────────────────

def make_test_image(text: str, color: str = "#FF2A2A") -> Path:
    """Generate a simple test image with text."""
    img = Image.new("RGB", (600, 400), "#0a0a14")
    draw = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("arial.ttf", 32)
        small = ImageFont.truetype("arial.ttf", 18)
    except OSError:
        font = ImageFont.load_default()
        small = font

    draw.text((300, 160), text, fill=color, font=font, anchor="mm")
    draw.text((300, 220), time.strftime("%H:%M:%S"), fill="#888888", font=small, anchor="mm")
    draw.text((300, 350), "test_edit_message.py", fill="#444444", font=small, anchor="mm")

    path = Path(__file__).parent / "test_edit_img.png"
    img.save(path)
    return path


def send_photo(chat_id: str, image_path: Path, caption: str) -> int | None:
    """Send a photo and return the message_id."""
    with open(image_path, "rb") as f:
        resp = requests.post(
            f"{API}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption},
            files={"photo": ("test.png", f, "image/png")},
            timeout=30,
        )
    if resp.ok:
        msg_id = resp.json().get("result", {}).get("message_id")
        print(f"  [OK] sendPhoto → message_id={msg_id}")
        return msg_id
    else:
        print(f"  [FAIL] sendPhoto: {resp.status_code} {resp.text[:120]}")
        return None


def edit_photo(chat_id: str, message_id: int, image_path: Path, caption: str) -> bool:
    """Edit an existing photo message with a new image + caption."""
    media_payload = json.dumps({
        "type": "photo",
        "media": "attach://photo",
        "caption": caption,
    })
    with open(image_path, "rb") as f:
        resp = requests.post(
            f"{API}/editMessageMedia",
            data={
                "chat_id": chat_id,
                "message_id": message_id,
                "media": media_payload,
            },
            files={"photo": ("test.png", f, "image/png")},
            timeout=30,
        )
    if resp.ok:
        print(f"  [OK] editMessageMedia → message_id={message_id} updated")
        return True
    else:
        print(f"  [FAIL] editMessageMedia: {resp.status_code} {resp.text[:120]}")
        return False


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        print("ERROR: CLEARMAP_BOT_TOKEN not set in config.env or env vars")
        return
    if not CHANNEL_ID:
        print("ERROR: TELEGRAM_CHANNEL_ID not set in config.env or env vars")
        return

    print(f"Bot token: ...{BOT_TOKEN[-6:]}")
    print(f"Channel: {CHANNEL_ID}")
    print()

    # Step 1: Send initial message
    print("Step 1: Sending initial screenshot...")
    img1 = make_test_image("Initial Alert\n3 cities")
    msg_id = send_photo(CHANNEL_ID, img1, "🔴 3 התרעות ירי רקטות וטילים:\n  • תל אביב\n  • חולון\n  • בת ים")

    if not msg_id:
        print("\nFailed to send — check bot token and channel ID.")
        return

    # Step 2: Wait, then edit
    wait = 5
    print(f"\nStep 2: Waiting {wait}s then editing the same message...")
    time.sleep(wait)

    img2 = make_test_image("Updated Alert\n7 cities", color="#FF6A00")
    ok = edit_photo(
        CHANNEL_ID, msg_id, img2,
        "🔴 7 התרעות ירי רקטות וטילים:\n  • תל אביב\n  • חולון\n  • בת ים\n  • רמת גן\n  • גבעתיים\n  • בני ברק\n  • פתח תקווה"
    )

    # Step 3: Edit again
    if ok:
        print(f"\nStep 3: Waiting {wait}s then editing again...")
        time.sleep(wait)

        img3 = make_test_image("Final Update\n12 cities", color="#E040FB")
        edit_photo(
            CHANNEL_ID, msg_id, img3,
            "🔴 12 התרעות ירי רקטות וטילים\n\n(This message was edited twice — test passed!)"
        )

    # Cleanup
    img1.unlink(missing_ok=True)
    img2.unlink(missing_ok=True)
    img3.unlink(missing_ok=True)

    print("\nDone! Check your Telegram channel — you should see ONE message that was updated in-place.")


if __name__ == "__main__":
    main()
