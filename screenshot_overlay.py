"""
Screenshot overlay — logo, legend, and UAV disclaimer via Pillow.

This module handles all post-processing of raw browser screenshots:
- Center-crop to square
- Resize to target size
- Overlay logo in top-right corner
- Draw legend with active alert counts
- Draw UAV disclaimer if applicable

Kept separate from the browser capture logic so it can be tested independently.
"""

import os
import re
from pathlib import Path

import requests as http_requests
from PIL import Image, ImageDraw, ImageFont

# Firebase REST API for fetching active alert status counts
FIREBASE_DB_URL = "https://clear-map-f20d0-default-rtdb.europe-west1.firebasedatabase.app"
FIREBASE_ALERTS_PATH = "/public_state/active_alerts.json"

LOGO_DIR = Path(__file__).parent / "public"

# Status → (color, Hebrew label)
LEGEND_ITEMS = [
    ("alert",       (255,  42,  42), "התרעות ירי רקטות וטילים"),
    ("uav",         (224,  64, 251), "התרעות חדירת כלי טיס עוין"),
    ("terrorist",   (255,   0,  85), "חדירת מחבלים"),
    ("pre_alert",   (255, 106,   0), "התרעות מוקדמות"),
    ("after_alert", (255,  80,  80), "להישאר בממ\"ד"),
]


def _bidi_text(text: str) -> str:
    """Convert Hebrew (RTL) text to visual order based on Pillow's capabilities."""
    from PIL import features
    if features.check('raqm'):
        return text

    try:
        from bidi.algorithm import get_display
        return get_display(text, base_dir='R')
    except ImportError:
        pass

    tokens = re.findall(r'\S+|\s+', text)
    visual = []
    for token in reversed(tokens):
        if token.isspace() or token.isdigit() or re.match(r'^[0-9\W]+$', token):
            visual.append(token)
        else:
            visual.append(token[::-1])
    return "".join(visual)


def _load_hebrew_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Try to load a Hebrew-capable font."""
    candidates = [
        str(LOGO_DIR / "hebrew_font.ttf"),
        "public/hebrew_font.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                pass
    return ImageFont.load_default()


def fetch_active_statuses() -> tuple[set[str], dict[str, int]]:
    """Fetch currently active alert statuses and counts from Firebase REST API."""
    try:
        resp = http_requests.get(
            f"{FIREBASE_DB_URL}{FIREBASE_ALERTS_PATH}",
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return set(), {}
        counts: dict[str, int] = {}
        for v in data.values():
            if isinstance(v, dict):
                # Skip test alerts
                if v.get("is_test") or v.get("test") or v.get("isTest"):
                    continue
                s = v.get("status", "alert")
                counts[s] = counts.get(s, 0) + 1
        return set(counts.keys()), counts
    except Exception as e:
        print(f"  [warn] Could not fetch active statuses: {e}")
        return set(), {}


def draw_legend(img: Image.Image, active_statuses: set[str], theme: str,
                counts: dict[str, int] | None = None) -> Image.Image:
    """Draw a small legend overlay in the bottom-left corner."""
    items = [(color, label, status) for status, color, label in LEGEND_ITEMS
             if status in active_statuses]

    if not items:
        return img

    img = img.copy().convert("RGBA")
    w, h = img.size

    font_size = max(18, int(w * 0.024))
    font = _load_hebrew_font(font_size)
    dot_radius = max(5, int(font_size * 0.4))
    row_height = int(font_size * 1.8)
    padding = int(w * 0.02)
    inner_pad = int(w * 0.012)

    dummy_draw = ImageDraw.Draw(img)
    max_text_w = 0
    for _, label, status in items:
        cnt = counts.get(status, 0) if counts else 0
        if status == "after_alert":
            display = f"{cnt} מקומות להישאר במרחב מוגן" if cnt else "מקומות להישאר במרחב מוגן"
        else:
            display = f"{cnt} {label}" if cnt else label

        visual_label = _bidi_text(display)
        from PIL import features
        kwargs = {"direction": "rtl", "language": "he"} if features.check('raqm') else {}
        bbox = dummy_draw.textbbox((0, 0), visual_label, font=font, **kwargs)
        text_w = bbox[2] - bbox[0]
        max_text_w = max(max_text_w, text_w)

    legend_w = dot_radius * 2 + inner_pad + max_text_w + padding * 2
    legend_h = row_height * len(items) + padding * 2

    lx = padding
    ly = h - legend_h - padding

    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)

    bg_color = (20, 20, 30, 180) if theme == "dark" else (30, 30, 40, 170)
    overlay_draw.rounded_rectangle(
        [lx, ly, lx + legend_w, ly + legend_h],
        radius=int(w * 0.012),
        fill=bg_color,
    )
    img = Image.alpha_composite(img, overlay)
    draw = ImageDraw.Draw(img)

    for i, (color, label, status) in enumerate(items):
        row_y = ly + padding + i * row_height
        dot_cx = lx + legend_w - padding - dot_radius
        dot_cy = row_y + row_height // 2

        draw.ellipse(
            [dot_cx - dot_radius, dot_cy - dot_radius,
             dot_cx + dot_radius, dot_cy + dot_radius],
            fill=(*color, 255),
        )

        cnt = counts.get(status, 0) if counts else 0
        if status == "after_alert":
            display = f"{cnt} מקומות להישאר במרחב מוגן" if cnt else "מקומות להישאר במרחב מוגן"
        else:
            display = f"{cnt} {label}" if cnt else label

        visual_label = _bidi_text(display)
        from PIL import features
        kwargs = {"direction": "rtl", "language": "he"} if features.check('raqm') else {}
        bbox = draw.textbbox((0, 0), visual_label, font=font, **kwargs)
        text_w = bbox[2] - bbox[0]
        text_x = dot_cx - dot_radius - inner_pad - text_w
        text_y = row_y + (row_height - font_size) // 2

        draw.text((text_x, text_y), visual_label, fill=(255, 255, 255, 230), font=font, **kwargs)

    return img


def draw_uav_disclaimer(img: Image.Image, theme: str) -> Image.Image:
    """Draw a disclaimer centered at the top about UAV predictions."""
    w, h = img.size

    text = "* שימו לב: מיקומי כלי הטיס הם בגדר השערת המערכת בלבד ואין להתבסס עליהם."
    visual_text = _bidi_text(text)

    font_size = max(15, int(w * 0.022))
    font = _load_hebrew_font(font_size)
    padding = int(w * 0.012)

    dummy_draw = ImageDraw.Draw(img)
    from PIL import features
    kwargs = {"direction": "rtl", "language": "he"} if features.check('raqm') else {}
    bbox = dummy_draw.textbbox((0, 0), visual_text, font=font, **kwargs)
    text_w = bbox[2] - bbox[0]
    text_h = int(font_size * 1.2)

    box_w = text_w + padding * 2
    box_h = text_h + padding * 2

    # Center at the top
    lx = (w - box_w) // 2
    ly = padding * 2

    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)

    bg_color = (20, 20, 30, 200) if theme == "dark" else (30, 30, 40, 190)
    overlay_draw.rounded_rectangle(
        [lx, ly, lx + box_w, ly + box_h],
        radius=int(w * 0.01),
        fill=bg_color,
    )
    img = Image.alpha_composite(img.convert("RGBA"), overlay)
    draw = ImageDraw.Draw(img)

    text_x = lx + padding
    text_y = ly + padding + (box_h - padding * 2 - text_h) // 2

    draw.text((text_x, text_y), visual_text, fill=(253, 224, 71, 230), font=font, **kwargs)

    return img


def overlay_screenshot(raw_path: Path, output_path: Path, size: int = 1080,
                       theme: str = "dark", custom_logo_path: Path | None = None) -> Path:
    """Full post-processing pipeline: crop, resize, logo, legend, disclaimer."""
    print(f"[+] Fetching active alert statuses...")
    active_statuses, status_counts = fetch_active_statuses()
    if active_statuses:
        print(f"  Active types: {', '.join(sorted(active_statuses))}")
        print(f"  Counts: {status_counts}")

    img = Image.open(raw_path).convert("RGBA")

    # Center-crop to square
    w, h = img.size
    side = min(w, h)
    left = (w - side) // 2
    top = (h - side) // 2
    img = img.crop((left, top, left + side, top + side))

    # Resize to target size
    img = img.resize((size, size), Image.LANCZOS)

    # Overlay default logo (top-right)
    logo_path = LOGO_DIR / f"logo-{theme}-theme.png"
    if logo_path.exists():
        logo = Image.open(logo_path).convert("RGBA")
        logo_w = int(size * 0.25)
        logo_h = int(logo.height * (logo_w / logo.width))
        logo = logo.resize((logo_w, logo_h), Image.LANCZOS)

        padding = int(size * 0.03)
        x = size - logo_w - padding
        y = padding
        img.paste(logo, (x, y), logo)
    else:
        print(f"  [warn] Logo not found: {logo_path}")

    # Overlay custom logo (top-left) if provided
    if custom_logo_path and custom_logo_path.exists():
        try:
            c_logo = Image.open(custom_logo_path).convert("RGBA")
            # Limit width to 20% of image size
            c_logo_w = int(size * 0.20)
            c_logo_h = int(c_logo.height * (c_logo_w / c_logo.width))
            # If height is too large, scale by height instead
            if c_logo_h > int(size * 0.15):
                c_logo_h = int(size * 0.15)
                c_logo_w = int(c_logo.width * (c_logo_h / c_logo.height))

            c_logo = c_logo.resize((c_logo_w, c_logo_h), Image.LANCZOS)

            padding = int(size * 0.03)
            img.paste(c_logo, (padding, padding), c_logo)
            print(f"  [+] Overlayed custom logo: {custom_logo_path.name}")
        except Exception as e:
            print(f"  [warn] Could not overlay custom logo {custom_logo_path}: {e}")

    # Draw legend
    if active_statuses:
        img = draw_legend(img, active_statuses, theme, counts=status_counts)
        if "uav" in active_statuses:
            img = draw_uav_disclaimer(img, theme)

    # Save
    img = img.convert("RGB")
    img.save(str(output_path), "PNG", quality=95)
    return output_path

