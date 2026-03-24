from __future__ import annotations

import asyncio
import logging
import os
import re
import textwrap
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse, parse_qs
from PIL import Image
import io

import aiosqlite
import requests
from bs4 import BeautifulSoup

import discord
from discord.ext import commands, tasks

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1484113318095622315"))

WIKI_BASE = "https://silveraqworld.wikidot.com"
RECENT_URL_HTTP = "http://silveraqworld.wikidot.com/system:recent-changes"
RSS_URL = "http://aqwwiki.wikidot.com/feed/site-changes.xml"
DB = "drops.db"

CHECK_DAYS = 7
MAX_POSTS_PER_RUN = 100

MAX_DESC_LENGTH = 3800  # keep under discord 4096
MAX_TITLE_LENGTH = 256
WRAP_WIDTH = 55

# ---------------- DISCORD ----------------
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)


# ---------------- DATABASE ----------------
async def init_db():
    """Initialize the SQLite database for tracking posted items with grouping."""
    async with aiosqlite.connect(DB) as db:
        # Create items table for individual items
        await db.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                content TEXT,
                price TEXT,
                rarity TEXT,
                image TEXT,
                group_key TEXT,
                created_at TIMESTAMP
            )
        """)
        
        # Create groups table for message tracking
        await db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                group_key TEXT PRIMARY KEY,
                message_id TEXT,
                channel_id TEXT,
                last_updated TIMESTAMP
            )
        """)
        
        await db.commit()


def make_group_key(item: dict) -> str:
    """Generate group key from item fields for grouping."""
    # Get normalized fields
    loc = (item.get("content", "") or "").lower()
    price = (item.get("price", "") or "").lower()
    rarity = (item.get("rarity", "") or "").lower()
    
    # Extract locations and normalize
    loc_match = re.search(r"locations?\s*:?\s*(.+)", loc, re.IGNORECASE)
    if loc_match:
        locations = loc_match.group(1).strip()
    else:
        locations = ""
    
    # Build group key from normalized fields
    key_parts = []
    if locations and locations != "n/a":
        key_parts.append(f"loc:{locations}")
    if price and price != "n/a":
        key_parts.append(f"price:{price}")
    if rarity and rarity != "n/a":
        key_parts.append(f"rarity:{rarity}")
    
    return "|".join(sorted(key_parts))


def categorize_item(item: dict) -> str:
    """Categorize an item using keyword-based detection."""
    title = item.get("title", "").lower()
    content = item.get("content", "").lower()
    
    # Define category mappings with proper singular/plural forms
    category_mappings = {
        # Weapon types (singular -> plural)
        "axe": "axes",
        "bow": "bows", 
        "dagger": "daggers",
        "gauntlet": "gauntlets",
        "gun": "guns",
        "handgun": "handguns",
        "mace": "maces",
        "polearm": "polearms",
        "rifle": "rifles",
        "staff": "staffs",
        "sword": "swords",
        "wand": "wands",
        "whip": "whips",
        
        # Main categories (singular -> plural)
        "weapon": "weapons",
        "armor": "armors", 
        "helm": "helms",
        "cape": "capes",
        "pet": "pets"
    }
    
    # Check keywords in title and content
    for singular, plural in category_mappings.items():
        if singular in title or singular in content:
            return plural
    
    # Fallback to misc
    return "misc"


def format_category_header(category: str, item_count: int) -> str:
    """Format category header with proper singular/plural form."""
    # Irregular plurals mapping (plural -> singular)
    singular_forms = {
        "axes": "axe",
        "bows": "bow", 
        "daggers": "dagger",
        "gauntlets": "gauntlet",
        "guns": "gun",
        "handguns": "handgun",
        "maces": "mace",
        "polearms": "polearm",
        "rifles": "rifle",
        "staffs": "staff",
        "swords": "sword",
        "wands": "wand",
        "whips": "whip",
        "weapons": "weapon",
        "armors": "armor",
        "helms": "helm", 
        "capes": "cape",
        "pets": "pet",
        "misc": "misc"
    }
    
    # Use singular form for 1 item, plural for multiple
    if item_count == 1:
        return f"**{singular_forms.get(category, category)}:**"
    else:
        return f"**{category.capitalize()}:**"

async def is_posted(pid: str) -> bool:
    """Check if item is already posted."""
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT 1 FROM items WHERE id=?", (pid,)) as cur:
            return await cur.fetchone() is not None

async def mark_posted(pid: str, item: dict):
    """Mark an item as posted to avoid duplicates."""
    async with aiosqlite.connect(DB) as db:
        await db.execute("INSERT OR IGNORE INTO items (id, url, title, content, price, rarity, image, group_key, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))", 
                          (pid, item.get("url"), item.get("title"), item.get("content"), 
                           item.get("price"), item.get("rarity"), item.get("image"), 
                           make_group_key(item)))
        await db.commit()


# ---------------- HELPERS ----------------
def _make_absolute(url: str, base: str | None = None) -> str:
    if not url or url.startswith(("http://", "https://")):
        return url or ""
    base = WIKI_BASE if not base else base
    return urljoin(base, url)


def parse_wiki_time(text: str) -> datetime | None:
    """
    Parse Wikidot recent-changes time strings.
    Supported:
    - `19 Mar 2026 06:46` / `19 Mar 2026 06:46:10` 
    - `20 Mar 26 - 00:00:00`  (your http format)
    Returns timezone-aware datetime in UTC.
    """
    if not text:
        return None

    t = text.replace("\xa0", " ").strip()
    t = re.sub(r"\s+", " ", t)

    # DD Mon YY - HH:MM:SS
    m = re.match(
        r"^(?P<day>\d{1,2})\s+(?P<mon>[A-Za-z]{3})\s+(?P<year>\d{2})\s*-\s*(?P<h>\d{1,2}):(?P<m>\d{2})(?::(?P<s>\d{2}))?$",
        t,
    )
    if m:
        year = 2000 + int(m.group("year"))
        mon = m.group("mon")
        mon_norm = mon[:1].upper() + mon[1:3].lower()
        month = datetime.strptime(mon_norm, "%b").month
        day = int(m.group("day"))
        hour = int(m.group("h"))
        minute = int(m.group("m"))
        second = int(m.group("s")) if m.group("s") else 0
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)

    # DD Mon YYYY HH:MM(:SS)
    m = re.match(
        r"^(?P<day>\d{1,2})\s+(?P<mon>[A-Za-z]{3})\s+(?P<year>\d{4})\s*(?:-|)?\s*(?P<h>\d{1,2}):(?P<m>\d{2})(?::(?P<s>\d{2}))?$",
        t,
    )
    if m:
        year = int(m.group("year"))
        mon = m.group("mon")
        mon_norm = mon[:1].upper() + mon[1:3].lower()
        month = datetime.strptime(mon_norm, "%b").month
        day = int(m.group("day"))
        hour = int(m.group("h"))
        minute = int(m.group("m"))
        second = int(m.group("s")) if m.group("s") else 0
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)

    return None


def page_has_aegift(soup: BeautifulSoup) -> bool:
    # Item pages have a tag list at the bottom; detect that
    for tag_el in soup.select(
        ".page-tags a, a[href*='tag/aegift'], a[href*='system:page-tags/tag/aegift']"
    ):
        txt = tag_el.get_text(strip=True).lower()
        if txt == "aegift":
            return True
        href = tag_el.get("href", "")
        if "aegift" in href.lower():
            return True
    
    # Debug: log what tags we actually find
    tags = soup.select(".page-tags a")
    if tags:
        tag_texts = [tag.get_text(strip=True) for tag in tags]
        log.debug("Found tags: %s", ", ".join(tag_texts))
    
    return False


def _wrap_lines(text: str) -> str:
    """Wrap lines to Discord's 4096 character limit with word boundaries."""
    if not text:
        return ""
    wrapped = textwrap.fill(text, width=WRAP_WIDTH, replace_whitespace=False, break_long_words=False)
    return wrapped


def generate_collage(image_urls: list[str]) -> bytes:
    """Generate a collage from multiple item images."""
    if not image_urls:
        return None
    
    # Download images temporarily
    images = []
    for url in image_urls:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                img_data = io.BytesIO(response.content)
                img = Image.open(img_data)
                images.append(img)
        except Exception as e:
            log.warning(f"Failed to download image {url}: {e}")
            continue
    
    if not images:
        return None
    
    # Determine layout based on number of images
    n = len(images)
    if n == 1:
        # Single image - use original
        return io.BytesIO(response.content)
    elif n == 2:
        # Two images - side by side
        width, height = max(img.width for img in images), max(img.height for img in images)
        collage = Image.new('RGBA', (width * 2, height), (255, 255, 255, 0))
        collage.paste(images[0], (0, 0))
        collage.paste(images[1], (width, 0))
    elif n <= 4:
        # 3-4 images - 2x2 grid
        width, height = max(img.width for img in images), max(img.height for img in images)
        collage = Image.new('RGBA', (width * 2, height * 2), (255, 255, 255, 0))
        for i, img in enumerate(images):
            x = (i % 2) * width
            y = (i // 2) * height
            collage.paste(img, (x, y))
    else:
        # 5-9 images - square grid
        size = int((300 * 300) ** 0.5)  # Approximate square layout
        grid_size = int(size ** 0.5)
        collage = Image.new('RGBA', (grid_size, grid_size), (255, 255, 255, 0))
        img_size = size // len(images)
        for i, img in enumerate(images):
            x = (i % grid_size) * img_size
            y = (i // grid_size) * img_size
            # Resize images to uniform size
            resized_img = img.resize((img_size, img_size), Image.Resampling.LANCZOS)
            collage.paste(resized_img, (x, y))
    
    # Convert to bytes
    img_bytes = io.BytesIO()
    collage.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    return img_bytes.getvalue()


def _extract_imgur_image(content_el: BeautifulSoup) -> str | None:
    # Prefer actual item image (usually i.imgur.com / imgur.com) but skip thumbnails/icons.
    for img in content_el.select("img[src]"):
        src = img.get("src")
        if not src:
            continue
        s = src.lower()
        if any(x in s for x in ("pixel", "spacer", "icon", "thumb")):
            continue
        full = _make_absolute(src, None)
        if "imgur.com" in full.lower():
            return full
    return None


def _extract_title_icons(soup: BeautifulSoup) -> str | None:
    """
    Extract the small "icon" tags displayed under the page title.

    AQW Wiki uses a `.page-tags` block with many `<a>` tag links (sometimes
    with `javascript:;` href). We render them as a space-separated list
    right under the embed title.
    """
    tag_els = soup.select(".page-tags a")
    if not tag_els:
        return None

    parts: list[str] = []
    for a in tag_els:
        txt = a.get_text(strip=True)
        if not txt:
            continue
        href = a.get("href") or ""
        href = href.strip()
        if href.startswith("javascript:"):
            parts.append(txt)
            continue
        full = _make_absolute(href, None)
        # Only hyperlink for normal urls; otherwise keep plain text.
        if full and full.lower().startswith(("http://", "https://")):
            parts.append(f"[{txt}]({full})")
        else:
            parts.append(txt)

    if not parts:
        return None
    return " ".join(parts)


def _clean_item_text(raw_text: str) -> tuple[str, str]:
    """
    Parse the item page text into a clean structured description.
    Only shows important fields: Location, Price/Dropped by, Rarity.
    """
    text = raw_text.replace("\r\n", "\n").replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    # Remove unwanted sections entirely (be more specific to avoid removing Notes)
    text = re.sub(
        r"Sell\s*back\s*:\s*.+?(?=(?:Rarity:\s*)|(?:Description:\s*)|(?:Notes?\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*)|$)",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(
        r"Description\s*:?\s*.+?(?=(?:Notes?\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*)|$)",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(
        r"Base\s*Damage\s*:?\s*.+?(?=(?:Notes?\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*)|$)",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    def _norm(val: str) -> str:
        val = re.sub(r"system:page-tags/tag/[^ \n]+", "", val, flags=re.IGNORECASE)
        # Keep newlines so multi-value fields (like multiple locations) can be listed.
        val = re.sub(r"[ \t]+", " ", val)
        val = re.sub(r"\n{3,}", "\n\n", val)
        val = val.strip()
        return val

    def _format_list(val: str) -> str:
        """
        Convert a multi-value field into Discord-friendly line items.
        Preserves original formatting including dashes and line connections.
        """
        v = (val or "").strip()
        if not v or v.upper() == "N/A":
            return "N/A"

        # Normalize multiple spaces but preserve single spaces and original structure
        v = re.sub(r"[ \t]+", " ", v).strip()
        
        # Split by newlines first to preserve original line structure
        lines = [ln.strip() for ln in v.split("\n") if ln.strip()]
        
        if len(lines) > 1:
            # Join lines with spaces to preserve connections like "Phlegethon Arena Trophies - Phlegethon Arena"
            # but keep different location groups on separate lines
            formatted_lines = []
            current_line = ""
            
            for line in lines:
                if line == "-":
                    # Dash separator, join with previous line
                    if current_line:
                        current_line += " - "
                    continue
                elif current_line and not current_line.endswith(" - "):
                    # Previous line was complete, start new line
                    formatted_lines.append(current_line)
                    current_line = line
                else:
                    # Continue current line (after dash or first line)
                    current_line += line if current_line.endswith(" - ") else f" {line}"
            
            if current_line:
                formatted_lines.append(current_line)
            
            return "\n".join(formatted_lines)

        # Fallback: comma-separated values
        if "," in v:
            parts = [p.strip() for p in v.split(",") if p.strip()]
            return "\n".join(parts) if parts else v

        return v

    # Capture only the important fields
    loc = "N/A"
    price = "N/A"
    rarity = "N/A"
    dropped_by = None
    merge_following = None
    note = None

    # Location field
    m_loc = re.search(
        r"Locations?\s*:?\s*(?P<val>.+?)\s*(?=(?:Price\s*:?)|(?:Dropped by\s*:?)|(?:Rarity\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_loc:
        loc = _norm(m_loc.group("val"))

    # Price field
    m_price = re.search(
        r"Price\s*:?\s*(?P<val>.+?)\s*(?=(?:Rarity\s*:?)|(?:Dropped by\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_price:
        price = _norm(m_price.group("val"))

    # Dropped by field (when Price is N/A)
    m_dropped = re.search(
        r"Dropped by\s*:?\s*(?P<val>.+?)\s*(?=(?:Merge the following\s*:?)|(?:Rarity\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_dropped:
        candidate = _norm(m_dropped.group("val"))
        if candidate and candidate.lower() not in {"n/a", "na"}:
            dropped_by = candidate

    # Merge the following field
    m_merge = re.search(
        r"Merge the following\s*:?\s*(?P<val>.+?)\s*(?=(?:Rarity\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_merge:
        candidate = _norm(m_merge.group("val"))
        if candidate and candidate.lower() not in {"n/a", "na"}:
            merge_following = candidate

    # Rarity field - more specific to stop at Note field
    m_rarity = re.search(
        r"Rarity\s*:?\s*(?P<val>.+?)\s*(?=(?:Rarity Description\s*:?)|(?:Notes?\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|\Z)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_rarity:
        rarity = _norm(m_rarity.group("val"))

    # Note field - capture only the first Note: occurrence
    m_note = re.search(
        r"Notes?\s*:?\s*(?P<val>.+?)(?=(?:\n\s*Notes?\s*:)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|\Z)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not m_note:
        # Try singular "Note:" pattern
        m_note = re.search(
            r"Note\s*:?\s*(?P<val>.+?)(?=(?:\n\s*Note\s*:)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|\Z)",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
    if m_note:
        candidate = _norm(m_note.group("val"))
        if candidate and candidate.lower() not in {"n/a", "na"}:
            note = candidate

    def _price_is_na(p: str) -> bool:
        p_norm = (p or "").strip()
        return p_norm.upper() == "N/A" or p_norm.upper().startswith("N/A")

    # Assemble only the important fields
    parts: list[str] = [
        f"**Location:**\n{_format_list(loc)}",
    ]

    if _price_is_na(price):
        # When Price is N/A, show Dropped by / Merge the following
        if dropped_by:
            parts.append(f"**Dropped by:**\n{_format_list(dropped_by)}")
        if merge_following:
            parts.append(f"**Merge the following:**\n{_format_list(merge_following)}")
        # Fallback if neither exists
        if not dropped_by and not merge_following:
            parts.append(f"**Price:**\n{_format_list(price)}")
    else:
        parts.append(f"**Price:**\n{_format_list(price)}")

    parts.append(f"**Rarity:**\n{_format_list(rarity)}")

    if note:
        parts.append(f"**Note:**\n{_format_list(note)}")
        log.info("Found note field: %s", note)

    structured = "\n\n".join(parts).strip()
    log.info("Final structured content: %s", structured)
    return structured, price


def extract_item_details(page_url: str) -> dict | None:
    try:
        r = requests.get(
            page_url,
            timeout=8,  # Reduced timeout
            headers={"User-Agent": "aqw-wiki-bot/1.0"},
        )
        r.raise_for_status()
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            log.debug("Page not found: %s", page_url)
        elif e.response.status_code in (503, 429):
            log.debug("Rate limited/blocked for %s: %s", page_url, e.response.status_code)
        else:
            log.warning("HTTP error %s for %s: %s", e.response.status_code, page_url, e)
        return None
    except requests.Timeout:
        log.debug("Timeout fetching %s", page_url)
        return None
    except Exception as e:
        log.warning("Failed to fetch %s: %s", page_url, e)
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    if not page_has_aegift(soup):
        log.debug("No aegift tag found on %s", page_url)
        return None

    title_el = soup.select_one("#page-title")
    if title_el:
        title = title_el.get_text(strip=True)
    else:
        title = soup.title.get_text(strip=True) if soup.title else "Untitled"
        title = title.replace(" - AQW", "").strip()

    if len(title) > MAX_TITLE_LENGTH:
        title = title[: MAX_TITLE_LENGTH - 3] + "..."

    content_el = soup.select_one("#page-content") or soup.select_one("#main-content")
    if not content_el:
        return None

    title_icons = _extract_title_icons(soup)

    # Remove tag UI (page-tags) but KEEP the info blocks because they contain:
    # Location/Price/Rarity/Notes/Drop/merge info used in the final structured output.
    for el in content_el.select(".page-tags"):
        el.decompose()
    for a in content_el.select("a[href*='/system:page-tags/tag/']"):
        a.decompose()
    for el in content_el.select("script, style"):
        el.decompose()

    raw_text = content_el.get_text(separator="\n", strip=True)
    cleaned, price = _clean_item_text(raw_text)

    # Debug: if the page actually has a Location label but our parser failed,
    # log a small snippet so we can tune the regex to the real wording.
    try:
        if "**Locations:**" in (cleaned or "") and "\nN/A" in (cleaned or ""):
            lower = (raw_text or "").lower()
            idx = lower.find("location")
            if idx != -1:
                snippet = raw_text[max(0, idx - 120) : idx + 280]
                log.warning("Location parse failed for %s. Snippet:\n%s", page_url, snippet)
    except Exception:
        # Never break scraping due to debug-only logging.
        pass

    img_url = _extract_imgur_image(content_el)

    if len(cleaned) > MAX_DESC_LENGTH:
        cleaned = cleaned[: MAX_DESC_LENGTH - 3] + "..."

    return {
        "title": title or "Untitled",
        "content": cleaned or "No item info available.",
        "price": price,
        "image": img_url,
        "url": page_url,
        "title_icons": title_icons,
    }


def _extract_recent_changes_entries() -> dict[str, datetime]:
    """
    Get mapping: page_url -> earliest change_time within CHECK_DAYS.
    Only checks the main recent changes page - no pagination.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=CHECK_DAYS)
    page_times: dict[str, datetime] = {}

    log.info("Starting recent changes extraction, cutoff: %s", cutoff)

    try:
        res = requests.get(RECENT_URL_HTTP, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        log.info("Fetching page: %s", RECENT_URL_HTTP)

        any_in_window = False
        rows_found = 0
        for row in soup.select("table tr"):
            cols = row.find_all("td")
            if len(cols) < 3:
                continue

            rows_found += 1
            link = cols[0].find("a")
            if not link:
                continue

            href = link.get("href", "")
            if not href or href.startswith("#"):
                continue

            time_text = cols[2].get_text(strip=True)
            change_time = parse_wiki_time(time_text)
            if not change_time:
                log.debug("Failed to parse time: %s", time_text)
                continue

            if change_time < cutoff:
                log.debug("Skipping old entry: %s (changed %s)", href, change_time)
                continue

            any_in_window = True
            page_url = _make_absolute(href).rstrip("/")
            prev = page_times.get(page_url)
            if prev is None or change_time < prev:
                page_times[page_url] = change_time
                log.debug("Found recent page: %s (changed %s)", page_url, change_time)

        log.info("Main page: %d rows found, %d in window, %d total pages", rows_found, any_in_window, len(page_times))

    except Exception as e:
        log.warning("Failed to fetch recent changes: %s", e)
        return page_times

    log.info("Recent changes extraction complete: %d pages found", len(page_times))
    return page_times


def _extract_related_item_links(page_url: str, max_links: int = 25) -> list[str]:
    """
    From a recent-changes page, extract likely internal item page links.
    Skips system pages and returns absolute URLs.
    """
    try:
        r = requests.get(page_url, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        r.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch page content for links %s: %s", page_url, e)
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    content = soup.select_one("#page-content")
    if not content:
        return []

    links: list[str] = []
    for a in content.select("a[href]"):
        href = a.get("href", "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        if href.startswith(("http://", "https://")):
            full = href
        else:
            full = _make_absolute(href, page_url)

        # Skip system pages and external links
        if any(x in full.lower() for x in ("system:", "forum:", "search:", "nav:", "admin:", "help:")):
            continue

        links.append(full)
        if len(links) >= max_links:
            break

    return list(dict.fromkeys(links))  # dedupe while preserving order


def fetch_recent_aegifts_fast(limit: int = MAX_POSTS_PER_RUN, newest_first: bool = False) -> list[dict]:
    """
    Fast version for slash commands - checks main page only.
    """
    page_times = _extract_recent_changes_entries()  # Check main page only
    if not page_times:
        log.info("No recent changes found")
        return []

    sorted_pages = sorted(page_times.items(), key=lambda kv: kv[1])
    if newest_first:
        sorted_pages = list(reversed(sorted_pages))

    results: list[dict] = []
    seen_ids: set[str] = set()
    pages_checked = 0

    for page_url, _t in sorted_pages:  # Process all pages
        pid = urlparse(page_url).path.strip("/").replace("/", "-") or page_url
        if pid in seen_ids:
            continue

        pages_checked += 1
        log.info("Checking page %d: %s", pages_checked, page_url)

        # Try the page itself first
        details = extract_item_details(page_url)
        if details:
            results.append({"id": pid, **details})
            seen_ids.add(pid)
            log.info("✓ Found aegift: %s", details["title"])
            if len(results) >= limit:
                break

        if len(results) >= limit:
            break

    log.info("Checked %d pages, found %d aegift items", pages_checked, len(results))
    return results


def fetch_recent_aegifts(limit: int = MAX_POSTS_PER_RUN, newest_first: bool = False) -> list[dict]:
    """
    Fetch aegift pages from the main recent changes page only.
    """
    page_times = _extract_recent_changes_entries()  # Check main page only
    if not page_times:
        log.info("No recent changes found")
        return []

    sorted_pages = sorted(page_times.items(), key=lambda kv: kv[1])
    if newest_first:
        sorted_pages = list(reversed(sorted_pages))

    results: list[dict] = []
    seen_ids: set[str] = set()

    for page_url, _t in sorted_pages:
        pid = urlparse(page_url).path.strip("/").replace("/", "-") or page_url
        if pid in seen_ids:
            continue

        log.info("Checking page: %s", page_url)

        # Try the page itself first
        details = extract_item_details(page_url)
        if details:
            results.append({"id": pid, **details})
            seen_ids.add(pid)
            log.info("✓ Found aegift: %s", details["title"])
        else:
            # If not a direct item page, try its child links
            child_links = _extract_related_item_links(page_url, max_links=3)
            log.debug("Found %d child links for %s", len(child_links), page_url)
            for child_url in child_links:
                child_pid = urlparse(child_url).path.strip("/").replace("/", "-") or child_url
                if child_pid in seen_ids:
                    continue
                child_details = extract_item_details(child_url)
                if child_details:
                    results.append({"id": child_pid, **child_details})
                    seen_ids.add(child_pid)
                    log.info("✓ Found aegift child: %s", child_details["title"])
                    if len(results) >= limit:
                        break

        if len(results) >= limit:
            break

    log.info("Checked %d pages, found %d aegift items", len(seen_ids), len(results))
    return results


def create_embed(post: dict) -> discord.Embed:
    wrapped_content = _wrap_lines(post["content"])
    # Remove title_icons to eliminate aegift hyperlink below item name
    desc = f"{wrapped_content}\n\n[View on Wiki]({post['url']})"
    if len(desc) > 4096:
        desc = desc[:4090] + "..."

    embed = discord.Embed(
        title=post["title"],
        description=desc,
        url=post["url"],
        color=0xFF4500,
    )
    if post.get("image"):
        embed.set_image(url=post["image"])
    embed.set_footer(text="AQW Daily Gift")
    return embed


# ---------------- LOOP ----------------
@tasks.loop(seconds=30)
async def check_posts():
    await bot.wait_until_ready()

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        log.warning("Channel %s not found", CHANNEL_ID)
        return

    posts = await asyncio.to_thread(fetch_recent_aegifts, limit=10)  # Check more pages for background
    if not posts:
        return
    
    # Group items by their origin fields
    groups = {}
    for post in posts:
        group_key = make_group_key(post)
        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(post)
    
    # Process each group
    for group_key, group_posts in groups.items():
        if not group_posts:
            continue
            
        # Check if this group was already posted with improved error handling
        existing_message = None
        stored_message_id = None
        stored_channel_id = None
        
        async with aiosqlite.connect(DB) as db:
            # Use immediate lock to prevent race conditions
            await db.execute("BEGIN IMMEDIATE")
            try:
                async with db.execute("""
                    SELECT message_id, channel_id FROM groups 
                    WHERE group_key = ? 
                    ORDER BY last_updated DESC 
                    LIMIT 1
                """, (group_key,)) as cur:
                    result = await cur.fetchone()
                    if result:
                        stored_message_id = result[0]
                        stored_channel_id = result[1]
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        
        # Try to fetch existing message if we have stored details
        if stored_message_id and stored_channel_id:
            try:
                target_channel = bot.get_channel(stored_channel_id)
                if target_channel:
                    existing_message = await target_channel.fetch_message(stored_message_id)
                    log.info("Found existing grouped message for group key %s: %s", group_key[:8], stored_message_id)
            except discord.NotFound:
                log.warning("Stored message %s not found, will create new one", stored_message_id)
                existing_message = None
            except discord.Forbidden:
                log.warning("No permission to fetch message %s", stored_message_id)
                existing_message = None
            except Exception as e:
                log.error("Error fetching existing message %s: %s", stored_message_id, e)
                existing_message = None
        
        if existing_message:
            # Update existing message
            log.info("Updating existing grouped message for group key %s with %d items", group_key[:8], len(group_posts))
            await update_group_message(channel, existing_message, group_posts)
        else:
            # Create new message
            log.info("Creating new grouped message for group key %s with %d items", group_key[:8], len(group_posts))
            await create_new_group_message(channel, group_key, group_posts)


# ---------------- MESSAGE HELPERS ----------------
async def update_group_message(channel: discord.TextChannel, message: discord.Message, posts: list[dict]):
    """Update an existing group message with new items."""
    if not posts:
        return
    
    # Generate collage if multiple items
    collage_bytes = None
    if len(posts) > 1:
        image_urls = [post.get("image") for post in posts if post.get("image")]
        collage_bytes = await asyncio.to_thread(generate_collage, image_urls)
    
    # Build updated embed with categorized items
    titles = [post["title"] for post in posts]
    title = f"{titles[0]} ({len(titles)} Variants Found)" if len(titles) > 1 else titles[0]
    
    # Categorize items and build content by category
    categorized_items = {}
    for post in posts:
        category = categorize_item(post)
        if category not in categorized_items:
            categorized_items[category] = []
        categorized_items[category].append(post)
    
    # Build content with proper category headers
    content_parts = []
    for category, items_in_cat in categorized_items.items():
        category_header = format_category_header(category, len(items_in_cat))
        content_parts.append(category_header)
        
        for i, post in enumerate(items_in_cat):
            content_parts.append(f"• **{post['title']}**")
            if post.get('price') and post.get('price') != 'N/A':
                content_parts.append(f"  💰 {post.get('price')}")
            if post.get('rarity') and post.get('rarity') != 'N/A':
                content_parts.append(f"  ⭐ {post.get('rarity')}")
        
        content_parts.append("")  # Empty line between categories
    
    updated_content = "\n".join(content_parts).strip()
    
    embed = discord.Embed(
        title=title,
        description=updated_content,
        color=0xFF4500,
        url=posts[0]["url"]
    )
    
    # Add collage as attachment if generated
    files = []
    if collage_bytes:
        files.append(discord.File(io.BytesIO(collage_bytes), "collage.png"))
    
    await message.edit(embed=embed, attachments=files)
    
    # Update database
    group_key = make_group_key(posts[0])
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            UPDATE groups SET last_updated = datetime('now') 
            WHERE group_key = ?
        """, (group_key,))
        await db.commit()

async def create_new_group_message(channel: discord.TextChannel, group_key: str, posts: list[dict]):
    """Create a new group message with collage."""
    if not posts:
        return
    
    # Generate collage
    image_urls = [post.get("image") for post in posts if post.get("image")]
    collage_bytes = await asyncio.to_thread(generate_collage, image_urls)
    
    # Build embed with categorized items
    titles = [post["title"] for post in posts]
    title = f"{titles[0]} ({len(titles)} Variants Found)" if len(titles) > 1 else titles[0]
    
    # Categorize items and build content by category
    categorized_items = {}
    for post in posts:
        category = categorize_item(post)
        if category not in categorized_items:
            categorized_items[category] = []
        categorized_items[category].append(post)
    
    # Build content with proper category headers
    content_parts = []
    for category, items_in_cat in categorized_items.items():
        category_header = format_category_header(category, len(items_in_cat))
        content_parts.append(category_header)
        
        for i, post in enumerate(items_in_cat):
            content_parts.append(f"• **{post['title']}**")
            if post.get('price') and post.get('price') != 'N/A':
                content_parts.append(f"  💰 {post.get('price')}")
            if post.get('rarity') and post.get('rarity') != 'N/A':
                content_parts.append(f"  ⭐ {post.get('rarity')}")
        
        content_parts.append("")  # Empty line between categories
    
    updated_content = "\n".join(content_parts).strip()
    
    embed = discord.Embed(
        title=title,
        description=updated_content,
        color=0xFF4500,
        url=posts[0]["url"]
    )
    
    # Add collage as attachment
    files = []
    if collage_bytes:
        files.append(discord.File(io.BytesIO(collage_bytes), "collage.png"))
    
    message = await channel.send(embed=embed, attachments=files)
    
    # Save to database with proper message ID
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            INSERT INTO groups (group_key, message_id, channel_id, last_updated) 
            VALUES (?, ?, ?, datetime('now'))
        """, (group_key, message.id, channel.id))
        await db.commit()
        log.info("Stored new grouped message ID %s for group key %s", message.id, group_key[:8])

# ---------------- COMMAND ----------------
@bot.tree.command(name="latestdrops", description="Check latest AE gift pages")
async def latestdrops(interaction: discord.Interaction):
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        # Interaction token expired / no longer valid (common right after redeploy)
        return

    try:
        # Check the main page only
        posts = await asyncio.wait_for(
            asyncio.to_thread(fetch_recent_aegifts_fast, 1, True),
            timeout=15  # Shorter timeout for single page
        )
        if not posts:
            await interaction.followup.send("No recent AE gifts found in the last 30 pages.")
            return

        await interaction.followup.send(embed=create_embed(posts[0]))
    except asyncio.TimeoutError:
        await interaction.followup.send("Timed out fetching latest drops. Please try again in a few seconds.")
    except Exception as e:
        log.exception("latestdrops failed: %s", e)
        await interaction.followup.send("Something went wrong while fetching recent AE gifts.")





@bot.tree.command(name="checkpage", description="Check if a specific page has the aegift tag")
async def checkpage(interaction: discord.Interaction, page_name: str):
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        return

    try:
        page_url = f"{WIKI_BASE}/{page_name}"
        details = await asyncio.wait_for(
            asyncio.to_thread(extract_item_details, page_url),
            timeout=10
        )
        
        if details:
            await interaction.followup.send(f"✅ Found aegift: {details['title']}", embed=create_embed(details))
        else:
            await interaction.followup.send(f"❌ No aegift tag found on {page_url}")
    except asyncio.TimeoutError:
        await interaction.followup.send("Timed out checking page.")
    except Exception as e:
        log.exception("checkpage failed: %s", e)
        await interaction.followup.send(f"Error checking page: {e}")


@bot.tree.command(name="testaegift", description="Test a known aegift page")
async def testaegift(interaction: discord.Interaction):
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        return

    try:
        # Test with a known aegift page from the listing
        page_url = f"{WIKI_BASE}/alteon-plushie"
        details = await asyncio.wait_for(
            asyncio.to_thread(extract_item_details, page_url),
            timeout=15
        )
        
        if details:
            await interaction.followup.send(f"✅ Found aegift: {details['title']}", embed=create_embed(details))
        else:
            await interaction.followup.send(f"❌ No aegift tag found on {page_url}")
    except asyncio.TimeoutError:
        await interaction.followup.send("Timed out checking page.")
    except Exception as e:
        log.exception("testaegift failed: %s", e)
        await interaction.followup.send(f"Error checking page: {e}")


@bot.tree.command(name="ping", description="Test if bot is responding")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("Pong! Bot is working!")


# ---------------- READY ----------------
@bot.event
async def on_ready():
    log.info("Logged in as %s", bot.user)
    await init_db()
    if not check_posts.is_running():
        check_posts.start()
    
    # Debug: Log all registered commands
    commands = [cmd.name for cmd in bot.tree.get_commands()]
    log.info("Registered commands: %s", ", ".join(commands))
    
    await bot.tree.sync()
    log.info("Commands synced.")
    
    # Force global sync to ensure commands appear
    try:
        synced = await bot.tree.sync()
        log.info("Globally synced %d commands", len(synced))
    except Exception as e:
        log.error("Failed to sync commands globally: %s", e)


if __name__ == "__main__":
    max_retries = 5
    base_delay = 60

    for attempt in range(max_retries):
        try:
            bot.run(TOKEN)
            break
        except discord.HTTPException as e:
            if e.status == 429 and attempt < max_retries - 1:
                delay = base_delay * (2**attempt)
                retry_after = getattr(e, "retry_after", None)
                wait = retry_after if retry_after is not None else delay
                log.warning(
                    "Rate limited (429). Waiting %ds before retry (%d/%d)...",
                    wait,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(wait)
                continue
            raise
