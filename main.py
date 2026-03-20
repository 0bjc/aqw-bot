from __future__ import annotations

import os
import logging
import re
import asyncio
import time
import textwrap
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import aiosqlite
import requests
from bs4 import BeautifulSoup

import discord
from discord.ext import commands, tasks

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1484113318095622315"))

WIKI_BASE = "https://aqwwiki.wikidot.com"
RECENT_URL_HTTP = "http://aqwwiki.wikidot.com/system:recent-changes"
AEGIFT_TAG_URL = f"{WIKI_BASE}/system:page-tags/tag/aegift"

DB = "drops.db"
CHECK_DAYS = 7

MAX_DESC_LENGTH = 3800  # keep under 4096
MAX_TITLE_LENGTH = 256
WRAP_WIDTH = 55

# ---------------- DISCORD ----------------
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)


# ---------------- DATABASE ----------------
async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS posted (id TEXT PRIMARY KEY)")
        await db.commit()


async def is_posted(pid: str) -> bool:
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT 1 FROM posted WHERE id=?", (pid,)) as cur:
            return await cur.fetchone() is not None


async def mark_posted(pid: str):
    async with aiosqlite.connect(DB) as db:
        await db.execute("INSERT OR IGNORE INTO posted VALUES (?)", (pid,))
        await db.commit()


# ---------------- HELPERS ----------------
def parse_wiki_time(text: str) -> datetime | None:
    """
    Wikidot recent-changes shows like: '20 Mar 2026 00:59'
    Robustly parse even with weird whitespace.
    """
    if not text:
        return None
    t = text.replace("\xa0", " ").strip()

    m = re.search(
        r"(?P<day>\d{1,2})\s+(?P<mon>[A-Za-z]{3})\s+(?P<year>\d{4})\s+(?P<h>\d{2}):(?P<m>\d{2})",
        t,
    )
    if not m:
        return None

    day = int(m.group("day"))
    mon = m.group("mon")
    year = int(m.group("year"))
    hour = int(m.group("h"))
    minute = int(m.group("m"))

    mon_norm = mon[:1].upper() + mon[1:3].lower()
    try:
        return datetime(year, datetime.strptime(mon_norm, "%b").month, day, hour, minute)
    except ValueError:
        return None


def page_has_aegift(soup: BeautifulSoup) -> bool:
    for tag_el in soup.select(".page-tags a, a[href*='tag/aegift']"):
        if tag_el.get_text(strip=True).lower() == "aegift":
            return True
        href = tag_el.get("href", "")
        if "aegift" in href.lower():
            return True
    return False


def _make_absolute(url: str, base: str | None = None) -> str:
    if not url or url.startswith(("http://", "https://")):
        return url or ""
    base = WIKI_BASE if not base or url.startswith("/") else base
    return urljoin(base, url)


def _wrap_text(text: str, width: int = WRAP_WIDTH) -> str:
    # Wrap paragraphs separately to keep formatting readable
    paras = text.split("\n\n")
    wrapped_paras: list[str] = []
    for p in paras:
        p = p.strip()
        if not p:
            continue
        wrapped = textwrap.wrap(p, width=width, replace_whitespace=False)
        wrapped_paras.append("\n".join(wrapped))
    return "\n\n".join(wrapped_paras).strip()


def _extract_price_and_clean_text(content_text: str) -> tuple[str, str]:
    """
    Returns (price, cleaned_text).
    - Removes Sellback, Description, Also see, Thanks to.
    - Extracts Price: ... and puts it separately (so we can show it on its own line).
    """
    t = content_text

    # Remove Sellback line(s)
    t = re.sub(r"Sellback:\s*[^\n]+", "", t, flags=re.IGNORECASE)

    # Remove Rarity Description block/lines
    t = re.sub(
        r"Rarity Description:\s*[\s\S]*?(?=(?:Description:|Notes:|Also see:|Thanks to|$))",
        "",
        t,
        flags=re.IGNORECASE,
    )

    # Extract price (up to next known label)
    price = "N/A"
    m = re.search(
        r"Price:\s*(?P<val>[\s\S]*?)(?=(?:Sellback:|Rarity Description:|Rarity:|Description:|Notes:|Also see:|Thanks to|$))",
        t,
        flags=re.IGNORECASE,
    )
    if m:
        price = m.group("val").strip()
        # Remove price from text
        t = re.sub(
            r"Price:\s*[\s\S]*?(?=(?:Sellback:|Rarity Description:|Rarity:|Description:|Notes:|Also see:|Thanks to|$))",
            "",
            t,
            flags=re.IGNORECASE,
        )

    # Remove Description: ... block
    t = re.sub(
        r"Description:\s*[\s\S]*?(?=(?:Notes:|Also see:|Thanks to|$))",
        "",
        t,
        flags=re.IGNORECASE,
    )

    # Remove Also see: block/list
    t = re.sub(
        r"Also see:\s*[\s\S]*?(?=(?:Thanks to|$))",
        "",
        t,
        flags=re.IGNORECASE,
    )

    # Remove Thanks to line
    t = re
