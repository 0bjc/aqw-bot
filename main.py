from __future__ import annotations

import os
import logging
import re
import asyncio
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
RECENT_URL = f"{WIKI_BASE}/system:recent-changes"
AEGIFT_TAG_URL = f"{WIKI_BASE}/system:page-tags/tag/aegift"

DB = "drops.db"
CHECK_DAYS = 7
MAX_DESC_LENGTH = 3800
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
        await db.execute(
            "CREATE TABLE IF NOT EXISTS posted (id TEXT PRIMARY KEY)"
        )
        await db.commit()


async def is_posted(pid: str) -> bool:
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT 1 FROM posted WHERE id=?",
            (pid,),
        ) as cur:
            return await cur.fetchone() is not None


async def mark_posted(pid: str):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT OR IGNORE INTO posted VALUES (?)",
            (pid,),
        )
        await db.commit()


# ---------------- HELPERS ----------------
def parse_wiki_time(text: str) -> datetime | None:
    text = text.strip()
    for fmt in ("%d %b %Y %H:%M", "%d %b %Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
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


# ---------------- PAGE EXTRACTION ----------------
def extract_page_content(url: str) -> dict:
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        r.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch %s: %s", url, e)
        return {}

    soup = BeautifulSoup(r.text, "html.parser")

    if not page_has_aegift(soup):
        return {}

    title_el = soup.select_one("#page-title")
    if title_el:
        title = title_el.get_text(strip=True)
    else:
        title = soup.title.get_text(strip=True) if soup.title else "Untitled"
        title = title.replace(" - AQW", "").strip()

    title = (
        title[: MAX_TITLE_LENGTH - 3] + "..."
        if len(title) > MAX_TITLE_LENGTH
        else title
    )

    content_el = (
        soup.select_one("#page-content")
        or soup.select_one("#main-content")
        or soup.select_one(".page-content, .yui-content")
    )

    content_text = ""
    images = []

    if content_el:
        for el in content_el.select(".page-tags, .page-info-bottom"):
            el.decompose()

        for el in content_el.find_all(
            "a", href=re.compile(r"system:page-tags/tag")
        ):
            el.decompose()

        for script in content_el.select("script, style"):
            script.decompose()

        content_text = content_el.get_text(separator="\n", strip=True)
        content_text = re.sub(r"\n{3,}", "\n\n", content_text)

        # ---------- CLEAN CONTENT (CHANGE #2 ONLY) ----------
        content_text = re.sub(
            r"(Price:)\s*",
            r"\n\1 ",
            content_text,
            flags=re.IGNORECASE,
        )

        content_text = re.sub(
            r"Sellback:\s*[^\n]+", "", content_text, flags=re.IGNORECASE
        )

        content_text = re.sub(
            r"Rarity Description:\s*[^\n]+(?:\n(?![A-Z][a-z]+:)[^\n]*)*",
            "",
            content_text,
            flags=re.IGNORECASE,
        )

        content_text = re.sub(
            r"(?<!\w)Description:\s*[^\n]+(?:\n(?![A-Z][a-z]+:)[^\n]*)*",
            "",
            content_text,
            flags=re.IGNORECASE,
        )

        # remove Thanks to
        content_text = re.sub(
            r"Thanks to:\s*[^\n]+",
            "",
            content_text,
            flags=re.IGNORECASE,
        )

        content_text = re.sub(
            r"Also see:[\s\S]*", "", content_text, flags=re.IGNORECASE
        )

        content_text = re.sub(r"\n{3,}", "\n\n", content_text).strip()

        imgur_urls = []
        other_urls = []

        for img in content_el.select("img[src]"):
            src = img.get("src")
            if not src or any(
                x in src.lower() for x in ("pixel", "spacer", "icon", "thumb")
            ):
                continue

            full_url = _make_absolute(src, url)

            if "imgur.com" in full_url:
                if full_url not in imgur_urls:
                    imgur_urls.append(full_url)
            else:
                if full_url not in other_urls:
                    other_urls.append(full_url)

        images = imgur_urls if imgur_urls else other_urls

    if len(content_text) > MAX_DESC_LENGTH:
        content_text = content_text[: MAX_DESC_LENGTH - 3] + "..."

    return {
        "title": title or "Untitled",
        "content": content_text or "No description available.",
        "images": images,
        "url": url,
    }
