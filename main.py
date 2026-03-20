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
    t = content_text

    t = re.sub(r"Sellback:\s*[^\n]+", "", t, flags=re.IGNORECASE)

    t = re.sub(
        r"Rarity Description:\s*[\s\S]*?(?=(?:Description:|Notes:|Also see:|Thanks to|$))",
        "",
        t,
        flags=re.IGNORECASE,
    )

    price = "N/A"
    m = re.search(
        r"Price:\s*(?P<val>[\s\S]*?)(?=(?:Sellback:|Rarity Description:|Rarity:|Description:|Notes:|Also see:|Thanks to|$))",
        t,
        flags=re.IGNORECASE,
    )
    if m:
        price = m.group("val").strip()
        t = re.sub(
            r"Price:\s*[\s\S]*?(?=(?:Sellback:|Rarity Description:|Rarity:|Description:|Notes:|Also see:|Thanks to|$))",
            "",
            t,
            flags=re.IGNORECASE,
        )

    t = re.sub(
        r"Description:\s*[\s\S]*?(?=(?:Notes:|Also see:|Thanks to|$))",
        "",
        t,
        flags=re.IGNORECASE,
    )

    t = re.sub(
        r"Also see:\s*[\s\S]*?(?=(?:Thanks to|$))",
        "",
        t,
        flags=re.IGNORECASE,
    )

    t = re.sub(r"Thanks to[\s\S]*?(?:\n|$)", "", t, flags=re.IGNORECASE)

    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return price, t


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

    title = (title[: MAX_TITLE_LENGTH - 3] + "...") if len(title) > MAX_TITLE_LENGTH else title

    content_el = soup.select_one("#page-content") or soup.select_one("#main-content") or soup
    if not content_el:
        return {}

    for el in content_el.select(".page-tags, .page-info-bottom, .page-info"):
        el.decompose()

    for el in content_el.select("script, style"):
        el.decompose()

    content_text = content_el.get_text(separator="\n", strip=True)
    content_text = re.sub(r"\n{3,}", "\n\n", content_text).strip()

    price, cleaned_text = _extract_price_and_clean_text(content_text)

    imgur_image = None
    for img in content_el.select("img[src]"):
        src = img.get("src")
        if not src:
            continue
        s = src.lower()
        if any(x in s for x in ("pixel", "spacer", "icon", "thumb")):
            continue

        full = _make_absolute(src, url)
        if "imgur.com" not in full:
            continue

        if "i.imgur.com" in full:
            imgur_image = full
            break
        if not imgur_image:
            imgur_image = full

    if not imgur_image:
        for img in soup.select("#page-content img[src], .page-content img[src]"):
            src = img.get("src")
            if not src:
                continue
            full = _make_absolute(src, url)
            if "imgur.com" in full:
                imgur_image = full
                break

    return {
        "title": title or "Untitled",
        "content": cleaned_text or "No item info available.",
        "price": price,
        "image": imgur_image,
        "url": url,
    }


def _fetch_aegift_page_urls() -> set[str]:
    try:
        res = requests.get(AEGIFT_TAG_URL, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        res.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch aegift tag page: %s", e)
        return set()

    soup = BeautifulSoup(res.text, "html.parser")
    content = soup.select_one("#page-content") or soup.select_one("#main-content") or soup

    urls: set[str] = set()
    for a in content.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        if "system:" in href or "forum:" in href or "/tag/" in href:
            continue
        if "aqwwiki.wikidot.com" in href or href.startswith("/"):
            full = _make_absolute(href).rstrip("/")
            if "aqwwiki.wikidot.com" in full and "system:" not in full:
                urls.add(full)
    return urls


# ✅ ONLY CHANGE MADE HERE
def _fetch_recent_changes_urls() -> list[tuple[str, datetime]]:
    cutoff = datetime.now() - timedelta(days=CHECK_DAYS)

    res = requests.get(
        f"{RECENT_URL_HTTP}?rev_limit=200",
        timeout=15,
        headers={"User-Agent": "aqw-wiki-bot/1.0"},
    )
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")
    recent: list[tuple[str, datetime]] = []

    for row in soup.select("table tr"):
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        link = cols[0].find("a")
        if not link:
            continue

        href = link.get("href", "")
        if not href or href.startswith("#"):
            continue

        time_text = cols[2].get_text(strip=True)
        change_time = parse_wiki_time(time_text)
        if not change_time or change_time < cutoff:
            continue

        page_url = _make_absolute(href).rstrip("/")
        recent.append((page_url, change_time))

    return recent
