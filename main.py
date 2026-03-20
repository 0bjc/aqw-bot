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
    t = re.sub(r"Thanks to[\s\S]*?(?:\n|$)", "", t, flags=re.IGNORECASE)

    # Cleanup whitespace
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

    # Title
    title_el = soup.select_one("#page-title")
    if title_el:
        title = title_el.get_text(strip=True)
    else:
        title = soup.title.get_text(strip=True) if soup.title else "Untitled"
        title = title.replace(" - AQW", "").strip()

    title = (title[: MAX_TITLE_LENGTH - 3] + "...") if len(title) > MAX_TITLE_LENGTH else title

    # Main content
    content_el = soup.select_one("#page-content") or soup.select_one("#main-content") or soup
    if not content_el:
        return {}

    # Remove tags section so tag icons/links don't pollute description
    for el in content_el.select(".page-tags, .page-info-bottom, .page-info"):
        el.decompose()

    # Remove scripts/styles
    for el in content_el.select("script, style"):
        el.decompose()

    content_text = content_el.get_text(separator="\n", strip=True)
    content_text = re.sub(r"\n{3,}", "\n\n", content_text).strip()

    price, cleaned_text = _extract_price_and_clean_text(content_text)

    # Extract ONLY the actual item image (prefer imgur)
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

        # Prefer i.imgur.com if available
        if "i.imgur.com" in full:
            imgur_image = full
            break
        if not imgur_image:
            imgur_image = full

    # If we still didn't find it, do a light fallback inside page-content
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


def _fetch_recent_changes_urls() -> list[tuple[str, datetime]]:
    """
    Fetch recent-changes and return (page_url, change_time) for changes in last CHECK_DAYS.
    Uses http://... as requested.
    """
    cutoff = datetime.utcnow() - timedelta(days=CHECK_DAYS)

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


def _get_links_from_page(page_url: str) -> set[str]:
    try:
        r = requests.get(page_url, timeout=10, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        content = soup.select_one("#page-content") or soup.select_one("#main-content") or soup
        urls: set[str] = set()

        for a in content.select("a[href]"):
            href = a.get("href", "")
            if not href:
                continue
            if "system:" in href or "forum:" in href or "/tag/" in href:
                continue
            full = _make_absolute(href).rstrip("/")
            if "aqwwiki.wikidot.com" in full and "system:" not in full:
                urls.add(full)

        return urls
    except Exception as e:
        log.debug("Could not fetch links from %s: %s", page_url, e)
        return set()


def fetch_recent_aegifts() -> list[dict]:
    log.info("Fetching aegift tag index...")
    aegift_urls = _fetch_aegift_page_urls()
    if not aegift_urls:
        return []

    log.info("Fetching recent changes (last %d days)...", CHECK_DAYS)
    recent_pages_with_time = _fetch_recent_changes_urls()
    recent_page_urls = [u for (u, _) in recent_pages_with_time]

    # 1) Strict mode: only show aegift pages that are themselves in recent-changes within last 7 days
    direct_urls = sorted([u for u in recent_page_urls if u in aegift_urls])
    if direct_urls:
        urls_to_try = direct_urls
    else:
        # 2) Fallback: for each recently changed page, pull aegift item pages linked from it
        log.info("No direct aegift pages in recent-changes; using linked-item fallback.")
        urls_to_try_set: set[str] = set()
        for page_url in recent_page_urls[:30]:  # limit fetches
            links = _get_links_from_page(page_url)
            urls_to_try_set |= (links & aegift_urls)
        urls_to_try = sorted(urls_to_try_set)

    results: list[dict] = []
    seen_ids: set[str] = set()

    for page_url in urls_to_try[:10]:
        data = extract_page_content(page_url)
        if not data:
            continue

        pid = urlparse(page_url).path.strip("/").replace("/", "-") or page_url
        if pid in seen_ids:
            continue
        seen_ids.add(pid)

        results.append(data)
        log.info("AE Gift: %s", data["title"])

    return results


def create_embed(post: dict) -> discord.Embed:
    wrapped = _wrap_text(post["content"], width=WRAP_WIDTH)

    price = post.get("price", "N/A")
    desc = f"🎁 **New AE Gift**\n\n{wrapped}\n\nPrice: {price}\n\n[View on Wiki]({post['url']})"
    if len(desc) > 4096:
        desc = desc[:4090] + "..."

    embed = discord.Embed(
        title=post["title"],
        description=desc,
        url=post["url"],
        color=0xFF4500,
    )

    # Only show the actual item image (imgur)
    if post.get("image"):
        embed.set_image(url=post["image"])

    embed.set_footer(text="AQW AE Gift Tracker")
    return embed


# ---------------- LOOP ----------------
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        log.warning("Channel %s not found", CHANNEL_ID)
        return

    posts = await asyncio.to_thread(fetch_recent_aegifts)
    for post in posts:
        if await is_posted(post["url"]):
            continue

        try:
            await channel.send(embed=create_embed(post))
            await mark_posted(post["url"])
            log.info("Posted %s", post["title"])
        except discord.DiscordException as e:
            log.error("Failed to post %s: %s", post.get("url"), e)


# ---------------- COMMAND ----------------
@bot.tree.command(name="latestdrops", description="Check latest AE gift pages")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer()

    posts = await asyncio.to_thread(fetch_recent_aegifts)
    if not posts:
        await interaction.followup.send("No AE gifts found in the last 7 days.")
        return

    await interaction.followup.send(embed=create_embed(posts[0]))


# ---------------- READY ----------------
@bot.event
async def on_ready():
    log.info("Logged in as %s", bot.user)
    await init_db()

    if not check_posts.is_running():
        check_posts.start()

    await bot.tree.sync()
    log.info("Commands synced.")


# ---------------- START ----------------
if __name__ == "__main__":
    max_retries = 5
    base_delay = 60  # seconds

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
            else:
                raise
