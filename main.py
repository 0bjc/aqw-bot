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
        return datetime(
            year,
            datetime.strptime(mon_norm, "%b").month,
            day,
            hour,
            minute,
        )
    except ValueError:
        return None


def _make_absolute(url: str, base: str | None = None) -> str:
    if not url or url.startswith(("http://", "https://")):
        return url or ""
    base = WIKI_BASE if not base or url.startswith("/") else base
    return urljoin(base, url)


# ---------------- RECENT CHANGES (FIXED) ----------------
def _fetch_recent_changes_urls() -> list[tuple[str, datetime]]:
    """
    Fetch recent-changes and return (page_url, change_time)
    for changes in last CHECK_DAYS.
    """

    # ✅ FIX 1 — match wikidot local timestamps
    cutoff = datetime.now() - timedelta(days=CHECK_DAYS)

    try:
        res = requests.get(
            f"{RECENT_URL_HTTP}?rev_limit=200",
            timeout=15,
            headers={
                # ✅ FIX 2 — browser headers (wikidot behaves better)
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        res.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch recent changes: %s", e)
        return []

    soup = BeautifulSoup(res.text, "html.parser")

    # ✅ FIX 3 — correct table selector
    table = soup.select_one("table.recent-changes-table")
    if not table:
        log.warning("Recent changes table not found.")
        return []

    recent: list[tuple[str, datetime]] = []

    for row in table.select("tr"):
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

        if not change_time:
            continue

        if change_time < cutoff:
            continue

        page_url = _make_absolute(href).rstrip("/")
        recent.append((page_url, change_time))

    log.info("Recent entries found: %d", len(recent))
    return recent


# ---------------- AE GIFT FETCH ----------------
def fetch_recent_aegifts() -> list[dict]:
    log.info("Fetching recent changes (last %d days)...", CHECK_DAYS)

    recent_pages_with_time = _fetch_recent_changes_urls()
    results = []

    for page_url, _ in recent_pages_with_time[:10]:
        results.append(
            {
                "title": urlparse(page_url).path.strip("/"),
                "content": "AE Gift page detected.",
                "price": "N/A",
                "image": None,
                "url": page_url,
            }
        )

    return results


# ---------------- EMBED ----------------
def create_embed(post: dict) -> discord.Embed:
    desc = (
        f"🎁 **New AE Gift**\n\n"
        f"{post['content']}\n\n"
        f"Price: {post['price']}\n\n"
        f"[View on Wiki]({post['url']})"
    )

    embed = discord.Embed(
        title=post["title"],
        description=desc,
        url=post["url"],
        color=0xFF4500,
    )

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
            else:
                raise
