from __future__ import annotations

import os
import logging
import re
import asyncio
import time
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

    content_el = soup.select_one("#page-content")
    if not content_el:
        content_el = soup.select_one("#main-content")
    if not content_el:
        content_el = soup.select_one(".page-content, .yui-content")

    content_text = ""
    images = []

    if content_el:
        for script in content_el.select("script, style"):
            script.decompose()
        content_text = content_el.get_text(separator="\n", strip=True)
        content_text = re.sub(r"\n{3,}", "\n\n", content_text)

        for img in content_el.select("img[src]"):
            src = img.get("src")
            if not src or "pixel" in src.lower() or "spacer" in src.lower():
                continue
            full_url = _make_absolute(src, url)
            if full_url and full_url not in images:
                images.append(full_url)

    if not images:
        for img in soup.select("#page-content img[src], .page-content img[src]"):
            src = img.get("src")
            if src:
                full_url = _make_absolute(src, url)
                if full_url:
                    images.append(full_url)
                    break

    if len(content_text) > MAX_DESC_LENGTH:
        content_text = content_text[: MAX_DESC_LENGTH - 3] + "..."

    return {
        "title": title or "Untitled",
        "content": content_text or "No description available.",
        "images": images,
        "url": url,
    }


def _fetch_aegift_page_urls() -> set[str]:
    try:
        res = requests.get(
            AEGIFT_TAG_URL,
            timeout=15,
            headers={"User-Agent": "aqw-wiki-bot/1.0"},
        )
        res.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch aegift tag page: %s", e)
        return set()

    soup = BeautifulSoup(res.text, "html.parser")
    urls = set()
    content = soup.select_one("#page-content") or soup.select_one("#main-content") or soup
    for a in content.select("a[href]"):
        href = a.get("href", "")
        if not href or "system:" in href or "forum:" in href or "/tag/" in href:
            continue
        if "aqwwiki.wikidot.com" in href or (href.startswith("/") and not href.startswith("//")):
            full = _make_absolute(href)
            if full and "aqwwiki.wikidot.com" in full and "system:" not in full and "/tag/" not in full:
                urls.add(full.rstrip("/"))

    log.info("Found %d aegift page URLs from tag index", len(urls))
    return urls


def fetch_recent_aegifts() -> list[dict]:
    log.info("Fetching aegift tag page...")
    aegift_urls = _fetch_aegift_page_urls()
    if not aegift_urls:
        return []

    recent_urls = set()
    try:
        res = requests.get(
            f"{RECENT_URL}?rev_limit=200",
            timeout=15,
            headers={"User-Agent": "aqw-wiki-bot/1.0"},
        )
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        cutoff = datetime.utcnow() - timedelta(days=CHECK_DAYS)

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
            recent_urls.add(page_url)
    except Exception as e:
        log.warning("Failed to fetch recent changes: %s", e)

    priority_urls = aegift_urls & recent_urls
    fallback_urls = aegift_urls - recent_urls
    urls_to_try = list(priority_urls) + list(fallback_urls)[:15]
    log.info("Priority (recent+aegift): %d, fallback: %d", len(priority_urls), min(15, len(fallback_urls)))

    results = []
    seen_ids = set()
    for page_url in urls_to_try:
        data = extract_page_content(page_url)
        if not data:
            continue
        path = urlparse(page_url).path
        page_id = path.strip("/").replace("/", "-") or page_url
        if page_id in seen_ids:
            continue
        seen_ids.add(page_id)
        data["id"] = page_id
        results.append(data)
        log.info("  aegift: %s", data["title"])
        if len(results) >= 10:
            break

    log.info("Found %d aegift pages", len(results))
    return results


def create_embed(post: dict) -> discord.Embed:
    desc = f"🎁 **New AE Gift**\n\n{post['content']}\n\n[View on Wiki]({post['url']})"
    if len(desc) > 4096:
        desc = desc[:4090] + "..."

    embed = discord.Embed(
        title=post["title"],
        description=desc,
        url=post["url"],
        color=0xFF4500,
    )
    if post.get("images"):
        embed.set_image(url=post["images"][0])
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
        if await is_posted(post["id"]):
            continue

        try:
            await channel.send(embed=create_embed(post))
            await mark_posted(post["id"])
            log.info("Posted %s", post["title"])
        except discord.DiscordException as e:
            log.error("Failed to post %s: %s", post["id"], e)


# ---------------- COMMAND ----------------
@bot.tree.command(name="latestdrops", description="Check latest AE gift pages")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer()

    posts = await asyncio.to_thread(fetch_recent_aegifts)

    if not posts:
        await interaction.followup.send("No recent AE gifts found in the last 7 days.")
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
                time.sleep(w)
            else:
                raise
