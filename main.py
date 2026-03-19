from __future__ import annotations

import os
import logging
import asyncio
from datetime import datetime, timedelta

import aiosqlite
import requests
from bs4 import BeautifulSoup

import discord
from discord.ext import commands, tasks

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1484113318095622315"))

RECENT_URL = "https://aqwwiki.wikidot.com/system:recent-changes"

DB = "drops.db"

CHECK_DAYS = 7  # only scan last 7 days

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


async def is_posted(pid: str):
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
    """
    Example:
    17 Feb 2026 07:53
    """
    try:
        return datetime.strptime(text.strip(), "%d %b %Y %H:%M")
    except Exception:
        return None


def page_has_aegift(url: str) -> bool:
    """Check if page contains aegift tag"""
    try:
        r = requests.get(url, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        tags = soup.select(".page-tags a")
        for tag in tags:
            if tag.text.strip().lower() == "aegift":
                return True
    except Exception:
        pass

    return False


# ---------------- FETCH RECENT AEGIFTS ----------------
def fetch_recent_aegifts() -> list[dict]:
    log.info("Scanning recent changes...")

    try:
        res = requests.get(RECENT_URL, timeout=15)
        res.raise_for_status()
    except Exception as e:
        log.error("Failed fetching recent changes: %s", e)
        return []

    soup = BeautifulSoup(res.text, "html.parser")

    rows = soup.select("table tr")

    cutoff = datetime.utcnow() - timedelta(days=CHECK_DAYS)

    results = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        link = cols[0].find("a")
        if not link:
            continue

        title = link.text.strip()
        href = link.get("href")

        time_text = cols[2].text.strip()
        change_time = parse_wiki_time(time_text)

        if not change_time:
            continue

        if change_time < cutoff:
            continue  # older than 7 days

        page_url = f"https://aqwwiki.wikidot.com{href}"

        # check tag
        if not page_has_aegift(page_url):
            continue

        results.append({
            "id": href,
            "title": title,
            "url": page_url,
        })

    log.info("Found %d aegift pages in last 7 days", len(results))
    return results


# ---------------- EMBED ----------------
def create_embed(post):
    embed = discord.Embed(
        title=post["title"],
        description=f"🎁 **New AE Gift Detected**\n{post['url']}",
        color=0xFF4500,
    )
    embed.set_footer(text="AQW AE Gift Tracker")
    return embed


# ---------------- LOOP ----------------
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return

    posts = await asyncio.to_thread(fetch_recent_aegifts)

    for post in posts:
        if await is_posted(post["id"]):
            continue

        await channel.send(embed=create_embed(post))
        await mark_posted(post["id"])

        log.info("Posted %s", post["title"])


# ---------------- COMMAND ----------------
@bot.tree.command(name="latestdrops")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer()

    posts = await asyncio.to_thread(fetch_recent_aegifts)

    if not posts:
        await interaction.followup.send("No recent AE gifts found.")
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


# ---------------- START ----------------
if __name__ == "__main__":
    bot.run(TOKEN)
