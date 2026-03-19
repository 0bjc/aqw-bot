from __future__ import annotations

import os
import logging
import asyncio
import aiosqlite
import requests
from bs4 import BeautifulSoup

import discord
from discord.ext import commands, tasks

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1484113318095622315"))

# AQW Wiki tag page (aegift)
AQW_TAG_URL = "https://aqwwiki.wikidot.com/system:page-tags/tag/aegift"

DB = "drops.db"

# ------------------ DISCORD SETUP ------------------
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

# ------------------ DATABASE ------------------
async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS posted (
                id TEXT PRIMARY KEY
            )
        """)
        await db.commit()


async def is_posted(post_id: str) -> bool:
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT 1 FROM posted WHERE id = ?", (post_id,)
        ) as cursor:
            return await cursor.fetchone() is not None


async def mark_posted(post_id: str):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT OR IGNORE INTO posted (id) VALUES (?)",
            (post_id,),
        )
        await db.commit()

# ------------------ FETCH LATEST AEGIFT ------------------
def fetch_latest_aegift() -> list[dict]:
    log.info("Fetching latest AQW Wiki aegift page...")

    headers = {
        "User-Agent": "aqw-discord-bot/2.1"
    }

    try:
        res = requests.get(AQW_TAG_URL, headers=headers, timeout=15)
        res.raise_for_status()
    except requests.RequestException as e:
        log.error("Failed to fetch aegift pages: %s", e)
        return []

    soup = BeautifulSoup(res.text, "html.parser")

    # Get newest page only
    link = soup.select_one("div.pages-list a")

    if not link:
        log.warning("No aegift pages found.")
        return []

    title = link.text.strip()
    href = link.get("href")

    if not href:
        return []

    page_url = f"https://aqwwiki.wikidot.com{href}"
    post_id = href

    post = {
        "id": post_id,
        "title": title,
        "body": f"🎁 **Latest AE Gift Detected**\n{page_url}",
        "image": None,
    }

    log.info("Latest aegift page: %s", title)

    return [post]

# ------------------ EMBED ------------------
def create_embed(post: dict) -> discord.Embed:
    embed = discord.Embed(
        title=post["title"],
        description=post["body"],
        color=0xFF4500,
    )
    embed.set_footer(text="AQW AE Gift Tracker")
    return embed

# ------------------ LOOP ------------------
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        log.warning("Channel %s not found", CHANNEL_ID)
        return

    posts = await asyncio.to_thread(fetch_latest_aegift)

    for post in posts:
        if await is_posted(post["id"]):
            continue

        try:
            embed = create_embed(post)
            await channel.send(embed=embed)
            await mark_posted(post["id"])
            log.info("Posted: %s", post["title"])
        except discord.DiscordException as e:
            log.error("Failed posting %s: %s", post["id"], e)

# ------------------ SLASH COMMAND ------------------
@bot.tree.command(
    name="latestdrops",
    description="Show latest AQW AE Gift page"
)
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)

    posts = await asyncio.to_thread(fetch_latest_aegift)

    if not posts:
        await interaction.followup.send("No AE Gift page found.")
        return

    embed = create_embed(posts[0])
    await interaction.followup.send(embed=embed)

# ------------------ READY ------------------
@bot.event
async def on_ready():
    log.info("Logged in as %s", bot.user)

    await init_db()

    if not check_posts.is_running():
        check_posts.start()

    await bot.tree.sync()
    log.info("Commands synced.")

# ------------------ START ------------------
if __name__ == "__main__":
    bot.run(TOKEN)
