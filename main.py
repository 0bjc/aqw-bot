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

AQW_TAG_URL = "https://aqwwiki.wikidot.com/system:page-tags/tag/aegift"

DB = "drops.db"

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

# ------------------ AQW WIKI FETCH ------------------
def fetch_aegift_pages() -> list[dict]:
    log.info("Fetching AQW Wiki aegift pages...")

    headers = {
        "User-Agent": "aqw-discord-bot/2.0"
    }

    try:
        res = requests.get(AQW_TAG_URL, headers=headers, timeout=15)
        res.raise_for_status()
    except requests.RequestException as e:
        log.error("Failed to fetch aegift pages: %s", e)
        return []

    soup = BeautifulSoup(res.text, "html.parser")

    posts = []

    # Wikidot list of tagged pages
    for link in soup.select("div.pages-list a"):
        title = link.text.strip()
        href = link.get("href")

        if not href or not title:
            continue

        page_url = f"https://aqwwiki.wikidot.com{href}"
        post_id = href

        posts.append({
            "id": post_id,
            "title": title,
            "body": f"🎁 **AE Gift Page Detected**\n{page_url}",
            "image": None,
        })

    log.info("Found %d aegift pages", len(posts))
    return posts

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

    posts = await asyncio.to_thread(fetch_aegift_pages)

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
    description="Show latest AQW AE Gift pages"
)
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)

    posts = await asyncio.to_thread(fetch_aegift_pages)

    if not posts:
        await interaction.followup.send(
            "No AE Gift pages found."
        )
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
