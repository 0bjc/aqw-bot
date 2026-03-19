from __future__ import annotations

import os
import html
import logging
import textwrap
from urllib.parse import urlparse

import discord
from discord.ext import commands, tasks
import requests
import aiosqlite
import asyncio

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1484113318095622315"))
REDDIT_USER = os.getenv("REDDIT_USER", "DefNotDatenshi")

DB = "drops.db"
KEYWORDS = ["daily", "gift", "drop", "drops"]
MAX_LINE_LENGTH = 80

# Image host domains that Reddit uses for direct image links
IMAGE_DOMAINS = ("i.redd.it", "i.imgur.com", "imgur.com")

# Enable message_content if you need to read messages
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
        async with db.execute("SELECT 1 FROM posted WHERE id = ?", (post_id,)) as cursor:
            return await cursor.fetchone() is not None


async def mark_posted(post_id: str) -> None:
    async with aiosqlite.connect(DB) as db:
        await db.execute("INSERT OR IGNORE INTO posted (id) VALUES (?)", (post_id,))
        await db.commit()


# ------------------ REDDIT IMAGE EXTRACTION ------------------
def _fix_reddit_image_url(url: str) -> str:
    """
    Reddit API returns URLs with HTML-escaped ampersands (&amp;).
    Discord embeds fail to load these - must unescape to &.
    """
    if not url:
        return ""
    return html.unescape(url)


def _extract_image_url(post_data: dict) -> str | None:
    """
    Extract the best available image URL from a Reddit post.
    Handles: direct image links (i.redd.it), preview thumbnails, gallery posts.
    """
    url = post_data.get("url") or ""

    # 1. Direct image URL (e.g. i.redd.it, imgur)
    parsed = urlparse(url)
    if parsed.netloc in IMAGE_DOMAINS:
        return _fix_reddit_image_url(url)

    # 2. Preview image (text posts with embedded images)
    try:
        preview = post_data.get("preview", {})
        images = preview.get("images", [])
        if images:
            first = images[0]
            # Prefer full-size source over resolutions
            source = first.get("source")
            if source and source.get("url"):
                return _fix_reddit_image_url(source["url"])
            resolutions = first.get("resolutions", [])
            if resolutions:
                img_url = resolutions[-1].get("url")  # highest res
                if img_url:
                    return _fix_reddit_image_url(img_url)
    except (KeyError, IndexError, TypeError):
        pass

    # 3. Thumbnail (low quality fallback for link posts)
    thumb = post_data.get("thumbnail")
    if thumb and thumb not in ("self", "default", "nsfw", "spoiler", ""):
        return _fix_reddit_image_url(thumb)

    return None


# ------------------ PARAPHRASER ------------------
def paraphrase_text(text: str) -> str:
    if not text or not text.strip():
        return "No details provided."

    text = text.replace("&", "and")

    wrapped_lines = []
    for paragraph in text.splitlines():
        paragraph = paragraph.strip()
        if paragraph:
            wrapped_lines.extend(textwrap.wrap(paragraph, width=MAX_LINE_LENGTH))
            wrapped_lines.append("")

    return "\n".join(wrapped_lines).strip()


# ------------------ REDDIT FETCH ------------------
def fetch_reddit_user_posts() -> list[dict]:
    url = f"https://www.reddit.com/user/{REDDIT_USER}/submitted.json?limit=20"
    headers = {"User-Agent": "aqw-discord-bot/1.0"}

    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()
    except requests.RequestException as e:
        log.error("Reddit request failed: %s", e)
        return []
    except ValueError as e:
        log.error("Invalid Reddit JSON response: %s", e)
        return []

    posts = []
    for post in data.get("data", {}).get("children", []):
        d = post.get("data", {})
        full_text = (d.get("title", "") + "\n" + d.get("selftext", "")).lower()

        if not any(k in full_text for k in KEYWORDS):
            continue

        image = _extract_image_url(d)
        body_text = d.get("selftext", "")
        paraphrased_body = paraphrase_text(body_text)

        posts.append({
            "id": d.get("id"),
            "title": d.get("title", "Untitled"),
            "image": image,
            "body": paraphrased_body,
        })

    return posts


# ------------------ EMBED ------------------
def create_embed(post: dict) -> discord.Embed:
    embed = discord.Embed(
        title=post["title"],
        description=post["body"],
        color=0xFF4500,
    )
    if post.get("image"):
        embed.set_image(url=post["image"])
    embed.set_footer(text="AQW Tracker")
    return embed


# ------------------ LOOP ------------------
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        log.warning("Channel %s not found", CHANNEL_ID)
        return

    posts = await asyncio.to_thread(fetch_reddit_user_posts)
    for post in posts:
        if await is_posted(post["id"]):
            continue
        try:
            embed = create_embed(post)
            await channel.send(embed=embed)
            await mark_posted(post["id"])
            log.info("Posted: %s", post["title"][:50])
        except discord.DiscordException as e:
            log.error("Failed to post %s: %s", post["id"], e)


# ------------------ SLASH COMMAND ------------------
@bot.tree.command(name="latestdrops", description="Check latest AQW daily gifts/drops")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    posts = await asyncio.to_thread(fetch_reddit_user_posts)

    if not posts:
        await interaction.followup.send("No relevant daily gifts/drops found.")
        return

    embed = create_embed(posts[0])
    await interaction.followup.send(embed=embed)


# ------------------ READY ------------------
@bot.event
async def on_ready():
    log.info("Logged in as %s", bot.user)
    await init_db()
    check_posts.start()
    await bot.tree.sync()
    log.info("Commands synced.")


if __name__ == "__main__":
    bot.run(TOKEN)
