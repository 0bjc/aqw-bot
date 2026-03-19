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
REDDIT_SUBREDDIT = os.getenv("REDDIT_SUBREDDIT", "AQW")

DB = "drops.db"
KEYWORDS = ["daily", "gift", "drop", "drops", "wheel", "quest"]
SHOW_LATEST_IF_NO_MATCH = True
MAX_LINE_LENGTH = 80

IMAGE_DOMAINS = ("i.redd.it", "i.imgur.com", "imgur.com")

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
    if not url:
        return ""
    return html.unescape(url)


def _extract_image_url(post_data: dict) -> str | None:
    url = post_data.get("url") or ""
    parsed = urlparse(url)
    if parsed.netloc in IMAGE_DOMAINS:
        return _fix_reddit_image_url(url)

    try:
        preview = post_data.get("preview", {})
        images = preview.get("images", [])
        if images:
            first = images[0]
            source = first.get("source")
            if source and source.get("url"):
                return _fix_reddit_image_url(source["url"])
            resolutions = first.get("resolutions", [])
            if resolutions:
                img_url = resolutions[-1].get("url")
                if img_url:
                    return _fix_reddit_image_url(img_url)
    except (KeyError, IndexError, TypeError):
        pass

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
REDDIT_BASE_URLS = [
    "https://www.reddit.com",
    "https://old.reddit.com",
]


def fetch_reddit_user_posts() -> list[dict]:
    headers = {
        "User-Agent": "aqw-discord-bot/1.0 (by /u/DefNotDatenshi)",
        "Accept": "application/json",
    }
    last_error = None
    children = []

    for base_url in REDDIT_BASE_URLS:
        url = f"{base_url}/user/{REDDIT_USER}/submitted.json?limit=25"
        try:
            res = requests.get(url, headers=headers, timeout=15)
            log.info("Reddit %s -> status %s", base_url, res.status_code)

            if res.status_code == 429:
                log.warning("Reddit rate limited (429), trying next URL...")
                last_error = "Rate limited"
                continue

            if res.status_code != 200:
                log.warning("Reddit returned %s: %s", res.status_code, res.text[:200])
                last_error = f"HTTP {res.status_code}"
                continue

            data = res.json()
            children = data.get("data", {}).get("children", [])
            break
        except requests.Timeout:
            log.warning("Reddit timeout for %s", base_url)
            last_error = "Timeout"
            continue
        except requests.RequestException as e:
            log.warning("Reddit request failed for %s: %s", base_url, e)
            last_error = str(e)
            continue
        except ValueError as e:
            log.error("Invalid Reddit JSON: %s", e)
            return []
    else:
        log.warning("User endpoint failed, trying r/%s...", REDDIT_SUBREDDIT)
        for base_url in REDDIT_BASE_URLS:
            url = f"{base_url}/r/{REDDIT_SUBREDDIT}/new.json?limit=25"
            try:
                res = requests.get(url, headers=headers, timeout=15)
                if res.status_code == 200:
                    data = res.json()
                    raw = data.get("data", {}).get("children", [])
                    children = [c for c in raw if c.get("data", {}).get("author", "").lower() == REDDIT_USER.lower()]
                    log.info("r/%s: %d posts by %s", REDDIT_SUBREDDIT, len(children), REDDIT_USER)
                    break
            except Exception:
                pass
        if not children:
            log.error("All Reddit sources failed. Last error: %s", last_error)
            return []

    log.info("Reddit returned %d posts (before keyword filter)", len(children))

    posts = []
    fallback_post = None

    for post in children:
        d = post.get("data", {})
        full_text = (d.get("title", "") + "\n" + d.get("selftext", "")).lower()
        image = _extract_image_url(d)
        body_text = d.get("selftext", "")
        paraphrased_body = paraphrase_text(body_text)
        parsed = {
            "id": d.get("id"),
            "title": d.get("title", "Untitled"),
            "image": image,
            "body": paraphrased_body,
        }

        if not fallback_post:
            fallback_post = parsed

        if not any(k in full_text for k in KEYWORDS):
            continue

        posts.append(parsed)

    if not posts and fallback_post and SHOW_LATEST_IF_NO_MATCH:
        log.info("No keyword match; showing latest post as fallback")
        posts = [fallback_post]

    log.info("After keyword filter: %d posts", len(posts))
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
    import sys
    if "--test-reddit" in sys.argv:
        log.info("Testing Reddit fetch for user: %s", REDDIT_USER)
        posts = fetch_reddit_user_posts()
        log.info("Got %d posts", len(posts))
        for i, p in enumerate(posts[:3]):
            log.info("  [%d] %s (id=%s)", i + 1, p["title"][:60], p["id"])
        if not posts:
            log.warning("No posts - check logs above.")
        sys.exit(0)

    bot.run(TOKEN)
