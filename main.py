import os
import discord
from discord.ext import commands, tasks
import requests
import aiosqlite
import asyncio
import textwrap

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = 1484113318095622315
REDDIT_USER = "DefNotDatenshi"

DB = "drops.db"
KEYWORDS = ["daily", "gift", "drop", "drops"]
MAX_LINE_LENGTH = 80

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())

# ------------------ DATABASE ------------------
async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS posted (
                id TEXT PRIMARY KEY
            )
        """)
        await db.commit()

async def is_posted(post_id):
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT 1 FROM posted WHERE id = ?", (post_id,)) as cursor:
            return await cursor.fetchone() is not None

async def mark_posted(post_id):
    async with aiosqlite.connect(DB) as db:
        await db.execute("INSERT INTO posted (id) VALUES (?)", (post_id,))
        await db.commit()

# ------------------ PARAPHRASER ------------------
def paraphrase_text(text: str) -> str:
    if not text.strip():
        return "No details provided."

    text = text.replace("&", "and").replace("amp;", "")

    wrapped_lines = []
    for paragraph in text.splitlines():
        paragraph = paragraph.strip()
        if paragraph:
            wrapped_lines.extend(textwrap.wrap(paragraph, width=MAX_LINE_LENGTH))
            wrapped_lines.append("")
    return "\n".join(wrapped_lines).strip()

# ------------------ REDDIT FETCH ------------------
def fetch_reddit_user_posts():
    url = f"https://www.reddit.com/user/{REDDIT_USER}/submitted.json?limit=20"

    headers = {
        "User-Agent": "python:aqw.tracker:v1.0 (by /u/example)"
    }

    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()
    except:
        return []

    posts = []

    for post in data.get("data", {}).get("children", []):
        try:
            d = post["data"]

            title = d.get("title", "")
            selftext = d.get("selftext", "")
            full_text = (title + " " + selftext).lower()

            # ORIGINAL FILTER
            if not any(k in full_text for k in KEYWORDS):
                continue

            # ------------------ IMAGE ------------------
            image = None

            if "preview" in d:
                images = d["preview"].get("images")
                if images:
                    image = images[0]["source"]["url"]

            if not image:
                url_field = d.get("url", "")
                if url_field.endswith((".jpg", ".jpeg", ".png", ".gif")):
                    image = url_field

            if image:
                image = image.replace("&amp;", "&")

            # ------------------ TEXT ------------------
            body = paraphrase_text(selftext)

            posts.append({
                "id": d.get("id"),
                "title": title or "Untitled",
                "image": image,
                "body": body
            })

        except:
            continue

    return posts

# ------------------ EMBED ------------------
def create_embed(post):
    embed = discord.Embed(
        title=post["title"],
        description=post["body"],
        color=0xff4500
    )

    if post["image"]:
        embed.set_image(url=post["image"])

    embed.set_footer(text="AQW Tracker")
    return embed

# ------------------ LOOP ------------------
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)

    if not channel:
        return

    posts = await asyncio.to_thread(fetch_reddit_user_posts)

    for post in posts:
        if await is_posted(post["id"]):
            continue

        embed = create_embed(post)
        await channel.send(embed=embed)
        await mark_posted(post["id"])

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
    await init_db()
    check_posts.start()
    await bot.tree.sync()

bot.run(TOKEN)
