import os
import discord
from discord.ext import commands, tasks
import requests
import aiosqlite
import asyncio
import re

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = 1484113318095622315  # Replace with your Discord channel ID
REDDIT_USER = "DefNotDatenshi"

DB = "drops.db"

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

# ------------------ PARSER ------------------
def extract_fields(text):
    text = text[:1000]  # Limit to first 1000 chars
    def find(label):
        try:
            pattern = rf"{label}[:\-]\s*(.+)"
            match = re.search(pattern, text, re.IGNORECASE)
            return match.group(1).strip() if match else "Unknown"
        except:
            return "Unknown"
    return {
        "map": find("map"),
        "monster": find("monster"),
        "weapons": find("weapon|drop|item"),
        "rarity": find("rarity")
    }

# ------------------ REDDIT FETCH ------------------
def fetch_reddit_user_posts():
    url = f"https://www.reddit.com/user/{REDDIT_USER}/submitted.json?limit=20"
    headers = {"User-Agent": "aqw-discord-bot"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        print(f"Error fetching Reddit: {e}")
        return []

    posts = []
    for post in data.get("data", {}).get("children", []):
        d = post["data"]
        title = d.get("title", "Untitled").lower()
        if not any(word in title for word in ["daily", "gift", "drop", "drops"]):
            continue  # Filter by keywords

        post_id = d.get("id")
        body = d.get("selftext", "")
        full_text = d.get("title", "") + "\n" + body

        info = extract_fields(full_text)

        image = None
        if "preview" in d:
            try:
                image = d["preview"]["images"][0]["source"]["url"]
            except:
                image = None

        posts.append({
            "id": post_id,
            "title": d.get("title", "Untitled"),  # Keep original casing
            "image": image,
            "info": info
        })
    return posts

# ------------------ EMBED ------------------
def create_embed(post):
    info = post["info"]
    embed = discord.Embed(
        title=post["title"],  # No hyperlink
        color=0xff4500
    )
    embed.add_field(
        name="Drop Info",
        value=(
            f"**Map:** {info['map']}\n"
            f"**Monster:** {info['monster']}\n"
            f"**Weapons:** {info['weapons']}\n"
            f"**Rarity:** {info['rarity']}"
        ),
        inline=False
    )
    if post["image"]:
        embed.set_image(url=post["image"])
    # Remove user info
    embed.set_footer(text="AQW Tracker")
    return embed

# ------------------ LOOP ------------------
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        print("Channel not found")
        return

    posts = await asyncio.to_thread(fetch_reddit_user_posts)

    for post in posts:
        if await is_posted(post["id"]):
            continue
        embed = create_embed(post)
        await channel.send(embed=embed)
        await mark_posted(post["id"])

# ------------------ SLASH COMMAND ------------------
@bot.tree.command(name="latestdrops", description="Check latest AQW posts from Reddit user")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    posts = await asyncio.to_thread(fetch_reddit_user_posts)

    if not posts:
        await interaction.followup.send(f"No relevant daily gifts/drops found.")
        return

    embed = create_embed(posts[0])
    await interaction.followup.send(embed=embed)

# ------------------ READY ------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    await init_db()
    check_posts.start()
    await bot.tree.sync()

bot.run(TOKEN)
