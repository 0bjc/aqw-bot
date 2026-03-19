import os
import discord
from discord.ext import commands, tasks
import requests
import aiosqlite
import re
import asyncio

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")  # Railway environment variable
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

# ------------------ TEXT PARSER ------------------
def extract_info(text):
    def find(label):
        pattern = rf"{label}[:\-]\s*(.+)"
        match = re.search(pattern, text, re.IGNORECASE)
        return match.group(1).strip() if match else "Unknown"

    return {
        "map": find("map"),
        "monster": find("monster"),
        "weapons": find("weapon|drop|item"),
        "rarity": find("rarity")
    }

# ------------------ REDDIT FETCH ------------------
def fetch_posts():
    url = f"https://www.reddit.com/user/{REDDIT_USER}/submitted.json?limit=10"
    headers = {"User-Agent": "aqw-bot"}

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
        title = d.get("title", "")
        body = d.get("selftext", "")
        full_text = title + "\n" + body

        # Filter keywords
        if not any(word in full_text.lower() for word in ["gift", "drop", "daily"]):
            continue

        info = extract_info(full_text)

        image = None
        if "preview" in d:
            try:
                image = d["preview"]["images"][0]["source"]["url"]
            except:
                image = None

        posts.append({
            "id": d.get("id"),
            "title": title,
            "url": "https://reddit.com" + d.get("permalink", ""),
            "image": image,
            "info": info
        })

    return posts

# ------------------ EMBED ------------------
def create_embed(post):
    info = post["info"]
    embed = discord.Embed(
        title="🎁 Daily Gift Drop",
        color=0x00ff88,
        url=post["url"]
    )

    embed.add_field(
        name="Available for ALL Players",
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

    embed.set_footer(text="AQW Drop Tracker")
    return embed

# ------------------ LOOP ------------------
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        print("Channel not found")
        return

    posts = await asyncio.to_thread(fetch_posts)

    for post in posts:
        if await is_posted(post["id"]):
            continue

        embed = create_embed(post)
        await channel.send(embed=embed)
        await mark_posted(post["id"])

# ------------------ SLASH COMMAND ------------------
@bot.tree.command(name="latestdrops", description="Show latest AQW drops from Reddit user")
async def latestdrops(interaction: discord.Interaction):
    # Defer immediately (prevents "thinking" timeout)
    await interaction.response.defer(thinking=True)

    posts = await asyncio.to_thread(fetch_posts)

    if not posts:
        await interaction.followup.send("No drops found.")
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
