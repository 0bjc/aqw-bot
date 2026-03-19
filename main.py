import os
import discord
from discord.ext import commands, tasks
import requests
import aiosqlite
import asyncio
import re

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = 1484113318095622315
REDDIT_USER = "DefNotDatenshi"
KEYWORDS = ["daily", "gift", "drop", "drops"]

DB = "drops.db"

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())

# Database functions...
async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS posted (id TEXT PRIMARY KEY)")
        await db.commit()

async def is_posted(post_id):
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT 1 FROM posted WHERE id = ?", (post_id,)) as cursor:
            return await cursor.fetchone() is not None

async def mark_posted(post_id):
    async with aiosqlite.connect(DB) as db:
        await db.execute("INSERT INTO posted (id) VALUES (?)", (post_id,))
        await db.commit()

# Parser
def extract_fields(text):
    text = text[:1500]
    def find(label):
        m = re.search(rf"{label}[:\-]\s*(.+)", text, re.IGNORECASE)
        return m.group(1).split("\n")[0].strip() if m else "Unknown"
    return {
        "map": find("map"),
        "monster": find("monster"),
        "weapons": find("weapon|drop|item"),
        "rarity": find("rarity")
    }

# Fetch Reddit posts safely
def fetch_reddit_user_posts():
    url = f"https://www.reddit.com/user/{REDDIT_USER}/submitted.json?limit=20"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()
    except:
        return []

    posts = []
    for child in data.get("data", {}).get("children", []):
        d = child.get("data", {})
        full_text = (d.get("title", "") + "\n" + d.get("selftext", "")).lower()
        if not any(k in full_text for k in KEYWORDS):
            continue

        info = extract_fields(d.get("title", "") + "\n" + d.get("selftext", ""))
        image = None
        if "preview" in d:
            try:
                image = d["preview"]["images"][0]["source"]["url"]
            except: pass

        posts.append({
            "id": d.get("id"),
            "title": d.get("title", "Untitled"),
            "image": image,
            "info": info
        })
    return posts

# Embed
def create_embed(post):
    info = post["info"]
    embed = discord.Embed(title=post["title"], color=0xff4500)
    embed.add_field(
        name="Drop Info",
        value=(
            f"**Map:** {info['map']}\n"
            f"**Monster:** {info['monster']}\n"
            f"**Weapons:** {info['weapons']}\n"
            f"**Rarity:** {info['rarity']}"
        )
    )
    if post["image"]: embed.set_image(url=post["image"])
    embed.set_footer(text="AQW Tracker")
    return embed

# Loop
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel: return
    posts = await asyncio.to_thread(fetch_reddit_user_posts)
    for post in posts:
        if await is_posted(post["id"]): continue
        await channel.send(embed=create_embed(post))
        await mark_posted(post["id"])

# Slash command
@bot.tree.command(name="latestdrops", description="Check latest AQW daily gifts/drops")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    posts = await asyncio.to_thread(fetch_reddit_user_posts)
    if not posts:
        await interaction.followup.send("No relevant daily gifts/drops found.")
        return
    await interaction.followup.send(embed=create_embed(posts[0]))

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    await init_db()
    check_posts.start()
    await bot.tree.sync()

bot.run(TOKEN)
