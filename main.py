import os
import discord
from discord.ext import commands, tasks
import requests
import aiosqlite

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = 1484113318095622315  # Replace with your channel ID
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

# ------------------ REDDIT FETCH ------------------
def fetch_reddit_user_posts():
    url = f"https://www.reddit.com/user/{REDDIT_USER}/submitted.json?limit=10"

    headers = {"User-Agent": "aqw-discord-bot"}

    res = requests.get(url, headers=headers)
    data = res.json()

    posts = []

    for post in data["data"]["children"]:
        post_data = post["data"]

        title = post_data["title"]
        post_id = post_data["id"]
        link = "https://reddit.com" + post_data["permalink"]

        # Filter keywords
        if not any(word in title.lower() for word in ["gift", "drop", "daily"]):
            continue

        image = None

        # Get preview image if exists
        if "preview" in post_data:
            image = post_data["preview"]["images"][0]["source"]["url"]

        posts.append({
            "id": post_id,
            "title": title,
            "url": link,
            "image": image
        })

    return posts

# ------------------ EMBED ------------------
def create_embed(post):
    embed = discord.Embed(
        title=post["title"],
        url=post["url"],
        color=0xff4500
    )

    if post["image"]:
        embed.set_image(url=post["image"])

    embed.set_footer(text=f"AQW Reddit Tracker ({REDDIT_USER})")
    return embed

# ------------------ LOOP ------------------
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        print("Channel not found")
        return

    posts = fetch_reddit_user_posts()

    for post in posts:
        if await is_posted(post["id"]):
            continue

        embed = create_embed(post)
        await channel.send(embed=embed)
        await mark_posted(post["id"])

# ------------------ COMMAND ------------------
@bot.tree.command(name="latestdrops", description="Check latest AQW posts from Reddit user")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer()
    posts = fetch_reddit_user_posts()

    if not posts:
        await interaction.followup.send(f"No relevant posts found from u/{REDDIT_USER}.")
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
