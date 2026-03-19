import os
import discord
from discord.ext import commands, tasks
import requests
import aiosqlite
import asyncio

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = 1484113318095622315  # Replace with your channel ID
REDDIT_USER = "DefNotDatenshi"

DB = "drops.db"
KEYWORDS = ["daily", "gift", "drop", "drops"]  # simple filter

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

# ------------------ MOCK PARAPHRASER ------------------
def paraphrase_text(text: str) -> str:
    """
    Replace this with a real paraphrasing API (like OpenAI GPT or others).
    For now, just returns the text with minor formatting for demo.
    """
    # Simple mock: split lines and prepend "[Paraphrased]" to each line
    lines = text.splitlines()
    paraphrased = "\n".join(f"{line}" for line in lines if line.strip())
    return paraphrased if paraphrased else "No details provided."

# ------------------ REDDIT FETCH ------------------
def fetch_reddit_user_posts():
    url = f"https://www.reddit.com/user/{REDDIT_USER}/submitted.json?limit=20"
    headers = {"User-Agent": "aqw-discord-bot"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()
    except:
        return []

    posts = []
    for post in data.get("data", {}).get("children", []):
        d = post["data"]
        full_text = (d.get("title", "") + "\n" + d.get("selftext", "")).lower()
        if not any(k in full_text for k in KEYWORDS):
            continue

        image = None
        if "preview" in d:
            try:
                image = d["preview"]["images"][0]["source"]["url"]
            except:
                image = None

        # Paraphrase the body text
        body_text = d.get("selftext", "")
        paraphrased_body = paraphrase_text(body_text)

        posts.append({
            "id": d.get("id"),
            "title": d.get("title", "Untitled"),  # plain title
            "image": image,
            "body": paraphrased_body
        })
    return posts

# ------------------ EMBED ------------------
def create_embed(post):
    embed = discord.Embed(
        title=post["title"],  # no hyperlink
        description=post["body"],  # show paraphrased body
        color=0xff4500
    )
    if post["image"]:
        embed.set_image(url=post["image"])
    embed.set_footer(text="AQW Tracker")  # no username
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
    print(f"Logged in as {bot.user}")
    await init_db()
    check_posts.start()
    await bot.tree.sync()

bot.run(TOKEN)
