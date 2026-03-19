import os
import discord
from discord.ext import commands, tasks
import requests
import aiosqlite
import asyncio
import openai  # OpenAI library

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = 1484113318095622315  # Replace with your Discord channel ID
REDDIT_USER = "DefNotDatenshi"

DB = "drops.db"
KEYWORDS = ["daily", "gift", "drop", "drops"]

# OpenAI API key from environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

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

# ------------------ GPT PARAPHRASER ------------------
def paraphrase_text_gpt(text: str) -> str:
    """
    Uses OpenAI GPT to paraphrase text.
    """
    if not text.strip():
        return "No details provided."

    prompt = f"Paraphrase the following AQW daily gift/drop description naturally and concisely:\n\n{text}\n\nParaphrased:"
    try:
        response = openai.Completion.create(
            model="text-davinci-003",
            prompt=prompt,
            temperature=0.7,
            max_tokens=300
        )
        paraphrased = response.choices[0].text.strip()
        return paraphrased if paraphrased else "No details provided."
    except Exception as e:
        print(f"OpenAI paraphrase error: {e}")
        return "No details provided."

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
        full_text = (d.get("title", "") + "\n" + d.get("selftext", "")).lower()
        if not any(k in full_text for k in KEYWORDS):
            continue

        image = None
        if "preview" in d:
            try:
                image = d["preview"]["images"][0]["source"]["url"]
            except:
                image = None

        # Paraphrase body with GPT
        body_text = d.get("selftext", "")
        paraphrased_body = paraphrase_text_gpt(body_text)

        posts.append({
            "id": d.get("id"),
            "title": d.get("title", "Untitled"),
            "image": image,
            "body": paraphrased_body
        })
    return posts

# ------------------ EMBED ------------------
def create_embed(post):
    embed = discord.Embed(
        title=post["title"],
        description=post["body"],  # GPT-paraphrased text
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
