import os
import discord
from discord.ext import commands, tasks
import aiosqlite
import asyncio
import textwrap
import aiohttp
import xml.etree.ElementTree as ET

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

# ------------------ REDDIT FETCH (RSS) ------------------
async def fetch_reddit_user_posts():
    url = f"https://www.reddit.com/user/{REDDIT_USER}.rss"

    try:
        timeout = aiohttp.ClientTimeout(total=10)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as res:
                if res.status != 200:
                    return []
                text = await res.text()
    except:
        return []

    try:
        root = ET.fromstring(text)
    except:
        return []

    posts = []

    for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
        try:
            title = entry.find("{http://www.w3.org/2005/Atom}title").text or ""
            content = entry.find("{http://www.w3.org/2005/Atom}content").text or ""
            post_id = entry.find("{http://www.w3.org/2005/Atom}id").text

            full_text = (title + " " + content).lower()

            if not any(k in full_text for k in KEYWORDS):
                continue

            # Extract image
            image = None
            if 'img src="' in content:
                start = content.find('img src="') + 9
                end = content.find('"', start)
                image = content[start:end]

            body = paraphrase_text(content)

            posts.append({
                "id": post_id,
                "title": title,
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

    posts = await fetch_reddit_user_posts()

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

    try:
        posts = await asyncio.wait_for(fetch_reddit_user_posts(), timeout=12)
    except asyncio.TimeoutError:
        await interaction.followup.send("Request timed out. Try again.")
        return

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
