import os
import discord
from discord.ext import commands, tasks
import requests
from bs4 import BeautifulSoup
import aiosqlite

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")  # Discord bot token from Railway
CHANNEL_ID = 1484113318095622315  # Replace with your Discord channel ID
DB = "drops.db"

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# ------------------ DATABASE ------------------
async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS posted (
                id TEXT PRIMARY KEY
            )
        """)
        await db.commit()

async def is_posted(item_id):
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT 1 FROM posted WHERE id = ?", (item_id,)) as cursor:
            return await cursor.fetchone() is not None

async def mark_posted(item_id):
    async with aiosqlite.connect(DB) as db:
        await db.execute("INSERT INTO posted (id) VALUES (?)", (item_id,))
        await db.commit()

# ------------------ SCRAPER ------------------
def fetch_design_notes():
    """Scrape AQW Design Notes for daily gifts/drops"""
    url = "https://www.aq.com/gamedesignnotes/"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")

    posts = []

    # Each update is in an <article>
    for article in soup.find_all("article"):
        title_tag = article.find("h3") or article.find("h2")
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)

        # Filter using the requested words
        if any(word in title.lower() for word in ["daily gift", "daily drop", "gift", "drop"]):
            posts.append({
                "id": title,
                "title": title,
                "location": "/join luckdragon",
                "monster": "Guardian Luck Dragon",
                "drop": title,
                "rarity": "SEASONAL"
            })

    return posts

# ------------------ IMAGE FETCH ------------------
def get_item_image(item_name):
    """Get the real AQW item image from the wiki"""
    try:
        search_url = f"https://aqwwiki.wikidot.com/search:site/q/{item_name.replace(' ', '+')}"
        res = requests.get(search_url, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")

        result = soup.select_one(".title a")
        if not result:
            return None

        item_url = result["href"]
        item_res = requests.get(item_url, timeout=10)
        item_soup = BeautifulSoup(item_res.text, "html.parser")

        img = item_soup.select_one(".item-icon img")
        if img:
            return img["src"]

    except:
        return None
    return None

# ------------------ EMBED ------------------
def create_embed(data, image_url):
    embed = discord.Embed(
        title="🍀 AQW Daily Gifts / Drops 🍀",
        color=0x00ff88
    )
    embed.add_field(
        name="Details",
        value=(
            f"**Location:** {data['location']}\n"
            f"**Monster:** {data['monster']}\n"
            f"**Drop Item:** {data['drop']}\n"
            f"**Rarity:** {data['rarity']}"
        ),
        inline=False
    )
    if image_url:
        embed.set_image(url=image_url)
    embed.set_footer(text="AQW Auto Tracker")
    return embed

# ------------------ BOT LOOP ------------------
@tasks.loop(minutes=10)
async def check_drops():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        print(f"Channel {CHANNEL_ID} not found.")
        return

    posts = fetch_design_notes()
    for item in posts:
        if await is_posted(item["id"]):
            continue

        image_url = get_item_image(item["drop"])
        embed = create_embed(item, image_url)

        await channel.send(embed=embed)
        await mark_posted(item["id"])

# ------------------ SLASH COMMAND ------------------
@bot.tree.command(name="latestdrops", description="Check latest AQW daily gifts/drops manually")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer()
    posts = fetch_design_notes()
    if not posts:
        await interaction.followup.send("No drops found.")
        return
    item = posts[0]
    image_url = get_item_image(item["drop"])
    embed = create_embed(item, image_url)
    await interaction.followup.send(embed=embed)

# ------------------ READY ------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    await init_db()
    check_drops.start()
    await bot.tree.sync()

# ------------------ RUN ------------------
bot.run(TOKEN)
