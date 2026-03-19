import os
import discord
from discord.ext import commands, tasks
import requests
from bs4 import BeautifulSoup
import aiosqlite
from datetime import datetime, timedelta

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = 1484113318095622315  # replace with your channel ID
DB = "drops.db"

URL = "http://aqwwiki.wikidot.com/system:page-tags/tag/aegift#pages"

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
def parse_date(date_text):
    date_text = date_text.lower().strip()
    now = datetime.utcnow()

    if "today" in date_text:
        return now
    elif "yesterday" in date_text:
        return now - timedelta(days=1)
    else:
        try:
            return datetime.strptime(date_text, "%d %b %Y")
        except:
            return None

def fetch_recent_items():
    res = requests.get(URL)
    soup = BeautifulSoup(res.text, "html.parser")

    items = []
    cutoff = datetime.utcnow() - timedelta(days=7)

    rows = soup.select("table tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        link_tag = cols[0].find("a")
        if not link_tag:
            continue

        item_name = link_tag.text.strip()
        item_url = "http://aqwwiki.wikidot.com" + link_tag["href"]

        date_text = cols[1].text.strip()
        item_date = parse_date(date_text)

        if not item_date:
            continue

        if item_date >= cutoff:
            items.append({
                "id": item_name,
                "name": item_name,
                "url": item_url
            })

    return items

# ------------------ IMAGE FETCH ------------------
def get_item_image(item_url):
    try:
        res = requests.get(item_url, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")

        img = soup.select_one(".item-icon img")
        if img:
            return img["src"]
    except:
        return None
    return None

# ------------------ EMBED ------------------
def create_embed(item, image_url):
    embed = discord.Embed(
        title=item["name"],
        url=item["url"],
        color=0x00ff88
    )
    embed.add_field(
        name="Source",
        value="AQW Wiki (AE Gift tag)",
        inline=False
    )
    if image_url:
        embed.set_image(url=image_url)
    embed.set_footer(text="AQW Auto Tracker")
    return embed

# ------------------ BOT LOOP ------------------
@tasks.loop(minutes=10)
async def check_items():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)

    items = fetch_recent_items()

    for item in items:
        if await is_posted(item["id"]):
            continue

        image_url = get_item_image(item["url"])
        embed = create_embed(item, image_url)

        await channel.send(embed=embed)
        await mark_posted(item["id"])

# ------------------ SLASH COMMAND ------------------
@bot.tree.command(name="latestdrops", description="Show recent AE Gift items (last 7 days)")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer()

    items = fetch_recent_items()
    if not items:
        await interaction.followup.send("No recent AE Gift items found.")
        return

    item = items[0]
    image_url = get_item_image(item["url"])
    embed = create_embed(item, image_url)

    await interaction.followup.send(embed=embed)

# ------------------ READY ------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    await init_db()
    check_items.start()
    await bot.tree.sync()

# ------------------ RUN ------------------
bot.run(TOKEN)
