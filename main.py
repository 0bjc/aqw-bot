import os
import re
import requests
import aiosqlite
import discord
from discord.ext import commands, tasks
from bs4 import BeautifulSoup

# Config
TOKEN = os.getenv("TOKEN")  # Your Discord bot token in Railway environment variable
CHANNEL_ID = 1484113318095622315  # Replace with your Discord channel ID to post in
TAG_URL = "http://aqwwiki.wikidot.com/system:page-tags/tag/aegift"

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())

DB = "drops.db"

# Initialize SQLite DB
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

# Helper to build absolute URLs
def full_url(url):
    if url.startswith("http"):
        return url
    else:
        return "http://aqwwiki.wikidot.com" + url

# Extract image URLs from raw wikidot [[image URL]] markup
def extract_images_from_wikidot_raw(page_text):
    return re.findall(r'\[\[image\s+(https?://[^\]]+)\]\]', page_text)

# Scrape items from the tag page
def fetch_items():
    try:
        res = requests.get(TAG_URL)
        soup = BeautifulSoup(res.text, "html.parser")

        items = []
        # Grab all links inside the wikitable containing the items
        links = soup.select("table.wikitable a")[:20]  # Limit to first 20 to avoid overload

        for link in links:
            item_name = link.text.strip()
            item_url = full_url(link["href"])

            print(f"Found item: {item_name} - {item_url}")

            # Fetch item page raw content to parse image URLs
            item_res = requests.get(item_url, timeout=10)
            raw_text = item_res.text

            image_urls = extract_images_from_wikidot_raw(raw_text)

            if not image_urls:
                print(f"Skipping {item_name} - no image found")
                continue

            # Use first image found as main image
            image_url = image_urls[0]

            items.append({
                "id": item_name,
                "name": item_name,
                "url": item_url,
                "image": image_url,
            })

        return items

    except Exception as e:
        print(f"Error fetching items: {e}")
        return []

# Create Discord embed message for item
def create_embed(item):
    embed = discord.Embed(
        title=f"🍀 AE Gift Item: {item['name']}",
        url=item['url'],
        color=0x00ff88,
        description=f"[View on AQW Wiki]({item['url']})"
    )
    embed.set_image(url=item['image'])
    embed.set_footer(text="AQW AE Gift Tracker")
    return embed

# Background task to check for new items and post them
@tasks.loop(minutes=10)
async def check_drops():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        print("Channel not found")
        return

    items = fetch_items()

    for item in items:
        if await is_posted(item["id"]):
            continue

        embed = create_embed(item)
        await channel.send(embed=embed)
        await mark_posted(item["id"])

# Slash command to manually fetch latest items
@bot.tree.command(name="latestdrops", description="Check latest AE Gift items")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer()
    items = fetch_items()
    if not items:
        await interaction.followup.send("No AE Gift items found.")
        return

    item = items[0]
    embed = create_embed(item)
    await interaction.followup.send(embed=embed)

# Bot ready event
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    await init_db()
    check_drops.start()
    await bot.tree.sync()

bot.run(TOKEN)
