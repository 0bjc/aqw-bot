import os
import discord
from discord.ext import commands, tasks
import requests
from bs4 import BeautifulSoup
import aiosqlite

# ------------------ CONFIG ------------------
TOKEN = os.getenv("TOKEN")  # Your Railway token
CHANNEL_ID = 1484113318095622315  # Replace with your channel ID
DB = "drops.db"

TAG_URL = "http://aqwwiki.wikidot.com/system:page-tags/tag/aegift#pages"

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
def fetch_items():
    """Scrape the AE Gift tag page and visit each link to extract items with images"""
    res = requests.get(TAG_URL)
    soup = BeautifulSoup(res.text, "html.parser")

    items = []
    # Limit to top 20 links to avoid overloading the wiki
    links = soup.select("table a")[:20]

    for link in links:
        item_name = link.text.strip()
        item_url = "http://aqwwiki.wikidot.com" + link["href"]

        # Visit item page to get image
        try:
            item_res = requests.get(item_url, timeout=10)
            item_soup = BeautifulSoup(item_res.text, "html.parser")
            img_tag = item_soup.select_one(".item-icon img")

            if not img_tag or not img_tag.get("src"):
                continue  # Skip items with no image

            image_url = img_tag["src"]

            items.append({
                "id": item_name,
                "name": item_name,
                "url": item_url,
                "image": image_url
            })

        except Exception as e:
            print(f"Error fetching {item_name}: {e}")
            continue

    return items

# ------------------ EMBED ------------------
def create_embed(item):
    embed = discord.Embed(
        title=item["name"],
        url=item["url"],
        color=0x00ff88
    )
    embed.set_image(url=item["image"])
    embed.set_footer(text="AQW Auto Tracker")
    return embed

# ------------------ BOT LOOP ------------------
@tasks.loop(minutes=10)
async def check_items():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)

    items = fetch_items()

    for item in items:
        if await is_posted(item["id"]):
            continue

        embed = create_embed(item)
        await channel.send(embed=embed)
        await mark_posted(item["id"])

# ------------------ SLASH COMMAND ------------------
@bot.tree.command(name="latestdrops", description="Show recent AE Gift items")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer()

    items = fetch_items()
    if not items:
        await interaction.followup.send("No AE Gift items found with images.")
        return

    # Show the first valid item
    item = items[0]
    embed = create_embed(item)
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
