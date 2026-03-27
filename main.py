from __future__ import annotations

import asyncio
import logging
import os
import re
import textwrap
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse, parse_qs
import io

import aiosqlite
import requests
from bs4 import BeautifulSoup
import json
import hashlib

import discord
from discord.ext import commands, tasks

# ---------------- WIKIDOT SESSION ----------------
session = requests.Session()

def wikidot_login(session: requests.Session) -> bool:
    """Perform Wikidot login and store session cookies."""
    # Debug: Check all environment variables
    print("DEBUG: Checking environment variables...")
    print(f"DEBUG: All env vars starting with WIKIDOT: {[k for k in os.environ.keys() if k.startswith('WIKIDOT')]}")
    
    email = os.getenv("WIKIDOT_EMAIL")
    password = os.getenv("WIKIDOT_PASSWORD")
    
    # Debug: Print environment variables status
    print(f"DEBUG: WIKIDOT_EMAIL found: {email is not None}")
    print(f"DEBUG: WIKIDOT_PASSWORD found: {password is not None}")
    print(f"DEBUG: WIKIDOT_EMAIL value (first 3 chars): {email[:3] if email else 'None'}")
    print(f"DEBUG: WIKIDOT_PASSWORD length: {len(password) if password else 0}")
    
    if not email or not password:
        print("ERROR: WIKIDOT_EMAIL or WIKIDOT_PASSWORD not found in environment variables")
        print("Please set these environment variables in your deployment:")
        print("- WIKIDOT_EMAIL: your Wikidot email")
        print("- WIKIDOT_PASSWORD: your Wikidot password")
        print("\nDEBUG: Available environment variables:")
        for key in sorted(os.environ.keys()):
            if 'TOKEN' in key or 'WIKIDOT' in key or 'CHANNEL' in key:
                print(f"  {key}: {'*' * len(os.environ[key]) if os.environ[key] else 'None'}")
        return False
    
    login_url = "https://www.wikidot.com/default--flow/login__LoginPopupScreen"
    payload = {
        "login": email,
        "password": password,
        "action": "Login"
    }
    
    try:
        response = session.post(login_url, data=payload, timeout=30)
        
        # Check if login was successful by looking for success indicators
        # Wikidot typically redirects or sets specific cookies on successful login
        if response.status_code == 200 and len(session.cookies) > 0:
            # Verify we have the required Wikidot session cookies
            wikidot_cookies = [c for c in session.cookies if 'wikidot' in c.name.lower()]
            if wikidot_cookies:
                print("Wikidot session active")
                return True
            else:
                print("Wikidot login failed: No session cookies found")
                return False
        else:
            print(f"Wikidot login failed: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Wikidot login failed: {e}")
        return False


def ensure_wikidot_session(session: requests.Session) -> bool:
    """Ensure Wikidot session is active, re-login if necessary."""
    # Check if we have session cookies
    wikidot_cookies = [c for c in session.cookies if 'wikidot' in c.name.lower()]
    
    if not wikidot_cookies:
        # No session cookies, need to login
        return wikidot_login(session)
    
    # Test session by making a simple request
    try:
        test_url = f"{WIKI_BASE}/system:recent-changes"
        response = session.get(test_url, timeout=10)
        
        # Check if we're redirected to login page or get auth errors
        if (response.status_code in (403, 429) or 
            'login' in response.url.lower() or 
            'wikidot.com/default--flow/login' in response.text):
            # Session expired, re-login
            return wikidot_login(session)
        
        return True  # Session is active
        
    except Exception as e:
        print(f"Session check failed: {e}")
        return wikidot_login(session)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1484113318095622315"))

WIKI_BASE = "https://silveraqworld.wikidot.com"
RECENT_URL_HTTP = "http://silveraqworld.wikidot.com/system:recent-changes"
RSS_URL = "http://aqwwiki.wikidot.com/feed/site-changes.xml"
DB = "drops.db"

CHECK_DAYS = 7
MAX_POSTS_PER_RUN = 100

MAX_DESC_LENGTH = 3800  # keep under discord 4096
MAX_TITLE_LENGTH = 256
WRAP_WIDTH = 55

# ---------------- DISCORD ----------------
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)


# ---------------- DATABASE ----------------
async def init_db() -> None:
    """Initialize SQLite database with items, counters, and grouped_posts tables."""
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT,
                price TEXT,
                rarity TEXT,
                image TEXT,
                images TEXT,
                content_hash TEXT,
                discord_message_id INTEGER,
                discord_channel_id INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create counters table for daily gift numbering
        await db.execute("""
            CREATE TABLE IF NOT EXISTS counters (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create grouped_posts table for tracking grouped posts and preventing duplicates
        await db.execute("""
            CREATE TABLE IF NOT EXISTS grouped_posts (
                group_key TEXT PRIMARY KEY,
                location TEXT NOT NULL,
                price TEXT NOT NULL,
                item_titles TEXT NOT NULL,
                categories TEXT NOT NULL,
                discord_message_id INTEGER,
                discord_channel_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Initialize daily gift counter if it doesn't exist
        await db.execute("""
            INSERT OR IGNORE INTO counters (name, value) VALUES ('daily_gift', 0)
        """)
        
        await db.commit()


async def get_and_increment_counter(counter_name: str) -> int:
    """Get current counter value and increment it atomically."""
    async with aiosqlite.connect(DB) as db:
        # Get current value and increment in one transaction
        async with db.execute("SELECT value FROM counters WHERE name = ?", (counter_name,)) as cur:
            result = await cur.fetchone()
            if result is None:
                # Counter doesn't exist, create it
                await db.execute("INSERT INTO counters (name, value) VALUES (?, 1)", (counter_name,))
                new_value = 1
            else:
                current_value = result[0]
                new_value = current_value + 1
                await db.execute("UPDATE counters SET value = ?, last_updated = CURRENT_TIMESTAMP WHERE name = ?", 
                               (new_value, counter_name))
        
        await db.commit()
        return new_value


def generate_daily_gift_title(gift_number: int) -> str:
    """Generate formatted daily gift title with weekday (no numbering)."""
    from datetime import datetime
    weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    current_weekday = weekday_names[datetime.now().weekday()]
    
    return f"🎁 __{current_weekday} Daily Gift__ 🎁"


def extract_breadcrumb_category(html_content: str, page_url: str = "") -> str:
    """Extract specific category or weapon type from Wikidot breadcrumb navigation and URL path."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Define all possible categories including weapon types
        weapon_types = [
            "Axes", "Bows", "Daggers", "Gauntlets", "Guns", "HandGuns", 
            "Maces", "Polearms", "Rifles", "Staffs", "Swords", "Wands", "Whips"
        ]
        
        main_categories = ["Weapon", "Armor", "Helm", "Cape", "Pet"]
        all_categories = main_categories + weapon_types
        
        # Method 1: Extract from URL path
        url_category = extract_category_from_url(page_url, weapon_types, main_categories)
        if url_category and url_category != "No category found":
            log.debug("Category from URL: %s for %s", url_category, page_url)
            return url_category
        
        # Method 2: Extract from breadcrumb navigation
        breadcrumb_category = extract_from_breadcrumbs(soup, all_categories)
        if breadcrumb_category and breadcrumb_category != "No category found":
            log.debug("Category from breadcrumb: %s", breadcrumb_category)
            return breadcrumb_category
        
        return "No category found"
        
    except Exception as e:
        log.error("Error extracting breadcrumb category: %s", e)
        return "No category found"


def extract_category_from_url(page_url: str, weapon_types: list[str], main_categories: list[str]) -> str:
    """Extract category from URL path."""
    if not page_url:
        return "No category found"
    
    # Parse URL path
    from urllib.parse import urlparse
    parsed = urlparse(page_url)
    path_parts = [part.lower() for part in parsed.path.split('/') if part]
    
    # Check for weapon types in URL path
    for weapon_type in weapon_types:
        if weapon_type.lower() in path_parts:
            return weapon_type
    
    # Check for main categories in URL path
    for category in main_categories:
        if category.lower() in path_parts:
            return category
    
    return "No category found"


def extract_from_breadcrumbs(soup: BeautifulSoup, all_categories: list[str]) -> str:
    """Extract category from breadcrumb elements."""
    # Look for breadcrumb navigation - common patterns on Wikidot
    breadcrumb_selectors = [
        "#breadcrumbs",  # Standard Wikidot breadcrumb ID
        ".breadcrumbs",  # Alternative class
        ".breadcrumb",   # Another common class
        "#breadcrumb-container",  # Container
        ".nav-path",     # Navigation path
        ".page-path",    # Page path
        ".site-path",    # Site path
    ]
    
    breadcrumb_text = None
    
    for selector in breadcrumb_selectors:
        breadcrumb_el = soup.select_one(selector)
        if breadcrumb_el:
            breadcrumb_text = breadcrumb_el.get_text(" ", strip=True)
            break
    
    # If no structured breadcrumb found, try to find breadcrumb-like text
    if not breadcrumb_text:
        # Look for text patterns that look like breadcrumbs
        # Common pattern: "Site » Category » Subcategory » Page"
        for element in soup.find_all(text=True):
            text = element.strip()
            if "»" in text and len(text.split("»")) >= 3:
                breadcrumb_text = text
                break
    
    if not breadcrumb_text:
        # Try to find navigation links that form a breadcrumb trail
        nav_links = soup.select("a[href*='/']")
        if len(nav_links) >= 3:
            # Check if consecutive links might form a breadcrumb
            breadcrumb_parts = []
            for link in nav_links[:5]:  # Check first 5 links
                link_text = link.get_text(strip=True)
                if link_text and link_text not in breadcrumb_parts:
                    breadcrumb_parts.append(link_text)
            
            if len(breadcrumb_parts) >= 3:
                breadcrumb_text = " » ".join(breadcrumb_parts)
    
    if breadcrumb_text:
        # Normalize breadcrumb text for comparison
        breadcrumb_lower = breadcrumb_text.lower()
        
        # Check for specific weapon types first (more specific)
        for category in all_categories:
            if category.lower() in breadcrumb_lower:
                return category
        
        # Check for plural forms and variations
        variations = {
            "Weapon": ["weapons"],
            "Armor": ["armors", "armour"],
            "Helm": ["helms", "helmets", "headgear"],
            "Cape": ["capes", "cloaks", "mantles"],
            "Pet": ["pets", "companions", "mounts"],
            "Axes": ["axe"],
            "Bows": ["bow"],
            "Daggers": ["dagger"],
            "Gauntlets": ["gauntlet"],
            "Guns": ["gun"],
            "HandGuns": ["handgun"],
            "Maces": ["mace"],
            "Polearms": ["polearm"],
            "Rifles": ["rifle"],
            "Staffs": ["staff"],
            "Swords": ["sword"],
            "Wands": ["wand"],
            "Whips": ["whip"]
        }
        
        for category, variants in variations.items():
            for variant in variants:
                if variant in breadcrumb_lower:
                    return category
        
        # If no specific category found, return the breadcrumb for debugging
        log.debug("Breadcrumb found but no category: %s", breadcrumb_text)
        return "No category found"
    
    return "No category found"


def categorize_item(item: dict) -> str:
    """Categorize an item using breadcrumb data first, then fallback to keywords."""
    # First, try to extract category from breadcrumb if we have the HTML
    if "html_content" in item:
        breadcrumb_category = extract_breadcrumb_category(item["html_content"], item.get("url", ""))
        if breadcrumb_category != "No category found":
            log.info("Category from breadcrumb: %s for %s", breadcrumb_category, item.get("title", "Unknown"))
            return breadcrumb_category
    
    # Fallback to keyword-based categorization with specific weapon types
    title = item.get("title", "").lower()
    content = item.get("content", "").lower()
    title_icons = item.get("title_icons", "").lower()
    
    # Specific weapon type keywords
    axe_keywords = ["axe", "hatchet", "battleaxe", "cleaver", "splitter"]
    bow_keywords = ["bow", "archery", "crossbow", "longbow", "shortbow", "compound"]
    dagger_keywords = ["dagger", "knife", "shiv", "stiletto", "blade", "dirk"]
    gauntlet_keywords = ["gauntlet", "glove", "fist", "hand", "punch"]
    gun_keywords = ["gun", "firearm", "pistol", "revolver", "shotgun"]
    handgun_keywords = ["handgun", "pistol", "revolver", "sidearm"]
    mace_keywords = ["mace", "club", "morningstar", "flail", "bludgeon"]
    polearm_keywords = ["polearm", "spear", "lance", "pike", "halberd", "trident"]
    rifle_keywords = ["rifle", "sniper", "carbine", "assault", "musket"]
    staff_keywords = ["staff", "rod", "wand", "stick", "quarterstaff"]
    sword_keywords = ["sword", "blade", "saber", "katana", "rapier", "scimitar", "claymore"]
    wand_keywords = ["wand", "magic", "spell", "arcane", "mystic"]
    whip_keywords = ["whip", "lash", "chain", "rope", "flail"]
    
    # Main category keywords (fallback)
    armor_keywords = [
        "armor", "armour", "plate", "mail", "chain", "scale", "leather", "cloth",
        "robe", "tunic", "vest", "chest", "breastplate", "cuirass", "defense"
    ]
    
    helm_keywords = [
        "helm", "helmet", "hood", "mask", "crown", "tiara", "circlet", "hat",
        "cap", "head", "skull", "visor", "coif", "headgear", "helmets"
    ]
    
    cape_keywords = [
        "cape", "cloak", "mantle", "shawl", "wrap", "scarf", "drape", "cover",
        "back", "wings", "wing", "jetpack", "pack", "backpack"
    ]
    
    pet_keywords = [
        "pet", "companion", "familiar", "mount", "rider", "dragon", "wolf", "bear",
        "cat", "dog", "bird", "eagle", "hawk", "phoenix", "lion", "tiger", "snake",
        "summon", "minion", "ally", "creature", "beast", "animal"
    ]
    
    # Check all sources for category indicators - specific weapon types first
    
    # Check title first (most reliable)
    if any(keyword in title for keyword in axe_keywords):
        return "Axes"
    if any(keyword in title for keyword in bow_keywords):
        return "Bows"
    if any(keyword in title for keyword in dagger_keywords):
        return "Daggers"
    if any(keyword in title for keyword in gauntlet_keywords):
        return "Gauntlets"
    if any(keyword in title for keyword in gun_keywords):
        return "Guns"
    if any(keyword in title for keyword in handgun_keywords):
        return "HandGuns"
    if any(keyword in title for keyword in mace_keywords):
        return "Maces"
    if any(keyword in title for keyword in polearm_keywords):
        return "Polearms"
    if any(keyword in title for keyword in rifle_keywords):
        return "Rifles"
    if any(keyword in title for keyword in staff_keywords):
        return "Staffs"
    if any(keyword in title for keyword in sword_keywords):
        return "Swords"
    if any(keyword in title for keyword in wand_keywords):
        return "Wands"
    if any(keyword in title for keyword in whip_keywords):
        return "Whips"
    
    # Check main categories as fallback
    if any(keyword in title for keyword in armor_keywords):
        return "Armor"
    if any(keyword in title for keyword in helm_keywords):
        return "Helm"
    if any(keyword in title for keyword in cape_keywords):
        return "Cape"
    if any(keyword in title for keyword in pet_keywords):
        return "Pet"
    
    # Check content if title doesn't match
    if any(keyword in content for keyword in axe_keywords):
        return "Axes"
    if any(keyword in content for keyword in bow_keywords):
        return "Bows"
    if any(keyword in content for keyword in dagger_keywords):
        return "Daggers"
    if any(keyword in content for keyword in gauntlet_keywords):
        return "Gauntlets"
    if any(keyword in content for keyword in gun_keywords):
        return "Guns"
    if any(keyword in content for keyword in handgun_keywords):
        return "HandGuns"
    if any(keyword in content for keyword in mace_keywords):
        return "Maces"
    if any(keyword in content for keyword in polearm_keywords):
        return "Polearms"
    if any(keyword in content for keyword in rifle_keywords):
        return "Rifles"
    if any(keyword in content for keyword in staff_keywords):
        return "Staffs"
    if any(keyword in content for keyword in sword_keywords):
        return "Swords"
    if any(keyword in content for keyword in wand_keywords):
        return "Wands"
    if any(keyword in content for keyword in whip_keywords):
        return "Whips"
    
    if any(keyword in content for keyword in armor_keywords):
        return "Armor"
    if any(keyword in content for keyword in helm_keywords):
        return "Helm"
    if any(keyword in content for keyword in cape_keywords):
        return "Cape"
    if any(keyword in content for keyword in pet_keywords):
        return "Pet"
    
    # Check title icons (tags)
    if any(keyword in title_icons for keyword in axe_keywords):
        return "Axes"
    if any(keyword in title_icons for keyword in bow_keywords):
        return "Bows"
    if any(keyword in title_icons for keyword in dagger_keywords):
        return "Daggers"
    if any(keyword in title_icons for keyword in gauntlet_keywords):
        return "Gauntlets"
    if any(keyword in title_icons for keyword in gun_keywords):
        return "Guns"
    if any(keyword in title_icons for keyword in handgun_keywords):
        return "HandGuns"
    if any(keyword in title_icons for keyword in mace_keywords):
        return "Maces"
    if any(keyword in title_icons for keyword in polearm_keywords):
        return "Polearms"
    if any(keyword in title_icons for keyword in rifle_keywords):
        return "Rifles"
    if any(keyword in title_icons for keyword in staff_keywords):
        return "Staffs"
    if any(keyword in title_icons for keyword in sword_keywords):
        return "Swords"
    if any(keyword in title_icons for keyword in wand_keywords):
        return "Wands"
    if any(keyword in title_icons for keyword in whip_keywords):
        return "Whips"
    
    if any(keyword in title_icons for keyword in armor_keywords):
        return "Armor"
    if any(keyword in title_icons for keyword in helm_keywords):
        return "Helm"
    if any(keyword in title_icons for keyword in cape_keywords):
        return "Cape"
    if any(keyword in title_icons for keyword in pet_keywords):
        return "Pet"
    
    return "Misc"


def group_items_by_location_price(items: list[dict]) -> dict[str, list[dict]]:
    """Group items by normalized Location and Price using hash-based keys."""
    # First, extract location and price for all items
    item_data = []
    for item in items:
        content = item.get("content", "")
        location = "Unknown"
        price = "Unknown"
        
        # Parse location
        loc_match = re.search(r"__\*\*Location:\*\*__\s*\n(.+?)(?=\n\n|\n__\*\*|$)", content, re.IGNORECASE | re.DOTALL)
        if loc_match:
            location = normalize_string(loc_match.group(1).strip())
        
        # Parse price
        price_match = re.search(r"__\*\*Price:\*\*__\s*\n(.+?)(?=\n\n|\n__\*\*|$)", content, re.IGNORECASE | re.DOTALL)
        if price_match:
            price = normalize_string(price_match.group(1).strip())
        
        item_data.append({
            'item': item,
            'location': location,
            'price': price
        })
    
    # Group by location and price
    groups_by_location_price = {}
    for data in item_data:
        key = (data['location'], data['price'])
        if key not in groups_by_location_price:
            groups_by_location_price[key] = []
        groups_by_location_price[key].append(data['item'])
    
    # Now generate hash keys for each group
    final_groups = {}
    for (location, price), items_in_group in groups_by_location_price.items():
        # Generate hash key for the entire group
        group_key_hash = generate_group_key(location, price, items_in_group)
        final_groups[group_key_hash] = items_in_group
    
    return final_groups


def create_categorized_item_list(items: list[dict]) -> str:
    """Create a categorized list of items with clickable links including specific weapon types."""
    # Define category order - weapon types first, then main categories
    category_order = [
        # Weapon Types (specific)
        "Axes", "Bows", "Daggers", "Gauntlets", "Guns", "HandGuns", 
        "Maces", "Polearms", "Rifles", "Staffs", "Swords", "Wands", "Whips",
        # Main Categories
        "Weapon", "Armor", "Helm", "Cape", "Pet", 
        # Fallback
        "Misc"
    ]
    
    # Categorize items
    categorized = {}
    for item in items:
        category = categorize_item(item)
        if category not in categorized:
            categorized[category] = []
        categorized[category].append(item)
    
    # Build output in order
    sections = []
    for category in category_order:
        if category in categorized and categorized[category]:
            sections.append(f"__**{category}:**__")
            for item in categorized[category]:
                title = item.get("title", "Unknown")
                url = item.get("url", "")
                if url:
                    sections.append(f"• [{title}]({url})")
                else:
                    sections.append(f"• {title}")
            sections.append("")  # Empty line between categories
    
    # Remove trailing empty line and join
    if sections and sections[-1] == "":
        sections.pop()
    
    return "\n".join(sections)


# ---------------- UI COMPONENTS ----------------
# Move view classes here to avoid forward reference issues

class PublicPaneView(discord.ui.View):
    """View for public messages with Show Pane button."""
    def __init__(self, image_url: str, item_title: str, timeout: float = None):
        super().__init__(timeout=timeout)
        self.image_url = image_url
        self.item_title = item_title
        self.add_item(ShowPaneButton(self))


class GroupedPaneView(discord.ui.View):
    """View for grouped messages with multi-image Show Pane button."""
    def __init__(self, items: list[dict], group_title: str, timeout: float = None):
        super().__init__(timeout=timeout)
        self.items = items
        self.group_title = group_title
        self.current_image_index = 0
        self.add_item(GroupedShowPaneButton(self))


class ShowPaneButton(discord.ui.Button):
    """Button to show ephemeral image pane."""
    def __init__(self, view: PublicPaneView):
        self.view_ref = view
        super().__init__(
            label="View ▼",
            style=discord.ButtonStyle.secondary,
            custom_id="show_pane"
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view_ref
        
        # Create ephemeral embed with image
        embed = discord.Embed(
            title=f"{view.item_title} - Image Preview",
            description="Click 'Close ▲' to hide this preview",
            color=discord.Color.blue()
        )
        embed.set_image(url=view.image_url)
        
        # Create ephemeral message with close button
        await interaction.response.send_message(
            embed=embed,
            view=EphemeralPaneView(),
            ephemeral=True
        )


class GroupedShowPaneButton(discord.ui.Button):
    """Button to show ephemeral category-separated messages for grouped items."""
    def __init__(self, view: GroupedPaneView):
        self.view_ref = view
        super().__init__(
            label="View ▼",
            style=discord.ButtonStyle.secondary,
            custom_id="show_grouped_pane"
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view_ref
        
        # Categorize items first
        categorized_items = {}
        for item in view.items:
            category = categorize_item(item)
            if category not in categorized_items:
                categorized_items[category] = []
            categorized_items[category].append(item)
        
        if not categorized_items:
            await interaction.response.send_message(
                "No items available for this group.",
                ephemeral=True
            )
            return
        
        # Defer the interaction to avoid timeout, but don't send initial message
        await interaction.response.defer(ephemeral=True)
        
        # Send separate ephemeral message for each category
        for category, items_in_category in categorized_items.items():
            await self._send_category_message(interaction, category, items_in_category, view.group_title)
    
    async def _send_category_message(self, interaction: discord.Interaction, category: str, items: list[dict], group_title: str):
        """Send a separate ephemeral message for a specific category."""
        # Collect all images for this category
        category_images = []
        for item in items:
            if item.get("images"):
                category_images.extend(item.get("images", []))
            elif item.get("image"):
                category_images.append(item["image"])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_images = []
        for img in category_images:
            if img and img not in seen:
                seen.add(img)
                unique_images.append(img)
        
        # Create category embed
        embed = discord.Embed(
            title=f"📂 {category} ({len(items)} items)",
            description=f"From: {group_title}\n\n" + 
                        "\n".join(f"• **{item.get('title', 'Unknown')}**\n  💰 {item.get('price', 'N/A')}" for item in items),
            color=discord.Color.blue()
        )
        
        # Add images if available
        if unique_images:
            # If multiple images, create a description with image count
            if len(unique_images) > 1:
                embed.description += f"\n\n🖼️ **{len(unique_images)} images available** - Scroll down to see all"
            
            # Set first image as main embed image
            embed.set_image(url=unique_images[0])
            
            # Create view with navigation for this category's images
            view = CategoryImageView(unique_images, category, group_title)
        else:
            # No images available
            embed.description += f"\n\n🖼️ No images available"
            view = None
        
        # Send as follow-up ephemeral message
        try:
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        except discord.HTTPException as e:
            log.error("Failed to send category message: %s", e)
            # Fallback: send without images if embed is too large
            try:
                fallback_embed = discord.Embed(
                    title=f"📂 {category} ({len(items)} items)",
                    description=f"From: {group_title}\n\n" + 
                                "\n".join(f"• **{item.get('title', 'Unknown')}**" for item in items),
                    color=discord.Color.blue()
                )
                await interaction.followup.send(embed=fallback_embed, ephemeral=True)
            except:
                # Final fallback: send text only
                await interaction.followup.send(
                    f"📂 **{category}** ({len(items)} items) from {group_title}:\n" +
                    "\n".join(f"• {item.get('title', 'Unknown')}" for item in items),
                    ephemeral=True
                )


class CategoryImageView(discord.ui.View):
    """View for navigating images within a specific category."""
    def __init__(self, images: list[str], category: str, group_title: str, timeout: float = 600.0):
        super().__init__(timeout=timeout)
        self.images = images
        self.category = category
        self.group_title = group_title
        self.current_index = 0
        
        # Add navigation buttons
        self.add_item(CategoryPrevButton(self))
        self.add_item(CategoryNextButton(self))
        self.add_item(ClosePaneButton())
        
        # Disable prev button if we're at the first image
        self.children[0].disabled = (len(images) <= 1)
        # Disable next button if we're at the last image
        if len(self.children) > 1:
            self.children[1].disabled = (len(images) <= 1)


class CategoryPrevButton(discord.ui.Button):
    """Button to show previous image in category view."""
    def __init__(self, view: CategoryImageView):
        self.view_ref = view
        super().__init__(
            label="◀️",
            style=discord.ButtonStyle.primary,
            custom_id="category_prev_image"
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view_ref
        if view.current_index > 0:
            view.current_index -= 1
            
            # Update button states
            view.children[0].disabled = (view.current_index == 0)
            view.children[1].disabled = (view.current_index == len(view.images) - 1)
            
            await interaction.response.edit_message(
                embed=self._create_image_embed(),
                view=view
            )
        else:
            await interaction.response.defer()  # Already at first image
    
    def _create_image_embed(self) -> discord.Embed:
        """Create embed for current image."""
        current_image = self.view_ref.images[self.view_ref.current_index]
        
        embed = discord.Embed(
            title=f"📂 {self.view_ref.category} - Image {self.view_ref.current_index + 1}/{len(self.view_ref.images)}",
            description=f"From: {self.view_ref.group_title}\n\nUse ◀️/▶️ to navigate images\nClick 'Close ▲' to hide this preview",
            color=discord.Color.blue()
        )
        embed.set_image(url=current_image)
        return embed


class CategoryNextButton(discord.ui.Button):
    """Button to show next image in category view."""
    def __init__(self, view: CategoryImageView):
        self.view_ref = view
        super().__init__(
            label="▶️",
            style=discord.ButtonStyle.primary,
            custom_id="category_next_image"
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view_ref
        if view.current_index < len(view.images) - 1:
            view.current_index += 1
            
            # Update button states
            view.children[0].disabled = (view.current_index == 0)
            view.children[1].disabled = (view.current_index == len(view.images) - 1)
            
            await interaction.response.edit_message(
                embed=self._create_image_embed(),
                view=view
            )
        else:
            await interaction.response.defer()  # Already at last image
    
    def _create_image_embed(self) -> discord.Embed:
        """Create embed for current image."""
        current_image = self.view_ref.images[self.view_ref.current_index]
        
        embed = discord.Embed(
            title=f"📂 {self.view_ref.category} - Image {self.view_ref.current_index + 1}/{len(self.view_ref.images)}",
            description=f"From: {self.view_ref.group_title}\n\nUse ◀️/▶️ to navigate images\nClick 'Close ▲' to hide this preview",
            color=discord.Color.blue()
        )
        embed.set_image(url=current_image)
        return embed


class GroupedEphemeralPaneView(discord.ui.View):
    """View for ephemeral grouped messages with navigation and close buttons."""
    def __init__(self, images: list[str], group_title: str, timeout: float = 600.0):
        super().__init__(timeout=timeout)
        self.images = images
        self.group_title = group_title
        self.current_index = 0
        
        # Add navigation buttons
        self.add_item(GroupedPrevButton(self))
        self.add_item(GroupedNextButton(self))
        self.add_item(ClosePaneButton())
        
        # Disable prev button if we're at the first image
        self.children[0].disabled = (len(images) <= 1)
        # Disable next button if we're at the last image
        if len(self.children) > 1:
            self.children[1].disabled = (len(images) <= 1)


class GroupedPrevButton(discord.ui.Button):
    """Button to show previous image in grouped ephemeral pane."""
    def __init__(self, view: GroupedEphemeralPaneView):
        self.view_ref = view
        super().__init__(
            label="◀️",
            style=discord.ButtonStyle.primary,
            custom_id="prev_image"
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view_ref
        if view.current_index > 0:
            view.current_index -= 1
            
            # Update button states
            view.children[0].disabled = (view.current_index == 0)
            view.children[1].disabled = (view.current_index == len(view.images) - 1)
            
            await interaction.response.edit_message(
                embed=self._create_image_embed(),
                view=view
            )
        else:
            await interaction.response.defer()  # Already at first image
    
    def _create_image_embed(self) -> discord.Embed:
        """Create embed for current image."""
        current_image = self.view_ref.images[self.view_ref.current_index]
        
        # Find which item this image belongs to
        item_info = ""
        # This would need access to the original items, but for now just show basic info
        item_info = f"\n**Image:** {self.view_ref.current_index + 1}/{len(self.view_ref.images)}"
        
        embed = discord.Embed(
            title=f"{self.view_ref.group_title} - Image {self.view_ref.current_index + 1}/{len(self.view_ref.images)}",
            description=f"Use ◀️/▶️ to navigate images{item_info}\nClick 'Close ▲' to hide this preview",
            color=discord.Color.blue()
        )
        embed.set_image(url=current_image)
        return embed


class GroupedNextButton(discord.ui.Button):
    """Button to show next image in grouped ephemeral pane."""
    def __init__(self, view: GroupedEphemeralPaneView):
        self.view_ref = view
        super().__init__(
            label="▶️",
            style=discord.ButtonStyle.primary,
            custom_id="next_image"
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view_ref
        if view.current_index < len(view.images) - 1:
            view.current_index += 1
            
            # Update button states
            view.children[0].disabled = (view.current_index == 0)
            view.children[1].disabled = (view.current_index == len(view.images) - 1)
            
            await interaction.response.edit_message(
                embed=self._create_image_embed(),
                view=view
            )
        else:
            await interaction.response.defer()  # Already at last image
    
    def _create_image_embed(self) -> discord.Embed:
        """Create embed for current image."""
        current_image = self.view_ref.images[self.view_ref.current_index]
        
        # Find which item this image belongs to
        item_info = f"\n**Image:** {self.view_ref.current_index + 1}/{len(self.view_ref.images)}"
        
        embed = discord.Embed(
            title=f"{self.view_ref.group_title} - Image {self.view_ref.current_index + 1}/{len(self.view_ref.images)}",
            description=f"Use ◀️/▶️ to navigate images{item_info}\nClick 'Close ▲' to hide this preview",
            color=discord.Color.blue()
        )
        embed.set_image(url=current_image)
        return embed


class EphemeralPaneView(discord.ui.View):
    """View for ephemeral messages with Close Pane button."""
    def __init__(self, timeout: float = 600.0):  # 10 minutes timeout
        super().__init__(timeout=timeout)
        self.add_item(ClosePaneButton())


class ClosePaneButton(discord.ui.Button):
    """Button to close ephemeral pane."""
    def __init__(self):
        super().__init__(
            label="Close ▲",
            style=discord.ButtonStyle.danger,
            custom_id="close_pane"
        )
    
    async def callback(self, interaction: discord.Interaction):
        # Dismiss the ephemeral message
        await interaction.response.defer()  # Acknowledge the interaction
        try:
            await interaction.delete_original_response()  # Delete the original ephemeral message
        except:
            # If deletion fails, try editing to empty
            await interaction.followup.edit_message(
                content="",
                embed=None,
                view=None
            )


# ---------------- CATEGORY BUTTON VIEW ----------------
class CategoryButton(discord.ui.Button):
    """Dynamic button for a specific item category."""
    def __init__(self, category: str, items: list[dict], category_view: 'CategoryButtonsView'):
        self.category = category
        self.items = items
        self.category_view = category_view
        
        # Style buttons based on category type
        style_map = {
            "Axes": discord.ButtonStyle.danger,
            "Bows": discord.ButtonStyle.success, 
            "Daggers": discord.ButtonStyle.secondary,
            "Gauntlets": discord.ButtonStyle.primary,
            "Guns": discord.ButtonStyle.danger,
            "HandGuns": discord.ButtonStyle.secondary,
            "Maces": discord.ButtonStyle.primary,
            "Polearms": discord.ButtonStyle.success,
            "Rifles": discord.ButtonStyle.danger,
            "Staffs": discord.ButtonStyle.success,
            "Swords": discord.ButtonStyle.primary,
            "Wands": discord.ButtonStyle.success,
            "Whips": discord.ButtonStyle.secondary,
            "Weapon": discord.ButtonStyle.danger,
            "Armor": discord.ButtonStyle.primary,
            "Helm": discord.ButtonStyle.secondary,
            "Cape": discord.ButtonStyle.success,
            "Pet": discord.ButtonStyle.primary,
            "Misc": discord.ButtonStyle.secondary
        }
        
        style = style_map.get(category, discord.ButtonStyle.secondary)
        
        # Create emoji mapping for categories
        emoji_map = {
            "Axes": "🪓", "Bows": "🏹", "Daggers": "🗡️", "Gauntlets": "🥊",
            "Guns": "🔫", "HandGuns": "🔫", "Maces": "🔨", "Polearms": "🔱",
            "Rifles": "🔫", "Staffs": "🔮", "Swords": "⚔️", "Wands": "🪄",
            "Whips": "🪢", "Weapon": "⚔️", "Armor": "🛡️", "Helm": "🎩",
            "Cape": "🧥", "Pet": "🐾", "Misc": "📦"
        }
        
        emoji = emoji_map.get(category, "📦")
        
        super().__init__(
            label=f"{category} ({len(items)})",
            style=style,
            emoji=emoji,
            custom_id=f"category_{category.lower().replace(' ', '_')}"
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Send ephemeral message with items from this category."""
        # Filter items for this category
        category_items = [item for item in self.items if categorize_item(item) == self.category]
        
        if not category_items:
            await interaction.response.send_message(
                f"No items found in {self.category} category.",
                ephemeral=True
            )
            return
        
        # Create embed for this category
        embed = discord.Embed(
            title=f"📂 {self.category} ({len(category_items)} items)",
            description=f"**Location:** {self.category_view.location}\n**Price:** {self.category_view.price}\n\n",
            color=discord.Color.blue()
        )
        
        # Add items to embed
        item_list = []
        for item in category_items:
            title = item.get("title", "Unknown")
            url = item.get("url", "")
            price = item.get("price", "N/A")
            
            if url:
                item_list.append(f"• **[{title}]({url})**\n  💰 {price}")
            else:
                item_list.append(f"• **{title}**\n  💰 {price}")
        
        # Add items to description
        embed.description += "\n".join(item_list)
        
        # Truncate if too long
        if len(embed.description) > 4000:
            embed.description = embed.description[:3950] + "\n... *(truncated)*"
        
        # Collect images for this category
        category_images = []
        for item in category_items:
            if item.get("images"):
                category_images.extend(item.get("images", []))
            elif item.get("image"):
                category_images.append(item["image"])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_images = []
        for img in category_images:
            if img and img not in seen:
                seen.add(img)
                unique_images.append(img)
        
        # Add image if available
        view = None
        if unique_images:
            embed.set_image(url=unique_images[0])
            if len(unique_images) > 1:
                embed.description += f"\n\n🖼️ **{len(unique_images)} images available**"
            
            # Create navigation view for images
            view = CategoryImageView(unique_images, self.category, f"{len(category_items)} Items")
        
        embed.set_footer(text="AQW Daily Gift - Category View")
        
        # Send ephemeral message
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class CategoryButtonsView(discord.ui.View):
    """Dynamic view with category buttons for grouped items."""
    def __init__(self, items: list[dict], location: str, price: str, timeout: float = 600.0):
        super().__init__(timeout=timeout)
        self.items = items
        self.location = location
        self.price = price
        
        # Categorize items and create buttons dynamically
        categories = {}
        for item in items:
            category = categorize_item(item)
            if category not in categories:
                categories[category] = []
            categories[category].append(item)
        
        # Define category order for button layout
        category_order = [
            "Axes", "Bows", "Daggers", "Gauntlets", "Guns", "HandGuns", 
            "Maces", "Polearms", "Rifles", "Staffs", "Swords", "Wands", "Whips",
            "Weapon", "Armor", "Helm", "Cape", "Pet", "Misc"
        ]
        
        # Add buttons in order, but only for categories that have items
        for category in category_order:
            if category in categories:
                button = CategoryButton(category, categories[category], self)
                self.add_item(button)
        
        # Add close button at the end
        self.add_item(ClosePaneButton())


# ---------------- CATEGORY HELPER FUNCTIONS ----------------
async def create_category_buttons_view(items: list[dict], location: str = "Various", price: str = "Various") -> CategoryButtonsView:
    """Helper function to create a CategoryButtonsView for any group of items."""
    return CategoryButtonsView(items, location, price)


def get_categories_from_items(items: list[dict]) -> dict[str, list[dict]]:
    """Helper function to get categorized items from a list of items."""
    categories = {}
    for item in items:
        category = categorize_item(item)
        if category not in categories:
            categories[category] = []
        categories[category].append(item)
    return categories


# ---------------- EMBED CREATION ----------------
async def create_grouped_embed(group_key: tuple[str, str], items: list[dict]) -> tuple[discord.Embed, CategoryButtonsView]:
    """Create a grouped embed for items with same Location and Price with ephemeral images."""
    location, price = group_key
    
    # Get daily gift number and generate title
    gift_number = await get_and_increment_counter("daily_gift")
    title = generate_daily_gift_title(gift_number)
    
    # Create categorized item list
    item_list = create_categorized_item_list(items)
    
    # Build description
    description_parts = [
        f"**Location:** {location}",
        f"**Price:** {price}",
        "",
        item_list
    ]
    
    description = "\n".join(description_parts)
    
    # Truncate if needed (Discord embed limit is 4096)
    if len(description) > 4096:
        description = description[:4090] + "..."
    
    embed = discord.Embed(
        title=title.upper(),
        description=description,
        color=0xFF4500,
    )
    
    # NO thumbnail for grouped embeds (as requested)
    # Only single-item posts get thumbnail images
    
    embed.set_footer(text=f"AQW Daily Gift - {len(items)} items grouped")
    
    # Collect all images from all items for ephemeral view
    all_images = []
    for item in items:
        if item.get("images"):
            all_images.extend(item.get("images", []))
        elif item.get("image"):
            all_images.append(item["image"])
    
    # Remove duplicates while preserving order
    seen = set()
    unique_images = []
    for img in all_images:
        if img and img not in seen:
            seen.add(img)
            unique_images.append(img)
    
    # Create category buttons view for grouped items
    view = CategoryButtonsView(items, location, price)
    
    return embed, view


async def delete_old_individual_messages(items: list[dict]):
    """Delete old individual messages for items that are now grouped."""
    for item in items:
        pid = urlparse(item["url"]).path.strip("/").replace("/", "-") or item["url"]
        stored_item = await get_stored_item(pid)
        
        if stored_item:
            msg_id = stored_item.get("discord_message_id")
            ch_id = stored_item.get("discord_channel_id")
            
            if msg_id and ch_id:
                try:
                    target_channel = bot.get_channel(ch_id)
                    if target_channel:
                        existing_msg = await target_channel.fetch_message(msg_id)
                        await existing_msg.delete()
                        log.info("Deleted old individual message for grouped item: %s", item["title"])
                except discord.NotFound:
                    log.debug("Old message not found (already deleted): %s", item["title"])
                except discord.Forbidden:
                    log.warning("No permission to delete old message: %s", item["title"])
                except Exception as e:
                    log.error("Failed to delete old message for %s: %s", item["title"], e)


def generate_content_hash(item: dict) -> str:
    """Generate hash for change detection."""
    content_data = {
        "title": item.get("title", ""),
        "content": item.get("content", ""),
        "price": item.get("price", ""),
        "rarity": item.get("rarity", ""),
        "images": sorted(item.get("images", []))  # Sort for consistent hashing
    }
    content_str = json.dumps(content_data, sort_keys=True)
    return hashlib.md5(content_str.encode()).hexdigest()

async def is_posted(pid: str) -> bool:
    """Check if item is already posted."""
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT 1 FROM items WHERE id=?", (pid,)) as cur:
            return await cur.fetchone() is not None

async def get_stored_item(pid: str) -> dict | None:
    """Get stored item data for comparison."""
    async with aiosqlite.connect(DB) as db:
        async with db.execute("""
            SELECT id, url, title, content, price, rarity, image, images, content_hash, discord_message_id, discord_channel_id 
            FROM items WHERE id=?
        """, (pid,)) as cur:
            row = await cur.fetchone()
            if row:
                return {
                    "id": row[0],
                    "url": row[1], 
                    "title": row[2],
                    "content": row[3],
                    "price": row[4],
                    "rarity": row[5],
                    "image": row[6],
                    "images": json.loads(row[7]) if row[7] else [],
                    "content_hash": row[8],
                    "discord_message_id": row[9],
                    "discord_channel_id": row[10]
                }
            return None

async def has_item_changed(pid: str, new_item: dict) -> bool:
    """Check if item has changed since last posting."""
    stored = await get_stored_item(pid)
    if not stored:
        return True  # New item
    
    new_hash = generate_content_hash(new_item)
    return stored["content_hash"] != new_hash

async def update_stored_item(pid: str, item: dict):
    """Update stored item data with changes."""
    content_hash = generate_content_hash(item)
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            UPDATE items SET 
                title=?, content=?, price=?, rarity=?, image=?, images=?, 
                last_updated=datetime('now'), content_hash=?
            WHERE id=?
        """, (
            item.get("title"), item.get("content"), item.get("price"), 
            item.get("rarity"), item.get("image"), json.dumps(item.get("images", [])),
            content_hash, pid
        ))
        await db.commit()

async def mark_posted(pid: str, item: dict, message_id: int = None, channel_id: int = None):
    """Mark an item as posted to avoid duplicates."""
    content_hash = generate_content_hash(item)
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            INSERT OR REPLACE INTO items 
            (id, url, title, content, price, rarity, image, images, last_updated, content_hash, discord_message_id, discord_channel_id) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?)
        """, (
            pid, item.get("url"), item.get("title"), item.get("content"), 
            item.get("price"), item.get("rarity"), item.get("image"), 
            json.dumps(item.get("images", [])), content_hash, message_id, channel_id
        ))
        await db.commit()


async def update_discord_message_info(pid: str, message_id: int, channel_id: int):
    """Update Discord message info for an existing item."""
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            UPDATE items SET discord_message_id=?, discord_channel_id=?, last_updated=datetime('now')
            WHERE id=?
        """, (message_id, channel_id, pid))
        await db.commit()


def normalize_string(s: str) -> str:
    """Normalize string for consistent grouping and comparison with aggressive cleaning."""
    if not s:
        return ""
    
    # Convert to lowercase and strip whitespace
    normalized = s.lower().strip()
    
    # Replace multiple whitespace with single space
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Remove common Wikidot formatting artifacts
    normalized = re.sub(r'__\*\*(.*?)\*\*__', r'\1', normalized)  # Remove bold formatting
    normalized = re.sub(r'\*\*(.*?)\*\*', r'\1', normalized)     # Remove simple bold
    normalized = re.sub(r'__(.*?)__', r'\1', normalized)         # Remove underline
    normalized = re.sub(r'~~(.*?)~~', r'\1', normalized)         # Remove strikethrough
    
    # Remove extra punctuation but keep essential ones
    normalized = re.sub(r'[^\w\s\-.,:()\/]', '', normalized)
    
    # Normalize common variations
    normalized = re.sub(r'n/a', 'na', normalized)  # Normalize N/A variations
    normalized = re.sub(r'ac\s*$', 'ac', normalized)  # Normalize AC currency
    
    # Strip again after regex operations
    normalized = normalized.strip()
    
    return normalized


def generate_group_key(location: str, price: str, items: list[dict]) -> str:
    """Generate a stable unique key for a group of items with robust normalization."""
    # Normalize location and price with aggressive cleaning
    norm_location = normalize_string(location)
    norm_price = normalize_string(price)
    
    # Create a more stable item signature using only URLs (most stable identifier)
    item_urls = []
    for item in items:
        url = normalize_string(item.get("url", ""))
        if url:  # Only include non-empty URLs
            item_urls.append(url)
    
    # Sort URLs for consistent ordering
    item_urls.sort()
    
    # Create combined string with separator that won't appear in normalized data
    # Use count of items + sorted URLs for stability
    combined = f"{norm_location}||{norm_price}||{len(items)}||{'||'.join(item_urls)}"
    
    # Generate hash for unique key
    import hashlib
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


async def is_group_already_posted(group_key: str) -> bool:
    """Check if a group has already been posted with atomic operation."""
    async with aiosqlite.connect(DB) as db:
        # Use immediate lock for atomic check
        await db.execute("BEGIN IMMEDIATE")
        try:
            async with db.execute("SELECT 1 FROM grouped_posts WHERE group_key=?", (group_key,)) as cur:
                exists = await cur.fetchone() is not None
            await db.commit()
            return exists
        except Exception:
            await db.rollback()
            raise


async def get_stored_group(group_key: str) -> dict | None:
    """Get stored group data for comparison."""
    async with aiosqlite.connect(DB) as db:
        async with db.execute("""
            SELECT group_key, location, price, item_titles, categories, discord_message_id, discord_channel_id 
            FROM grouped_posts WHERE group_key=?
        """, (group_key,)) as cur:
            row = await cur.fetchone()
            if row:
                return {
                    "group_key": row[0],
                    "location": row[1],
                    "price": row[2],
                    "item_titles": json.loads(row[3]) if row[3] else [],
                    "categories": json.loads(row[4]) if row[4] else [],
                    "discord_message_id": row[5],
                    "discord_channel_id": row[6]
                }
            return None


async def mark_group_posted(group_key: str, location: str, price: str, items: list[dict], 
                          message_id: int = None, channel_id: int = None):
    """Mark a group as posted to avoid duplicates with atomic operation."""
    # Extract item titles and categories
    item_titles = [item.get("title", "") for item in items]
    categories = list(set([categorize_item(item) for item in items]))  # Unique categories
    
    async with aiosqlite.connect(DB) as db:
        # Use immediate lock for atomic operation
        await db.execute("BEGIN IMMEDIATE")
        try:
            await db.execute("""
                INSERT OR REPLACE INTO grouped_posts 
                (group_key, location, price, item_titles, categories, discord_message_id, discord_channel_id, last_updated) 
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                group_key, location, price, json.dumps(item_titles), json.dumps(categories),
                message_id, channel_id
            ))
            await db.commit()
        except Exception:
            await db.rollback()
            raise


async def update_group_discord_message_info(group_key: str, message_id: int, channel_id: int):
    """Update Discord message info for an existing group."""
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            UPDATE grouped_posts SET discord_message_id=?, discord_channel_id=?, last_updated=datetime('now')
            WHERE group_key=?
        """, (message_id, channel_id, group_key))
        await db.commit()


async def delete_group_post(group_key: str):
    """Delete a group post record."""
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM grouped_posts WHERE group_key=?", (group_key,))
        await db.commit()


async def safe_post_grouped_embed(channel, group_key: tuple[str, str], items_in_group: list[dict]) -> bool:
    """Safely post a grouped embed with proper locking and duplicate prevention."""
    global posting_lock
    
    async with posting_lock:  # Prevent race conditions
        location, price = group_key
        
        # Generate stable group key
        group_key_hash = generate_group_key(location, price, items_in_group)
        
        # Log group details for debugging
        item_titles = [item.get("title", "Unknown") for item in items_in_group]
        log.info("Checking group: %s | Location: '%s' | Price: '%s' | Items: %s", 
                group_key_hash[:8], location, price, item_titles)
        
        # Double-check if already posted (within lock)
        if await is_group_already_posted(group_key_hash):
            log.info("Group already posted (double-check), skipping: %s", group_key_hash[:8])
            return False
        
        try:
            # Delete old individual messages first
            await delete_old_individual_messages(items_in_group)
            
            # Create and send grouped embed
            grouped_embed, view = await create_grouped_embed(group_key, items_in_group)
            grouped_msg = await channel.send(embed=grouped_embed, view=view)
            
            # Mark group as posted atomically
            await mark_group_posted(group_key_hash, location, price, items_in_group, 
                                  grouped_msg.id, channel.id)
            
            # Update all items in the group to reference the grouped message
            for item in items_in_group:
                pid = item["pid"]
                await update_stored_item(pid, item)
                await update_discord_message_info(pid, grouped_msg.id, channel.id)
            
            log.info("✅ Posted grouped embed with %d items (key: %s)", len(items_in_group), group_key_hash[:8])
            return True
            
        except discord.HTTPException as e:
            log.error("❌ Failed to send grouped message: %s", e)
            return False
        except Exception as e:
            log.error("❌ Unexpected error posting group: %s", e)
            return False


# ---------------- HELPERS ----------------
def _make_absolute(url: str, base: str | None = None) -> str:
    if not url or url.startswith(("http://", "https://")):
        return url or ""
    base = WIKI_BASE if not base else base
    return urljoin(base, url)


def parse_wiki_time(text: str) -> datetime | None:
    """
    Parse Wikidot recent-changes time strings.
    Supported:
    - `19 Mar 2026 06:46` / `19 Mar 2026 06:46:10` 
    - `20 Mar 26 - 00:00:00`  (your http format)
    Returns timezone-aware datetime in UTC.
    """
    if not text:
        return None

    t = text.replace("\xa0", " ").strip()
    t = re.sub(r"\s+", " ", t)

    # DD Mon YY - HH:MM:SS
    m = re.match(
        r"^(?P<day>\d{1,2})\s+(?P<mon>[A-Za-z]{3})\s+(?P<year>\d{2})\s*-\s*(?P<h>\d{1,2}):(?P<m>\d{2})(?::(?P<s>\d{2}))?$",
        t,
    )
    if m:
        year = 2000 + int(m.group("year"))
        mon = m.group("mon")
        mon_norm = mon[:1].upper() + mon[1:3].lower()
        month = datetime.strptime(mon_norm, "%b").month
        day = int(m.group("day"))
        hour = int(m.group("h"))
        minute = int(m.group("m"))
        second = int(m.group("s")) if m.group("s") else 0
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)

    # DD Mon YYYY HH:MM(:SS)
    m = re.match(
        r"^(?P<day>\d{1,2})\s+(?P<mon>[A-Za-z]{3})\s+(?P<year>\d{4})\s*(?:-|)?\s*(?P<h>\d{1,2}):(?P<m>\d{2})(?::(?P<s>\d{2}))?$",
        t,
    )
    if m:
        year = int(m.group("year"))
        mon = m.group("mon")
        mon_norm = mon[:1].upper() + mon[1:3].lower()
        month = datetime.strptime(mon_norm, "%b").month
        day = int(m.group("day"))
        hour = int(m.group("h"))
        minute = int(m.group("m"))
        second = int(m.group("s")) if m.group("s") else 0
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)

    return None


def page_has_aegift(soup: BeautifulSoup) -> bool:
    # Item pages have a tag list at the bottom; detect that
    for tag_el in soup.select(
        ".page-tags a, a[href*='tag/aegift'], a[href*='system:page-tags/tag/aegift']"
    ):
        txt = tag_el.get_text(strip=True).lower()
        if txt == "aegift":
            return True
        href = tag_el.get("href", "")
        if "aegift" in href.lower():
            return True
    
    # Debug: log what tags we actually find
    tags = soup.select(".page-tags a")
    if tags:
        tag_texts = [tag.get_text(strip=True) for tag in tags]
        log.debug("Found tags: %s", ", ".join(tag_texts))
    
    return False


def _wrap_lines(text: str) -> str:
    """Wrap lines to Discord's 4096 character limit with word boundaries."""
    if not text:
        return ""
    # Don't wrap - preserve original structure and spacing
    return text




def _extract_all_images(content_el: BeautifulSoup) -> list[str]:
    """Extract ALL item images from Wikidot tabview sections."""
    images = []
    
    # Find all images in the content
    for img in content_el.select("img[src]"):
        src = img.get("src")
        if not src:
            continue
        
        s = src.lower()
        # Skip thumbnails/icons/spacers
        if any(x in s for x in ("pixel", "spacer", "icon", "thumb")):
            continue
            
        # Include all valid images (imgur and others)
        if any(x in s for x in ("imgur.com", "i.imgur.com", ".png", ".jpg", ".jpeg", ".gif")):
            # Convert relative URLs to absolute
            if not src.startswith(("http://", "https://")):
                src = urljoin(WIKI_BASE, src)
            images.append(src)
    
    return images

def _extract_imgur_image(content_el: BeautifulSoup) -> str | None:
    """Legacy function - returns first imgur image for backward compatibility."""
    images = _extract_all_images(content_el)
    # Return first imgur image for compatibility
    for img in images:
        if "imgur.com" in img.lower():
            return img
    return images[0] if images else None


def _extract_title_icons(soup: BeautifulSoup) -> str | None:
    """
    Extract the small "icon" tags displayed under the page title.

    AQW Wiki uses a `.page-tags` block with many `<a>` tag links (sometimes
    with `javascript:;` href). We render them as a space-separated list
    right under the embed title.
    """
    tag_els = soup.select(".page-tags a")
    if not tag_els:
        return None

    parts: list[str] = []
    for a in tag_els:
        txt = a.get_text(strip=True)
        if not txt:
            continue
        href = a.get("href") or ""
        href = href.strip()
        if href.startswith("javascript:"):
            parts.append(txt)
            continue
        full = _make_absolute(href, None)
        # Only hyperlink for normal urls; otherwise keep plain text.
        if full and full.lower().startswith(("http://", "https://")):
            parts.append(f"[{txt}]({full})")
        else:
            parts.append(txt)

    if not parts:
        return None
    return " ".join(parts)


def _clean_item_text(raw_text: str) -> tuple[str, str]:
    """
    Parse the item page text into a clean structured description.
    Only shows important fields: Location, Price/Dropped by, Rarity.
    """
    text = raw_text.replace("\r\n", "\n").replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    # Remove unwanted sections entirely (be more specific to avoid removing Notes)
    text = re.sub(
        r"Sell\s*back\s*:\s*.+?(?=(?:Rarity:\s*)|(?:Description:\s*)|(?:Notes?\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*)|$)",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(
        r"Description\s*:?\s*.+?(?=(?:Notes?\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*)|$)",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(
        r"Base\s*Damage\s*:?\s*.+?(?=(?:Notes?\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*)|$)",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(
        r"Also see\s*:?\s*.+?(?=(?:Notes?\s*:?)|(?:Thanks to\s*)|$)",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(
        r"Thanks to\s*:?\s*.+?(?=(?:Notes?\s*:?)|$)",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    def _norm(val: str) -> str:
        val = re.sub(r"system:page-tags/tag/[^ \n]+", "", val, flags=re.IGNORECASE)
        # Only clean up system tags, preserve original structure
        val = val.strip()
        return val

    def _format_list(val: str) -> str:
        """
        Preserve original line structure including dash connections.
        """
        v = (val or "").strip()
        if not v or v.upper() == "N/A":
            return "N/A"

        # Only normalize excessive spaces, preserve structure and dashes
        v = re.sub(r"[ \t]+", " ", v).strip()
        
        # Handle dash connections - join lines where dash indicates continuation
        lines = v.split("\n")
        result_lines = []
        current_line = ""
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if line == "-":
                # Dash separator - connect to previous line
                if current_line:
                    current_line += " - "
                continue
            elif current_line and not current_line.endswith(" - "):
                # Previous line complete, start new line
                result_lines.append(current_line)
                current_line = line
            else:
                # Continue current line or start new line
                current_line += line if current_line.endswith(" - ") else f" {line}"
        
        if current_line:
            result_lines.append(current_line)
        
        return "\n".join(result_lines)

    # Capture only the important fields
    loc = "N/A"
    price = "N/A"
    rarity = "N/A"
    dropped_by = None
    merge_following = None
    note = None

    # Location field
    m_loc = re.search(
        r"Locations?\s*:?\s*(?P<val>.+?)\s*(?=(?:Price\s*:?)|(?:Dropped by\s*:?)|(?:Rarity\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_loc:
        loc = _norm(m_loc.group("val"))

    # Price field
    m_price = re.search(
        r"Price\s*:?\s*(?P<val>[\s\S]*?)(?=\s*Rarity\s*:|\s*Dropped by\s*:|\s*Notes?\s*:|\s*Also see\s*:|\s*Thanks to\s*:|$)",
        text,
        flags=re.IGNORECASE,
    )
    if m_price:
        price_raw = m_price.group("val")
        # Clean up price formatting but preserve quest text structure and parentheses
        price = re.sub(r"\s+", " ", price_raw.strip())
        price = price.strip()

    # Dropped by field (when Price is N/A)
    m_dropped = re.search(
        r"Dropped by\s*:?\s*(?P<val>.+?)\s*(?=(?:Merge the following\s*:?)|(?:Rarity\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_dropped:
        candidate = _norm(m_dropped.group("val"))
        if candidate and candidate.lower() not in {"n/a", "na"}:
            dropped_by = candidate

    # Merge the following field
    m_merge = re.search(
        r"Merge the following\s*:?\s*(?P<val>.+?)\s*(?=(?:Rarity\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_merge:
        candidate = _norm(m_merge.group("val"))
        if candidate and candidate.lower() not in {"n/a", "na"}:
            merge_following = candidate

    # Rarity field - more specific to stop at Note field
    m_rarity = re.search(
        r"Rarity\s*:?\s*(?P<val>.+?)\s*(?=(?:Rarity Description\s*:?)|(?:Notes?\s*:?)|(?:Also see\s*:?)|\Z)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_rarity:
        rarity = _norm(m_rarity.group("val"))

    # Note field - capture only the first Note: occurrence
    m_note = re.search(
        r"Notes?\s*:?\s*(?P<val>.+?)(?=(?:\n\s*Notes?\s*:)|\Z)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not m_note:
        # Try singular "Note:" pattern
        m_note = re.search(
            r"Note\s*:?\s*(?P<val>.+?)(?=(?:\n\s*Note\s*:)|\Z)",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
    if m_note:
        candidate = _norm(m_note.group("val"))
        # Skip note if it only contains "Also see:" content
        if candidate and candidate.lower() not in {"n/a", "na"} and not re.search(r'^\s*(?:also see\s*:?.*|see\s*:?.*)\s*$', candidate, re.IGNORECASE):
            note = candidate

    def _price_is_na(p: str) -> bool:
        p_norm = (p or "").strip()
        return p_norm.upper() == "N/A" or p_norm.upper().startswith("N/A")

    # Assemble only the important fields
    parts: list[str] = [
        f"__**Location:**__\n{_format_list(loc)}",
    ]

    if _price_is_na(price):
        # When Price is N/A, show Dropped by / Merge the following
        if dropped_by:
            parts.append(f"__**Dropped by:**__\n{_format_list(dropped_by)}")
        if merge_following:
            parts.append(f"__**Merge the following:**__\n{_format_list(merge_following)}")
        # Fallback if neither exists
        if not dropped_by and not merge_following:
            parts.append(f"__**Price:**__\n{_format_list(price)}")
    else:
        parts.append(f"__**Price:**__\n{_format_list(price)}")

    parts.append(f"__**Rarity:**__\n{_format_list(rarity)}")

    if note:
        parts.append(f"__**Note:**__\n{_format_list(note)}")
        log.info("Found note field: %s", note)

    structured = "\n\n".join(parts).strip()
    log.info("Final structured content: %s", structured)
    return structured, price


def extract_item_details(page_url: str) -> dict | None:
    # Ensure we have an active session before making requests
    if not ensure_wikidot_session(session):
        return None
        
    try:
        r = session.get(
            page_url,
            timeout=8,  # Reduced timeout
            headers={"User-Agent": "aqw-wiki-bot/1.0"},
        )
        r.raise_for_status()
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            log.debug("Page not found: %s", page_url)
        elif e.response.status_code in (503, 429):
            log.debug("Rate limited/blocked for %s: %s", page_url, e.response.status_code)
        else:
            log.warning("HTTP error %s for %s: %s", e.response.status_code, page_url, e)
        return None
    except requests.Timeout:
        log.debug("Timeout fetching %s", page_url)
        return None
    except Exception as e:
        log.warning("Failed to fetch %s: %s", page_url, e)
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    if not page_has_aegift(soup):
        log.debug("No aegift tag found on %s", page_url)
        return None

    title_el = soup.select_one("#page-title")
    if title_el:
        title = title_el.get_text(strip=True)
    else:
        title = soup.title.get_text(strip=True) if soup.title else "Untitled"
        title = title.replace(" - AQW", "").strip()

    if len(title) > MAX_TITLE_LENGTH:
        title = title[: MAX_TITLE_LENGTH - 3] + "..."

    content_el = soup.select_one("#page-content") or soup.select_one("#main-content")
    if not content_el:
        return None

    title_icons = _extract_title_icons(soup)

    # Remove tag UI (page-tags) but KEEP the info blocks because they contain:
    # Location/Price/Rarity/Notes/Drop/merge info used in the final structured output.
    for el in content_el.select(".page-tags"):
        el.decompose()
    for a in content_el.select("a[href*='/system:page-tags/tag/']"):
        a.decompose()
    for el in content_el.select("script, style"):
        el.decompose()

    raw_text = content_el.get_text(separator="\n", strip=True)
    cleaned, price = _clean_item_text(raw_text)

    # Debug: if the page actually has a Location label but our parser failed,
    # log a small snippet so we can tune the regex to the real wording.
    try:
        if "**Locations:**" in (cleaned or "") and "\nN/A" in (cleaned or ""):
            lower = (raw_text or "").lower()
            idx = lower.find("location")
            if idx != -1:
                snippet = raw_text[max(0, idx - 120) : idx + 280]
                log.warning("Location parse failed for %s. Snippet:\n%s", page_url, snippet)
    except Exception:
        # Never break scraping due to debug-only logging.
        pass

    # Extract ALL images for collage generation
    img_urls = _extract_all_images(content_el)
    img_url = _extract_imgur_image(content_el)  # Keep for backward compatibility

    if len(cleaned) > MAX_DESC_LENGTH:
        cleaned = cleaned[: MAX_DESC_LENGTH - 3] + "..."

    return {
        "title": title or "Untitled",
        "content": cleaned or "No item info available.",
        "price": price,
        "image": img_url,
        "images": img_urls,  # All images for collage
        "url": page_url,
        "title_icons": title_icons,
        "html_content": r.text,  # Include full HTML for breadcrumb parsing
    }


def _extract_recent_changes_entries() -> dict[str, datetime]:
    """
    Get mapping: page_url -> earliest change_time within CHECK_DAYS.
    Only checks the main recent changes page - no pagination.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=CHECK_DAYS)
    page_times: dict[str, datetime] = {}

    log.info("Starting recent changes extraction, cutoff: %s", cutoff)

    try:
        # Ensure we have an active session before making requests
        if not ensure_wikidot_session(session):
            return page_times
            
        res = session.get(RECENT_URL_HTTP, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        log.info("Fetching page: %s", RECENT_URL_HTTP)

        any_in_window = False
        rows_found = 0
        for row in soup.select("table tr"):
            cols = row.find_all("td")
            if len(cols) < 3:
                continue

            rows_found += 1
            link = cols[0].find("a")
            if not link:
                continue

            href = link.get("href", "")
            if not href or href.startswith("#"):
                continue

            time_text = cols[2].get_text(strip=True)
            change_time = parse_wiki_time(time_text)
            if not change_time:
                log.debug("Failed to parse time: %s", time_text)
                continue

            if change_time < cutoff:
                log.debug("Skipping old entry: %s (changed %s)", href, change_time)
                continue

            any_in_window = True
            page_url = _make_absolute(href).rstrip("/")
            prev = page_times.get(page_url)
            if prev is None or change_time < prev:
                page_times[page_url] = change_time
                log.debug("Found recent page: %s (changed %s)", page_url, change_time)

        log.info("Main page: %d rows found, %d in window, %d total pages", rows_found, any_in_window, len(page_times))

    except Exception as e:
        log.warning("Failed to fetch recent changes: %s", e)
        return page_times

    log.info("Recent changes extraction complete: %d pages found", len(page_times))
    return page_times


def _extract_related_item_links(page_url: str, max_links: int = 25) -> list[str]:
    """
    From a recent-changes page, extract likely internal item page links.
    Skips system pages and returns absolute URLs.
    """
    try:
        # Ensure we have an active session before making requests
        if not ensure_wikidot_session(session):
            return []
            
        r = session.get(page_url, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        r.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch page content for links %s: %s", page_url, e)
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    content = soup.select_one("#page-content")
    if not content:
        return []

    links: list[str] = []
    for a in content.select("a[href]"):
        href = a.get("href", "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        if href.startswith(("http://", "https://")):
            full = href
        else:
            full = _make_absolute(href, page_url)

        # Skip system pages and external links
        if any(x in full.lower() for x in ("system:", "forum:", "search:", "nav:", "admin:", "help:")):
            continue

        links.append(full)
        if len(links) >= max_links:
            break

    return list(dict.fromkeys(links))  # dedupe while preserving order


def fetch_recent_aegifts_fast(limit: int = MAX_POSTS_PER_RUN, newest_first: bool = False) -> list[dict]:
    """
    Fast version for slash commands - checks main page only.
    """
    page_times = _extract_recent_changes_entries()  # Check main page only
    if not page_times:
        log.info("No recent changes found")
        return []

    sorted_pages = sorted(page_times.items(), key=lambda kv: kv[1])
    if newest_first:
        sorted_pages = list(reversed(sorted_pages))

    results: list[dict] = []
    seen_ids: set[str] = set()
    pages_checked = 0

    for page_url, _t in sorted_pages:  # Process all pages
        pid = urlparse(page_url).path.strip("/").replace("/", "-") or page_url
        if pid in seen_ids:
            continue

        pages_checked += 1
        log.info("Checking page %d: %s", pages_checked, page_url)

        # Try the page itself first
        details = extract_item_details(page_url)
        if details:
            results.append({"id": pid, **details})
            seen_ids.add(pid)
            log.info("✓ Found aegift: %s", details["title"])
            if len(results) >= limit:
                break

        if len(results) >= limit:
            break

    log.info("Checked %d pages, found %d aegift items", pages_checked, len(results))
    return results


def fetch_recent_aegifts(limit: int = MAX_POSTS_PER_RUN, newest_first: bool = False) -> list[dict]:
    """
    Fetch aegift pages from the main recent changes page only.
    """
    page_times = _extract_recent_changes_entries()  # Check main page only
    if not page_times:
        log.info("No recent changes found")
        return []

    sorted_pages = sorted(page_times.items(), key=lambda kv: kv[1])
    if newest_first:
        sorted_pages = list(reversed(sorted_pages))

    results: list[dict] = []
    seen_ids: set[str] = set()

    for page_url, _t in sorted_pages:
        pid = urlparse(page_url).path.strip("/").replace("/", "-") or page_url
        if pid in seen_ids:
            continue

        log.info("Checking page: %s", page_url)

        # Try the page itself first
        details = extract_item_details(page_url)
        if details:
            results.append({"id": pid, **details})
            seen_ids.add(pid)
            log.info("✓ Found aegift: %s", details["title"])
        else:
            # If not a direct item page, try its child links
            child_links = _extract_related_item_links(page_url, max_links=3)
            log.debug("Found %d child links for %s", len(child_links), page_url)
            for child_url in child_links:
                child_pid = urlparse(child_url).path.strip("/").replace("/", "-") or child_url
                if child_pid in seen_ids:
                    continue
                child_details = extract_item_details(child_url)
                if child_details:
                    results.append({"id": child_pid, **child_details})
                    seen_ids.add(child_pid)
                    log.info("✓ Found aegift child: %s", child_details["title"])
                    if len(results) >= limit:
                        break

        if len(results) >= limit:
            break

    log.info("Checked %d pages, found %d aegift items", len(seen_ids), len(results))
    return results



def create_embed(post: dict) -> discord.Embed:
    wrapped_content = _wrap_lines(post["content"])
    # Remove title_icons to eliminate aegift hyperlink below item name
    desc = f"{wrapped_content}\n\n[View on Wiki]({post['url']})"
    if len(desc) > 4096:
        desc = desc[:4090] + "..."

    embed = discord.Embed(
        title=post["title"],
        description=desc,
        url=post["url"],
        color=0xFF4500,
    )
    # Note: Image will be handled by ShowPaneView, not set here initially
    embed.set_footer(text="AQW Daily Gift")
    return embed

async def create_pane_embed(post: dict) -> tuple[discord.Embed, PublicPaneView]:
    """Create an embed with Show Pane functionality for images."""
    wrapped_content = _wrap_lines(post["content"])
    # Remove title_icons to eliminate aegift hyperlink below item name
    desc = f"{wrapped_content}\n\n[View on Wiki]({post['url']})"
    if len(desc) > 4096:
        desc = desc[:4090] + "..."

    # Get daily gift number and generate title
    gift_number = await get_and_increment_counter("daily_gift")
    title = generate_daily_gift_title(gift_number)

    embed = discord.Embed(
        title=title.upper(),
        description=f"⠀\n**[{post['title']}]({post['url']})**\n\n{desc}",
        color=0xFF4500,
    )
    # Note: Image will be shown in ephemeral message only
    embed.set_footer(text="AQW Daily Gift")
    
    # Create view with image URL if available
    view = None
    if post.get("image"):
        view = PublicPaneView(post["image"], post["title"])
    
    return embed, view


# ---------------- SMART POLLING STATE ----------------
class SmartPolling:
    def __init__(self):
        self.current_interval = 60.0  # Default idle mode
        self.last_change_timestamp = None
        self.burst_mode = False
        self.no_change_count = 0  # Track consecutive no-change cycles
        
    def update_interval(self, has_new_changes: bool, has_error: bool = False):
        if has_error:
            # Error backoff mode
            self.current_interval = 90.0
            log.info("SMART POLLING: Error backoff (90s)")
            return
            
        if has_new_changes:
            # Activity detected - enter burst mode
            if not self.burst_mode:
                self.burst_mode = True
                self.current_interval = 15.0
                log.info("SMART POLLING: Burst mode (15s)")
            self.last_change_timestamp = datetime.now(timezone.utc)
            self.no_change_count = 0
        else:
            # No changes detected
            self.no_change_count += 1
            
            if self.burst_mode:
                # Check if we should exit burst mode (3 minutes of no changes)
                time_since_change = (datetime.now(timezone.utc) - self.last_change_timestamp).total_seconds() if self.last_change_timestamp else float('inf')
                
                if time_since_change > 180:  # 3 minutes
                    self.burst_mode = False
                    self.current_interval = 60.0
                    log.info("SMART POLLING: Cooldown → Idle")
                    self.no_change_count = 0
            elif self.no_change_count >= 3 and not self.burst_mode:
                # Safety: if we've had no changes for 3+ cycles, ensure idle mode
                self.current_interval = 60.0
                log.info("SMART POLLING: Idle (60s)")
        
        return self.current_interval

# Global smart polling instance
smart_polling = SmartPolling()

# Global posting lock to prevent race conditions
posting_lock = asyncio.Lock()

# ---------------- LOOP ----------------
@tasks.loop(seconds=1)  # Base loop, interval managed dynamically
async def check_posts():
    await bot.wait_until_ready()

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        log.warning("Channel %s not found", CHANNEL_ID)
        return
    
    # Add delay between messages to avoid rate limiting
    message_delay = 2.0  # 2 seconds between messages
    
    while True:
        try:
            posts = await asyncio.to_thread(fetch_recent_aegifts, limit=10)
            
            if posts is None:
                # Request failed
                smart_polling.update_interval(has_new_changes=False, has_error=True)
                await asyncio.sleep(smart_polling.current_interval)
                continue
            
            # Check for new changes and collect changed items
            has_new_changes = False
            changed_items = []
            
            for post in posts:
                pid = urlparse(post["url"]).path.strip("/").replace("/", "-") or post["url"]
                
                if await has_item_changed(pid, post):
                    has_new_changes = True
                    # Store the item data for grouping
                    post["pid"] = pid
                    changed_items.append(post)
            
            if changed_items:
                # Group changed items by Location and Price
                groups = group_items_by_location_price(changed_items)
                
                # Process each group
                for group_key_hash, items_in_group in groups.items():
                    # Extract location and price from first item for display
                    first_item = items_in_group[0]
                    content = first_item.get("content", "")
                    location = "Unknown"
                    price = "Unknown"
                    
                    # Parse location from first item
                    loc_match = re.search(r"__\*\*Location:\*\*__\s*\n(.+?)(?=\n\n|\n__\*\*|$)", content, re.IGNORECASE | re.DOTALL)
                    if loc_match:
                        location = normalize_string(loc_match.group(1).strip())
                    
                    # Parse price from first item
                    price_match = re.search(r"__\*\*Price:\*\*__\s*\n(.+?)(?=\n\n|\n__\*\*|$)", content, re.IGNORECASE | re.DOTALL)
                    if price_match:
                        price = normalize_string(price_match.group(1).strip())
                    
                    if len(items_in_group) >= 2:
                        # GROUPED POST: 2+ items with same Location + Price
                        log.info("Processing group: %d items with Location='%s', Price='%s'", 
                                len(items_in_group), location, price)
                        
                        # Use safe posting with proper locking and duplicate prevention
                        success = await safe_post_grouped_embed(channel, (location, price), items_in_group)
                        
                        if success:
                            await asyncio.sleep(message_delay)  # Rate limiting
                        else:
                            await asyncio.sleep(5)  # Longer delay on error
                    
                    else:
                        # SINGLE ITEM: No grouping, treat as normal individual post
                        item = items_in_group[0]
                        pid = item["pid"]
                        stored_item = await get_stored_item(pid)
                        
                        if stored_item:
                            # Existing item changed - update it
                            await update_stored_item(pid, item)
                            log.info("Item changed: %s", item["title"])
                            
                            # Try to edit existing Discord message
                            try:
                                embed, view = await create_pane_embed(item)
                                
                                # Get stored message info
                                msg_id = stored_item.get("discord_message_id")
                                ch_id = stored_item.get("discord_channel_id")
                                
                                if msg_id and ch_id:
                                    # Try to edit existing message
                                    target_channel = bot.get_channel(ch_id)
                                    if target_channel:
                                        try:
                                            existing_msg = await target_channel.fetch_message(msg_id)
                                            embed.set_footer(text="AQW Daily Gift - Updated")
                                            await existing_msg.edit(embed=embed, view=view)
                                            log.info("Updated existing message for: %s", item["title"])
                                            await asyncio.sleep(message_delay)
                                            continue
                                        except discord.NotFound:
                                            log.info("Original message not found, creating new one for: %s", item["title"])
                                        except discord.Forbidden:
                                            log.error("No permission to edit message for: %s", item["title"])
                                        except Exception as e:
                                            log.error("Failed to edit message for %s: %s", item["title"], e)
                                
                                # Fallback: create new message
                                new_msg = await channel.send(embed=embed, view=view)
                                await update_discord_message_info(pid, new_msg.id, channel.id)
                                log.info("Created new message for changed item: %s", item["title"])
                                await asyncio.sleep(message_delay)
                                
                            except discord.HTTPException as e:
                                log.error("Failed to update message: %s", e)
                                await asyncio.sleep(5)  # Longer delay on error
                        else:
                            # New single item
                            try:
                                embed, view = await create_pane_embed(item)
                                new_msg = await channel.send(embed=embed, view=view)
                                await mark_posted(pid, item, new_msg.id, channel.id)
                                log.info("New item: %s", item["title"])
                                await asyncio.sleep(message_delay)
                            except discord.HTTPException as e:
                                log.error("Failed to send new message: %s", e)
                                await asyncio.sleep(5)  # Longer delay on error
            
            # Update polling interval based on whether we found changes
            smart_polling.update_interval(has_new_changes=has_new_changes, has_error=False)
            
        except Exception as e:
            log.error("Error in check_posts loop: %s", e)
            smart_polling.update_interval(has_new_changes=False, has_error=True)
        
        # Sleep for the dynamically determined interval
        await asyncio.sleep(smart_polling.current_interval)



# ---------------- COMMAND ----------------
@bot.tree.command(name="latestdrops", description="Check latest AE gift pages")
async def latestdrops(interaction: discord.Interaction):
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        # Interaction token expired / no longer valid (common right after redeploy)
        return

    try:
        # Check the main page only
        posts = await asyncio.wait_for(
            asyncio.to_thread(fetch_recent_aegifts, 1, True),
            timeout=15  # Shorter timeout for single page
        )
        if not posts:
            await interaction.followup.send("No recent AE gifts found in the last 30 pages.")
            return

        embed, view = await create_pane_embed(posts[0])
        await interaction.followup.send(embed=embed, view=view)
    except asyncio.TimeoutError:
        await interaction.followup.send("Timed out fetching latest drops. Please try again in a few seconds.")
    except Exception as e:
        log.exception("latestdrops failed: %s", e)
        await interaction.followup.send("Something went wrong while fetching recent AE gifts.")





@bot.tree.command(name="checkpage", description="Check if a specific page has the aegift tag")
async def checkpage(interaction: discord.Interaction, page_name: str):
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        return

    try:
        page_url = f"{WIKI_BASE}/{page_name}"
        details = await asyncio.wait_for(
            asyncio.to_thread(extract_item_details, page_url),
            timeout=10
        )
        
        if details:
            await interaction.followup.send(f"✅ Found aegift: {details['title']}", embed=create_embed(details))
        else:
            await interaction.followup.send(f"❌ No aegift tag found on {page_url}")
    except asyncio.TimeoutError:
        await interaction.followup.send("Timed out checking page.")
    except Exception as e:
        log.exception("checkpage failed: %s", e)
        await interaction.followup.send(f"Error checking page: {e}")


@bot.tree.command(name="testaegift", description="Test a known aegift page")
async def testaegift(interaction: discord.Interaction):
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        return

    try:
        # Test with a known aegift page from the listing
        page_url = f"{WIKI_BASE}/alteon-plushie"
        details = await asyncio.wait_for(
            asyncio.to_thread(extract_item_details, page_url),
            timeout=15
        )
        
        if details:
            await interaction.followup.send(f"✅ Found aegift: {details['title']}", embed=create_embed(details))
        else:
            await interaction.followup.send(f"❌ No aegift tag found on {page_url}")
    except asyncio.TimeoutError:
        await interaction.followup.send("Timed out checking page.")
    except Exception as e:
        log.exception("testaegift failed: %s", e)
        await interaction.followup.send(f"Error checking page: {e}")


@bot.tree.command(name="ping", description="Test if bot is responding")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("Pong! Bot is working!")


@bot.tree.command(name="testcategories", description="Test category buttons with sample items")
async def testcategories(interaction: discord.Interaction):
    """Test command to demonstrate category buttons functionality."""
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        return

    # Create sample items for testing
    sample_items = [
        {
            "title": "Dragon Sword",
            "url": "https://example.com/dragon-sword",
            "price": "1000 AC",
            "content": "__**Location:**__\nDragon Lair\n\n__**Price:**__\n1000 AC\n\n__**Rarity:**__\nEpic",
            "images": ["https://i.imgur.com/dragon.jpg"]
        },
        {
            "title": "Magic Staff",
            "url": "https://example.com/magic-staff", 
            "price": "1000 AC",
            "content": "__**Location:**__\nDragon Lair\n\n__**Price:**__\n1000 AC\n\n__**Rarity:**__\nEpic",
            "images": ["https://i.imgur.com/staff.jpg"]
        },
        {
            "title": "Steel Armor",
            "url": "https://example.com/steel-armor",
            "price": "1000 AC", 
            "content": "__**Location:**__\nDragon Lair\n\n__**Price:**__\n1000 AC\n\n__**Rarity:**__\nEpic",
            "images": ["https://i.imgur.com/armor.jpg"]
        }
    ]

    # Create test embed
    embed = discord.Embed(
        title="🧪 Category Buttons Test",
        description="This is a test of the dynamic category buttons system.\n\nClick any category button below to see items from that category!",
        color=discord.Color.purple()
    )
    
    embed.add_field(name="Sample Items", value=f"• {sample_items[0]['title']}\n• {sample_items[1]['title']}\n• {sample_items[2]['title']}", inline=False)
    embed.set_footer(text="AQW Daily Gift - Test Command")
    
    # Create category buttons view
    view = CategoryButtonsView(sample_items, "Dragon Lair", "1000 AC")
    
    await interaction.followup.send(embed=embed, view=view)


# ---------------- READY ----------------
@bot.event
async def on_ready():
    log.info("Logged in as %s", bot.user)
    
    # Perform Wikidot login once at startup
    if not wikidot_login(session):
        log.error("Wikidot login failed, bot will continue without authentication")
    
    await init_db()
    if not check_posts.is_running():
        check_posts.start()
    
    # Debug: Log all registered commands
    commands = [cmd.name for cmd in bot.tree.get_commands()]
    log.info("Registered commands: %s", ", ".join(commands))
    
    await bot.tree.sync()
    log.info("Commands synced.")
    
    # Force global sync to ensure commands appear
    try:
        synced = await bot.tree.sync()
        log.info("Globally synced %d commands", len(synced))
    except Exception as e:
        log.error("Failed to sync commands globally: %s", e)


if __name__ == "__main__":
    max_retries = 5
    base_delay = 60

    for attempt in range(max_retries):
        try:
            bot.run(TOKEN)
            break
        except discord.HTTPException as e:
            if e.status == 429 and attempt < max_retries - 1:
                delay = base_delay * (2**attempt)
                retry_after = getattr(e, "retry_after", None)
                wait = retry_after if retry_after is not None else delay
                log.warning(
                    "Rate limited (429). Waiting %ds before retry (%d/%d)...",
                    wait,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(wait)
                continue
            raise
