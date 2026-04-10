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

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, use system environment variables

# ==================== CUSTOM EMOJI SYSTEM ====================

# Custom emoji mapping for item types (using actual Discord emoji IDs)
ITEM_TYPE_EMOJIS = {
    'sword': '<:aqwsword:1491402704822468690>',
    'helm': '<:aqwhelm:1491402425683284078>',
    'armor': '<:aqwarmor:1491402207894179850>',
    'cape': '<:aqwcape:1491402257449877565>',
    'pet': '<:aqwpet:1491402662829359124>',
    'class': '<:aqwclass:1491402373833293936>',
    'house': '<:aqwhouse:1491402472688718068>',
    'floor': '<:aqwfloor:1491402510911410347>',
    'wall': '<:aqwwall:1491402554381172787>',
    'ground': '<:aqwground:1491402322318721074>',
    'necklace': '<:aqwnecklace:1491402627244884103>',
    'misc': '<:aqwmisc:1491402592884887612>',
    'gift': '<:aqwgift:1491402775009955950>'
}

# Fallback emoji for unknown types
DEFAULT_EMOJI = '<:aqwmisc:1491402592884887612>'

def parse_discord_emoji(emoji_string: str) -> discord.PartialEmoji:
    """Parse emoji string into discord.PartialEmoji object."""
    if not emoji_string:
        return None
    
    # Check if it's a custom emoji format <:name:id>
    if emoji_string.startswith('<:') and emoji_string.endswith('>'):
        try:
            # Remove <: and > to get name:id
            inner = emoji_string[2:-1]
            if ':' in inner:
                name, emoji_id = inner.split(':', 1)
                return discord.PartialEmoji(name=name, id=int(emoji_id))
        except (ValueError, IndexError):
            pass
    
    # Fallback: try to use as emoji name
    return discord.PartialEmoji(name=emoji_string)


def get_item_type_emoji(item_type: str) -> str:
    """Get the custom emoji for an item type, with fallback."""
    if not item_type:
        return DEFAULT_EMOJI
    
    # Normalize item type to lowercase
    normalized_type = item_type.lower().strip()
    
    # Direct mapping
    if normalized_type in ITEM_TYPE_EMOJIS:
        return ITEM_TYPE_EMOJIS[normalized_type]
    
    # Check if any keyword matches the type
    for key, emoji in ITEM_TYPE_EMOJIS.items():
        if key in normalized_type:
            return emoji
    
    # Fallback to misc
    return DEFAULT_EMOJI

def detect_item_type_from_title(title: str) -> str:
    """Detect item type from title or content."""
    if not title:
        return 'misc'
    
    title_lower = title.lower()
    
    # Check for daily gift
    if 'daily gift' in title_lower or 'gift' in title_lower:
        return 'gift'
    
    # Check for specific item types
    type_keywords = {
        'sword': ['sword', 'blade', 'dagger', 'weapon'],
        'helm': ['helm', 'helmet', 'hat', 'cap'],
        'armor': ['armor', 'armour', 'chest', 'plate'],
        'cape': ['cape', 'cloak', 'mantle'],
        'pet': ['pet', 'companion', 'mount'],
        'class': ['class', 'enhancement'],
        'house': ['house', 'home', 'building'],
        'floor': ['floor', 'tile', 'carpet'],
        'wall': ['wall', 'decoration'],
        'ground': ['ground', 'terrain'],
        'necklace': ['necklace', 'amulet', 'pendant']
    }
    
    for item_type, keywords in type_keywords.items():
        if any(keyword in title_lower for keyword in keywords):
            return item_type
    
    return 'misc'

# ---------------- WIKIDOT SESSION ----------------
session = requests.Session()

def wikidot_login(session: requests.Session) -> bool:
    """TEMP: Skip login for testing - just return True"""
    log.info("Skipping Wikidot login for testing")
    return True


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
# Discord Configuration
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1484113318095622315"))

WIKI_BASE = "https://reaqw.wikidot.com"
RECENT_URL_HTTP = "http://reaqw.wikidot.com/system:recent-changes"
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
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                location TEXT
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
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                group_data TEXT,
                content_hash TEXT
            )
        """)
        
        # Migration: Add location column to items table if it doesn't exist
        try:
            await db.execute("ALTER TABLE items ADD COLUMN location TEXT")
            log.info("Added location column to items table (migration)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                log.debug("Location column already exists in items table")
            else:
                log.error(f"Error adding location column: {e}")
        
        # Migration: Add group_data and content_hash columns to grouped_posts table if they don't exist
        try:
            await db.execute("ALTER TABLE grouped_posts ADD COLUMN group_data TEXT")
            log.info("Added group_data column to grouped_posts table (migration)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                log.debug("group_data column already exists in grouped_posts table")
            else:
                log.error(f"Error adding group_data column: {e}")
        
        try:
            await db.execute("ALTER TABLE grouped_posts ADD COLUMN content_hash TEXT")
            log.info("Added content_hash column to grouped_posts table (migration)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                log.debug("content_hash column already exists in grouped_posts table")
            else:
                log.error(f"Error adding content_hash column: {e}")
        
        # Initialize daily gift counter if it doesn't exist
        await db.execute("""
            INSERT OR IGNORE INTO counters (name, value) VALUES ('daily_gift', 0)
        """)
        
        # Create scraper state table for cursor-based tracking
        await db.execute("""
            CREATE TABLE IF NOT EXISTS scraper_state (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Initialize last_seen_change if not exists
        await db.execute("""
            INSERT OR IGNORE INTO scraper_state (key, value) 
            VALUES ('last_seen_change', NULL)
        """)
        
        await db.commit()
        
        # Create source ID tracking tables
        await db.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                source_id TEXT PRIMARY KEY,
                message_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                group_id TEXT NULL,
                content_hash TEXT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Add group_key column to posts table if it doesn't exist
        try:
            await db.execute("ALTER TABLE posts ADD COLUMN group_key TEXT")
            log.info("Added group_key column to posts table (migration)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                log.debug("group_key column already exists in posts table")
            else:
                log.error(f"Error adding group_key column to posts: {e}")
        
        # Add last_data column to posts table if it doesn't exist
        try:
            await db.execute("ALTER TABLE posts ADD COLUMN last_data TEXT")
            log.info("Added last_data column to posts table (migration)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                log.debug("last_data column already exists in posts table")
            else:
                log.error(f"Error adding last_data column to posts: {e}")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                group_key TEXT PRIMARY KEY,
                message_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                content_hash TEXT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Add content_hash column to existing posts table if it doesn't exist
        try:
            await db.execute("ALTER TABLE posts ADD COLUMN content_hash TEXT")
            log.info("Added content_hash column to posts table (migration)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                log.debug("content_hash column already exists in posts table")
            else:
                log.error(f"Error adding content_hash column to posts: {e}")
        
        try:
            await db.execute("ALTER TABLE groups ADD COLUMN content_hash TEXT")
            log.info("Added content_hash column to groups table (migration)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                log.debug("content_hash column already exists in groups table")
            else:
                log.error(f"Error adding content_hash column to groups: {e}")
        
        # Add updated_at column if it doesn't exist
        try:
            await db.execute("ALTER TABLE posts ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            log.info("Added updated_at column to posts table (migration)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                log.debug("updated_at column already exists in posts table")
            else:
                log.error(f"Error adding updated_at column to posts: {e}")
        
        try:
            await db.execute("ALTER TABLE groups ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            log.info("Added updated_at column to groups table (migration)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                log.debug("updated_at column already exists in groups table")
            else:
                log.error(f"Error adding updated_at column to groups: {e}")
        
        await db.commit()
        
        log.info("Source ID tracking system initialized")


# ==================== SOURCE ID TRACKING SYSTEM ====================

async def get_post(source_id: str) -> dict | None:
    """Get existing post record by source_id."""
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute("""
            SELECT message_id, channel_id, group_id 
            FROM posts 
            WHERE source_id = ?
        """, (source_id,))
        row = await cursor.fetchone()
        
        if row:
            message_id, channel_id, group_id = row
            return {
                "message_id": message_id,
                "channel_id": channel_id,
                "group_id": group_id
            }
        return None


async def save_single_post(source_id: str, message, content_hash: str = None) -> None:
    """Save a single post record (group_id = NULL) with content hash."""
    try:
        async with aiosqlite.connect(DB) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute("""
                    INSERT OR REPLACE INTO posts 
                    (source_id, message_id, channel_id, group_id, content_hash, updated_at) 
                    VALUES (?, ?, ?, NULL, ?, CURRENT_TIMESTAMP)
                """, (source_id, message.id, message.channel.id, content_hash))
                await db.commit()
                log.debug(f"Successfully saved post for {source_id}")
            except Exception as e:
                await db.rollback()
                log.error(f"Failed to save post for {source_id}: {e}")
                raise
    except Exception as e:
        log.error(f"Database connection error in save_single_post: {e}")
        raise


async def get_group(group_id: str) -> dict | None:
    """Get existing group record by group_id."""
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute("""
            SELECT message_id, channel_id, content_hash 
            FROM groups 
            WHERE group_id = ?
        """, (group_id,))
        row = await cursor.fetchone()
        
        if row:
            message_id, channel_id, content_hash = row
            return {
                "message_id": message_id,
                "channel_id": channel_id,
                "content_hash": content_hash
            }
        return None


async def save_group(group_id: str, message, content_hash: str = None) -> None:
    """Save a new group record with content hash."""
    try:
        async with aiosqlite.connect(DB) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute("""
                    INSERT OR REPLACE INTO groups 
                    (group_id, message_id, channel_id, content_hash, updated_at) 
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (group_id, message.id, message.channel.id, content_hash))
                await db.commit()
                log.debug(f"Successfully saved group {group_id}")
            except Exception as e:
                await db.rollback()
                log.error(f"Failed to save group {group_id}: {e}")
                raise
    except Exception as e:
        log.error(f"Database connection error in save_group: {e}")
        raise


async def add_item_to_group(source_id: str, group_id: str, message, content_hash: str = None) -> None:
    """Add an item to a group (maps source_id to group's message) with content hash."""
    try:
        async with aiosqlite.connect(DB) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute("""
                    INSERT OR REPLACE INTO posts 
                    (source_id, message_id, channel_id, group_id, content_hash, updated_at) 
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (source_id, message.id, message.channel.id, group_id, content_hash))
                await db.commit()
                log.debug(f"Successfully added {source_id} to group {group_id}")
            except Exception as e:
                await db.rollback()
                log.error(f"Failed to add {source_id} to group {group_id}: {e}")
                raise
    except Exception as e:
        log.error(f"Database connection error in add_item_to_group: {e}")
        raise


async def get_group_items(group_id: str) -> list[str]:
    """Get all source_ids that belong to a group."""
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute("""
            SELECT source_id 
            FROM posts 
            WHERE group_id = ?
            ORDER BY created_at
        """, (group_id,))
        rows = await cursor.fetchall()
        return [row[0] for row in rows]


async def delete_post_record(source_id: str) -> None:
    """Delete a post record (when Discord message is deleted)."""
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM posts WHERE source_id = ?", (source_id,))
        await db.commit()


async def delete_group_record(group_id: str) -> None:
    """Delete a group record and all associated posts."""
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM posts WHERE group_id = ?", (group_id,))
        await db.execute("DELETE FROM groups WHERE group_id = ?", (group_id,))
        await db.commit()


def get_source_id_from_item(item: dict) -> str:
    """Extract source_id from item (URL preferred, fallback to title)."""
    return item.get('url', item.get('id', item.get('title', 'unknown')))


def generate_content_signature(item: dict) -> str:
    """Generate a stable content signature for change detection."""
    import hashlib
    import json
    
    # Extract only the relevant fields that affect embed content
    content_fields = {
        'title': item.get('title', ''),
        'content': item.get('content', ''),
        'location': item.get('location', ''),
        'price': item.get('price', ''),
        'rarity': item.get('rarity', ''),
        'image_url': item.get('image_url', ''),
        'url': item.get('url', '')
    }
    
    # Create a normalized JSON string
    content_json = json.dumps(content_fields, sort_keys=True, separators=(',', ':'))
    
    # Generate hash
    return hashlib.sha256(content_json.encode('utf-8')).hexdigest()


async def has_content_changed(source_id: str, item: dict) -> bool:
    """Check if item content has actually changed."""
    try:
        # Get current content signature
        current_signature = generate_content_signature(item)
        
        # Get stored signature from database
        async with aiosqlite.connect(DB) as db:
            cursor = await db.execute("""
                SELECT content_hash FROM posts WHERE source_id = ?
            """, (source_id,))
            result = await cursor.fetchone()
            
            if not result:
                # No stored hash, treat as new
                return True
            
            stored_signature = result[0]
            
            # Compare signatures
            if stored_signature != current_signature:
                # Update stored signature
                await db.execute("""
                    UPDATE posts SET content_hash = ? WHERE source_id = ?
                """, (current_signature, source_id))
                await db.commit()
                return True
            
            return False
            
    except Exception as e:
        log.error(f"Error checking content changes for {source_id}: {e}")
        # If error, assume content changed to be safe
        return True


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
    
    return f"<:aqwgift:1491402775009955950> __{current_weekday} Daily Gift__ <:aqwgift:1491402775009955950>"


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
    """
    Improved categorization with better keyword matching and fallback logic.
    """
    item_title = item.get("title", "Unknown")
    log.debug("Categorizing item: %s", item_title)

    # First, try breadcrumb extraction
    if "html_content" in item:
        try:
            breadcrumb_category = extract_breadcrumb_category(item["html_content"], item.get("url", ""))
            if breadcrumb_category != "No category found":
                log.info("✓ Category from breadcrumb: %s for %s", breadcrumb_category, item_title)
                return breadcrumb_category
        except Exception as e:
            log.error("✗ Error extracting breadcrumb category for %s: %s", item_title, e)

    # Enhanced keyword matching
    title = item.get("title", "").lower()
    content = item.get("content", "").lower()
    url = item.get("url", "").lower()

    # Combine all text for analysis
    all_text = f"{title} {content} {url}"

    # Enhanced keyword categories with more specific matches
    categories = {
        "Swords": {
            "keywords": ["sword", "blade", "saber", "katana", "rapier", "scimitar", "claymore", "longsword", "broadsword", "greatsword"],
            "weight": 3,
            "priority": 1
        },
        "Helm": {
            "keywords": ["helm", "helmet", "hood", "mask", "crown", "tiara", "circlet", "hat", "cap", "head", "skull", "visor", "coif", "headgear", "helmets"],
            "weight": 3,
            "priority": 2
        },
        "Axes": {
            "keywords": ["axe", "hatchet", "battleaxe", "cleaver", "splitter", "greataxe", "handaxe"],
            "weight": 3,
            "priority": 3
        },
        "Bows": {
            "keywords": ["bow", "archery", "crossbow", "longbow", "shortbow", "compound", "arrow", "quiver"],
            "weight": 3,
            "priority": 4
        },
        "Daggers": {
            "keywords": ["dagger", "knife", "shiv", "stiletto", "blade", "dirk", "poison", "assassin"],
            "weight": 3,
            "priority": 5
        },
        "Maces": {
            "keywords": ["mace", "club", "morningstar", "flail", "bludgeon", "hammer", "maul", "warhammer"],
            "weight": 3,
            "priority": 6
        },
        "Polearms": {
            "keywords": ["polearm", "spear", "lance", "pike", "halberd", "trident", "glaive", "staff"],
            "weight": 3,
            "priority": 7
        },
        "Guns": {
            "keywords": ["gun", "firearm", "pistol", "revolver", "shotgun", "rifle", "sniper", "bullet", "ammo"],
            "weight": 3,
            "priority": 8
        },
        "Wands": {
            "keywords": ["wand", "magic", "spell", "arcane", "mystic", "staff", "rod", "spellbook"],
            "weight": 3,
            "priority": 9
        },
        "Armor": {
            "keywords": ["armor", "armour", "plate", "mail", "chain", "scale", "leather", "cloth", "robe", "tunic", "vest", "chest", "breastplate", "cuirass", "defense", "protection"],
            "weight": 2,
            "priority": 10
        },
        "Cape": {
            "keywords": ["cape", "cloak", "mantle", "shawl", "wrap", "covering", "back", "shoulder", "scarf"],
            "weight": 2,
            "priority": 11
        },
        "Pet": {
            "keywords": ["pet", "companion", "familiar", "mount", "animal", "creature", "beast"],
            "weight": 2,
            "priority": 12
        }
    }

    # Calculate scores for each category
    best_category = "Miscellaneous"
    best_score = 0
    match_details = {}

    for category, config in categories.items():
        keywords = config["keywords"]
        weight = config["weight"]

        score = 0
        matched_keywords = []

        # Check each keyword
        for keyword in keywords:
            # Count occurrences in all text
            count = all_text.count(keyword)
            if count > 0:
                # Title matches get higher weight
                title_count = title.count(keyword)
                content_count = content.count(keyword)
                url_count = url.count(keyword)

                # Calculate weighted score
                keyword_score = (title_count * 5 + content_count * 2 + url_count * 1) * weight
                score += keyword_score

                if keyword_score > 0:
                    matched_keywords.append(f"{keyword}({keyword_score})")

        if score > 0:
            match_details[category] = {
                "score": score,
                "matches": matched_keywords
            }

            if score > best_score:
                best_score = score
                best_category = category

    # Log detailed matching info
    if best_score > 0:
        log.info("✓ Best match: %s (score: %d) for %s", 
                best_category, best_score, item_title)
        if match_details:
            log.debug("All matches: %s", match_details)
    else:
        # Final fallback: try URL-based categorization
        url = item.get("url", "").lower()
        url_category = extract_category_from_url(url, 
            ["Swords", "Helm", "Axes", "Bows", "Daggers", "Maces", "Polearms", "Guns", "Wands"],
            ["Armor", "Cape", "Pet"]
        )
        
        if url_category != "No category found":
            best_category = url_category
            log.info("✓ Category from URL fallback: %s for %s", best_category, item_title)
        else:
            log.warning("✗ No keyword matches found for %s", item_title)
            log.info("→ Defaulted to Misc category for %s", item_title)
    
    return best_category


def extract_location_from_content(content: str) -> str:
    """
    Extract location from item content with robust parsing and multiple fallback patterns.
    
    Args:
        content (str): The item content text
        
    Returns:
        str: Normalized location string
    """
    log.debug("Extracting location from content: %s...", content[:100] if content else "Empty content")
    
    # Primary pattern: __**Location:**__\nLocation Name
    patterns = [
        r"__\*\*Location:\*\*__\s*\n(.+?)(?=\n\n|\n__\*\*|$)",
        r"\*\*Location:\*\*\s*\n(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"Location:\s*\n(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"Location[:\s]+(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"From[:\s]+(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"Found[:\s]+(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"Area[:\s]+(.+?)(?=\n\n|\n__|\n\*\*|$)"
    ]
    
    for i, pattern in enumerate(patterns):
        match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
        if match:
            location = match.group(1).strip()
            normalized_location = normalize_string(location)
            log.debug("Pattern %d matched: '%s' → '%s'", i + 1, location, normalized_location)
            
            # Validate location is meaningful
            if len(normalized_location) > 2 and normalized_location not in ['unknown', 'na', 'none']:
                return normalized_location
            else:
                log.debug("Location '%s' seems invalid, trying next pattern", normalized_location)
        else:
            log.debug("Pattern %d failed to match", i + 1)
    
    # Fallback: Look for common location indicators in the content
    location_keywords = [
        'location:', 'from:', 'found:', 'area:', 'zone:', 'dropped by:', 'obtained from:',
        'reward from:', 'quest:', 'drop:', 'monster:', 'boss:', 'npc:', 'shop:', 'store:'
    ]
    
    lines = content.split('\n')
    for line_num, line in enumerate(lines):
        line_lower = line.lower().strip()
        for keyword in location_keywords:
            if keyword in line_lower:
                # Extract the part after the keyword
                parts = line_lower.split(keyword, 1)
                if len(parts) > 1:
                    location_part = parts[1].strip()
                    # Clean up common suffixes
                    location_part = re.sub(r'[:\.\!].*$', '', location_part).strip()
                    if len(location_part) > 2:
                        normalized_location = normalize_string(location_part)
                        log.debug("Fallback location found on line %d: '%s' → '%s'", 
                                line_num + 1, location_part, normalized_location)
                        return normalized_location
    
    log.warning("No valid location found in content")
    return "Unknown"


def extract_price_from_content(content: str) -> str:
    """
    Extract price from item content with robust parsing and multiple fallback patterns.
    
    Args:
        content (str): The item content text
        
    Returns:
        str: Normalized price string
    """
    log.debug("Extracting price from content: %s...", content[:100] if content else "Empty content")
    
    # Primary pattern: __**Price:**__\nPrice Amount
    patterns = [
        r"__\*\*Price:\*\*__\s*\n(.+?)(?=\n\n|\n__\*\*|$)",
        r"\*\*Price:\*\*\s*\n(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"Price:\s*\n(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"Price[:\s]+(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"Cost[:\s]+(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"Value[:\s]+(.+?)(?=\n\n|\n__|\n\*\*|$)",
        r"Sells?[:\s]+(.+?)(?=\n\n|\n__|\n\*\*|$)"
    ]
    
    for i, pattern in enumerate(patterns):
        match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
        if match:
            price = match.group(1).strip()
            normalized_price = normalize_string(price)
            log.debug("Price pattern %d matched: '%s' → '%s'", i + 1, price, normalized_price)
            
            # Validate price is meaningful
            if len(normalized_price) > 1 and normalized_price not in ['unknown', 'na', 'none', 'free']:
                return normalized_price
            else:
                log.debug("Price '%s' seems invalid, trying next pattern", normalized_price)
        else:
            log.debug("Price pattern %d failed to match", i + 1)
    
    # Fallback: Look for price indicators and currency
    price_patterns = [
        r'(\d+(?:,\d+)*(?:\.\d+)?)\s*(?:ac|gold|coins?|g|c)',
        r'(?:ac|gold|coins?|g|c)\s*[:\s]*(\d+(?:,\d+)*(?:\.\d+)?)',
        r'(?:free|no cost|0|n/a)',
        r'(?:reward|drop|quest|monster|boss)',
        r'(?:shop|store|buy|purchase)'
    ]
    
    lines = content.split('\n')
    for line_num, line in enumerate(lines):
        line_lower = line.lower()
        for pattern in price_patterns:
            match = re.search(pattern, line_lower)
            if match:
                if match.groups():
                    # Found a numeric price
                    price_value = match.group(1)
                    normalized_price = normalize_string(price_value + " AC")
                    log.debug("Fallback price found on line %d: '%s' → '%s'", 
                            line_num + 1, price_value, normalized_price)
                    return normalized_price
                else:
                    # Found a non-numeric price indicator
                    price_type = match.group(0)
                    normalized_price = normalize_string(price_type.title())
                    log.debug("Fallback price type found on line %d: '%s' → '%s'", 
                            line_num + 1, price_type, normalized_price)
                    return normalized_price
    
    log.warning("No valid price found in content")
    return "Unknown"


def validate_and_normalize_item_data(item: dict) -> dict:
    """
    Validate and normalize item data, ensuring all required fields are present and properly formatted.
    
    Args:
        item (dict): Raw item data
        
    Returns:
        dict: Validated and normalized item data
    """
    log.debug("Validating item: %s", item.get("title", "Unknown"))
    
    # Ensure required fields exist, preserving all original fields including pid and location
    normalized_item = {
        'title': item.get("title", "Unknown Item"),
        'url': item.get("url", ""),
        'content': item.get("content", ""),
        'price': item.get("price", ""),
        'rarity': item.get("rarity", ""),
        'image': item.get("image", ""),
        'images': item.get("images", []),
        'html_content': item.get("html_content", "")
    }
    
    # Preserve the pid field if it exists
    if 'pid' in item:
        normalized_item['pid'] = item['pid']
    
    # Preserve the location field if it exists
    if 'location' in item:
        normalized_item['location'] = item['location']
    
    # Preserve the category field if it exists
    if 'category' in item:
        normalized_item['category'] = item['category']
    
    # Validate title
    if not normalized_item['title'] or normalized_item['title'].strip() == "Unknown":
        log.warning("Item has invalid title: %s", normalized_item['title'])
        normalized_item['title'] = "Unknown Item"
    
    # Validate URL
    if not normalized_item['url']:
        log.warning("Item '%s' has no URL", normalized_item['title'])
    
    # Validate content
    if not normalized_item['content']:
        log.warning("Item '%s' has no content", normalized_item['title'])
        # Don't set "No content available" for items that are supposed to be empty (existing grouped items)
        # Only set it for items that should have content but don't
        if normalized_item.get('url'):  # Only if it has a URL (current item)
            normalized_item['content'] = "No content available"
    
    return normalized_item


def deduplicate_items(items: list[dict]) -> list[dict]:
    """Remove duplicate items based on URL, keeping the most complete version.
    
    This function deduplicates items by URL, preferring items with more complete data.
    If multiple items have the same URL, the one with the most non-empty fields is kept.
    As a fallback, it also deduplicates by title for items without URLs or with similar titles.
    
    Args:
        items (list[dict]): List of items to deduplicate
        
    Returns:
        list[dict]: Deduplicated list of items
        
    Example:
        >>> items = [
        ...     {'url': 'http://test.com/item', 'title': 'Item', 'content': ''},
        ...     {'url': 'http://test.com/item', 'title': 'Item', 'content': 'Full content'}
        ... ]
        >>> deduped = deduplicate_items(items)"""
    log.info("Deduplicating %d items", len(items))
    
    # First pass: Group by URL, but handle items without URLs separately
    url_groups = {}
    
    for item in items:
        url = item.get("url", "").strip()
        
        # For items without URLs, use title as the deduplication key
        if not url:
            key = f"no_url:{item.get('title', '')}"
        else:
            key = url
            
        if key not in url_groups:
            url_groups[key] = []
        url_groups[key].append(item)
    
    # Select best item for each group
    first_pass = []
    duplicates_removed = 0
    
    for key, duplicate_items in url_groups.items():
        if len(duplicate_items) == 1:
            first_pass.append(duplicate_items[0])
        else:
            # Find the most complete item (most non-empty fields)
            best_item = max(duplicate_items, key=lambda x: sum(1 for v in x.values() if v and str(v).strip()))
            first_pass.append(best_item)
            duplicates_removed += len(duplicate_items) - 1
            
            if key.startswith("no_url:"):
                log.debug("Deduplicated item without URL '%s': kept item with %d fields, discarded %d items", 
                         duplicate_items[0].get('title', 'Unknown')[:50], 
                         sum(1 for v in best_item.values() if v and str(v).strip()), 
                         len(duplicate_items) - 1)
    
    log.info("Deduplication complete: %d items -> %d items (removed %d duplicates)", 
             len(items), len(first_pass), duplicates_removed)
    
    return first_pass


def _item_completeness_score(item: dict) -> int:
    """
    Calculate a completeness score for an item to determine which one to keep.
    Higher score = more complete data.
    """
    score = 0
    field_weights = {
        'title': 10,
        'content': 8,
        'url': 6,
        'location': 4,
        'price': 4,
        'rarity': 3,
        'image': 2,
        'images': 2,
        'category': 1
    }
    
    for field, weight in field_weights.items():
        value = item.get(field)
        if value:
            if isinstance(value, str) and value.strip():
                score += weight
            elif isinstance(value, list) and value:
                score += weight
            elif isinstance(value, dict) and value:
                score += weight
            else:
                score += weight  # Non-empty, non-string value
    
    return score


def improved_group_items_by_location_price(items: list[dict]) -> dict[str, list[dict]]:
    """
    Improved grouping function with deduplication and stable hash generation.
    
    This function provides enhanced reliability by:
    1. Deduplicating items before grouping
    2. Using stable hash generation for consistent keys
    3. Providing comprehensive logging and statistics
    4. Handling edge cases gracefully
    
    Args:
        items (list[dict]): List of items to group
        
    Returns:
        dict[str, list[dict]]: Dictionary with stable hash keys as keys and lists of items as values
        
    Example:
        >>> groups = improved_group_items_by_location_price(items)
        >>> print(f"Created {len(groups)} groups")
        Created 3 groups
    """
    log.info("Starting improved grouping of %d items", len(items))
    
    # Step 1: Deduplicate items first
    deduplicated_items = deduplicate_items(items)
    
    # Step 2: Validate and normalize all items
    validated_items = []
    validation_stats = {
        'total': len(deduplicated_items),
        'valid': 0,
        'invalid': 0,
        'missing_url': 0,
        'missing_content': 0
    }
    
    for i, item in enumerate(deduplicated_items):
        try:
            validated_item = validate_and_normalize_item_data(item)
            validated_items.append(validated_item)
            validation_stats['valid'] += 1
            log.debug("Validated item %d: %s", i + 1, validated_item['title'])
        except Exception as e:
            validation_stats['invalid'] += 1
            log.error("Failed to validate item %d: %s", i + 1, e)
            continue
    
    log.info("Validation results: %d valid, %d invalid out of %d items", 
             validation_stats['valid'], validation_stats['invalid'], validation_stats['total'])
    
    # Step 3: Extract location and price with comprehensive logging
    item_data = []
    extraction_stats = {
        'location_success': 0,
        'location_failed': 0,
        'price_success': 0,
        'price_failed': 0
    }
    
    for i, item in enumerate(validated_items):
        log.debug("Processing item %d: %s", i + 1, item['title'])
        
        content = item.get("content", "")
        
        # For existing grouped items, preserve their original location/price from database
        log.debug("DEBUG: Processing item '%s': content_length=%d, url='%s', has_location=%s, has_price=%s", 
                 item['title'], len(content), item.get("url", "None"), 
                 bool(item.get("location")), bool(item.get("price")))
        
        # Check if this is an existing grouped item (no content, no URL, but has stored location/price)
        is_existing_grouped_item = (
            not content and 
            not item.get("url") and 
            (item.get("location") or item.get("price"))
        )
        
        if is_existing_grouped_item:
            # This is an existing grouped item without content - use database values
            location = item.get("location", "Unknown")
            price = item.get("price", "Unknown")
            log.debug("DEBUG: Using database location/price for existing item '%s': Location='%s', Price='%s'", 
                     item['title'], location, price)
        else:
            # This is a current item - extract from content
            location = "Unknown"
            price = "Unknown"
            log.debug("DEBUG: Extracting location/price from content for item '%s' (existing=%s)", 
                     item['title'], is_existing_grouped_item)
            
            # Extract location with robust parsing
            try:
                location = extract_location_from_content(content)
                if location != "Unknown":
                    extraction_stats['location_success'] += 1
                    log.debug("✓ Location extracted: '%s' for %s", location, item['title'])
                else:
                    extraction_stats['location_failed'] += 1
                    log.debug("✗ Failed to extract location for %s", item['title'])
            except Exception as e:
                extraction_stats['location_failed'] += 1
                log.debug("✗ Exception extracting location for %s: %s", item['title'], e)
                location = "Unknown"
            
            # Extract price with robust parsing
            try:
                price = extract_price_from_content(content)
                if price != "Unknown":
                    extraction_stats['price_success'] += 1
                    log.debug("✓ Price extracted: '%s' for %s", price, item['title'])
                else:
                    extraction_stats['price_failed'] += 1
                    log.warning("✗ Failed to extract price for %s", item['title'])
            except Exception as e:
                extraction_stats['price_failed'] += 1
                log.error("✗ Error extracting price for %s: %s", item['title'], e)
        
        # Store extracted data in both wrapper and item
        item['location'] = location
        item['price'] = price
        item_data.append({
            'item': item,
            'location': location,
            'price': price,
            'original_location': location,
            'original_price': price
        })
    
    # Log extraction statistics
    log.info("Extraction results - Location: %d success, %d failed | Price: %d success, %d failed",
             extraction_stats['location_success'], extraction_stats['location_failed'],
             extraction_stats['price_success'], extraction_stats['price_failed'])
    
    # Step 4: Group items by location and price
    groups_by_location_price = {}
    grouping_stats = {
        'total_groups': 0,
        'items_grouped': 0,
        'items_ungrouped': 0,
        'unknown_groups': 0,
        'skipped_unknown': 0
    }
    
    for i, data in enumerate(item_data):
        location = data['location']
        price = data['price']
        item_title = data['item']['title']
        
        # Skip items with unknown location/price ONLY if they're truly new items
        # Existing grouped items should be preserved even if they lose their content
        # An item is truly new if it has a URL (from recent changes) OR content but not both missing
        has_url_or_content = bool(data['item'].get('url')) or bool(data['item'].get('content'))
        is_existing_grouped_item = not has_url_or_content and location != "Unknown" and price != "Unknown"
        
        if (location == "Unknown" or price == "Unknown") and has_url_or_content and not is_existing_grouped_item:
            log.warning("Skipping new item '%s' with unknown location/price from grouping: Location='%s', Price='%s'", 
                       item_title, location, price)
            grouping_stats['skipped_unknown'] += 1
            continue
        
        # Create grouping key
        key = (location, price)
        
        # Add to appropriate group
        if key not in groups_by_location_price:
            groups_by_location_price[key] = []
            grouping_stats['total_groups'] += 1
            log.debug("Created new group: Location='%s', Price='%s'", location, price)
        
        groups_by_location_price[key].append(data['item'])
        grouping_stats['items_grouped'] += 1
        
        log.debug("Added item '%s' to group (Location='%s', Price='%s', Group size: %d)", 
                 item_title, location, price, len(groups_by_location_price[key]))
    
    # Log grouping statistics
    log.info("Grouping results - Total groups: %d, Items grouped: %d, Unknown groups: %d, Skipped unknown: %d",
             grouping_stats['total_groups'], grouping_stats['items_grouped'], 
             grouping_stats['unknown_groups'], grouping_stats['skipped_unknown'])
    
    # Step 5: Generate stable hash keys for each group
    final_groups = {}
    final_groups = {}
    hash_generation_stats = {
        'successful': 0,
        'failed': 0
    }
    
    for key, items_in_group in groups_by_location_price.items():
        try:
            # Extract location and price from the key tuple
            location, price = key
            # Generate stable hash key for entire group
            group_key_hash = generate_stable_group_key(location, price, items_in_group)
            final_groups[group_key_hash] = items_in_group
            hash_generation_stats['successful'] += 1
                
            log.debug("Generated stable hash key for group: Location='%s', Price='%s', Items=%d, Hash=%s",
                     location, price, len(items_in_group), group_key_hash[:8])
                
        except Exception as e:
            hash_generation_stats['failed'] += 1
            log.error("Failed to generate hash key for group (Location='%s', Price='%s'): %s",
                     location, price, e)
    
    # Log final statistics
    log.info("Final grouping results - Hash keys generated: %d successful, %d failed",
             hash_generation_stats['successful'], hash_generation_stats['failed'])
    log.info("Total groups created: %d from %d deduplicated items", 
             len(final_groups), len(deduplicated_items))
    
    return final_groups


def create_categorized_item_list(items: list[dict]) -> str:
    """Create a categorized list of items with custom emojis and no prices."""
    # Group items by detected type
    categorized = {}
    for item in items:
        # Detect item type from title
        item_type = detect_item_type_from_title(item.get('title', ''))
        if item_type not in categorized:
            categorized[item_type] = []
        categorized[item_type].append(item)
    
    # Build output with custom emojis
    sections = []
    
    # Process each category with custom emoji
    for item_type, type_items in categorized.items():
        emoji = get_item_type_emoji(item_type)
        
        # Format category name with emoji
        category_name = item_type.capitalize()
        sections.append(f"__**{emoji} {category_name}:**__")
        
        # Add items in this category
        for item in type_items:
            title = item.get("title", "Unknown")
            url = item.get("url", "")
            if url:
                sections.append(f"** [{title}]({url})")
            else:
                sections.append(f"** {title}")
        
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
        try:
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
                
        except discord.errors.InteractionResponded:
            # If interaction was already responded to, try to edit original response
            pass
        except Exception as e:
            log.error(f"Error in grouped pane button callback: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred while processing your request.", 
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "An error occurred while processing your request.", 
                        ephemeral=True
                    )
            except:
                pass  # If even this fails, just log and continue
    
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
            label=f"{get_category_form(category, len(items))} ({len(items)})",
            style=style,
            emoji=emoji,
            custom_id=f"category_{category.lower().replace(' ', '_')}"
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Send ephemeral message with items from this category."""
        try:
            # Acknowledge immediately to prevent timeout
            await interaction.response.defer(ephemeral=True)
            
            # Filter items for this category
            category_items = [item for item in self.items if categorize_item(item) == self.category]
            
            if not category_items:
                await interaction.followup.send(
                    f"No items found in {get_category_form(self.category, len(category_items))} category.",
                    ephemeral=True
                )
                return
            
            # Create embed for this category
            embed = discord.Embed(
                title=f"📂 {get_category_form(self.category, len(category_items))} ({len(category_items)} items)",
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
            
            # Send ephemeral message using followup since we already deferred
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        
        except discord.errors.InteractionResponded:
            # If interaction was already responded to, try to edit original response
            pass
        except Exception as e:
            log.error(f"Error in category button callback: {e}")
            try:
                await interaction.followup.send(
                    "An error occurred while processing your request.", 
                    ephemeral=True
                )
            except:
                pass  # If even this fails, just log and continue


class CategoryButtonsView(discord.ui.View):
    """Dynamic view with category buttons for grouped items using custom emojis."""
    def __init__(self, items: list[dict], location: str, timeout: float = None):
        super().__init__(timeout=timeout)
        self.items = items  # Structured items with name, image, price, location, rarity, type
        self.location = location
        
        # Group items by type for category buttons
        categories = {}
        for item in items:
            item_type = item.get('type', 'misc')
            if item_type not in categories:
                categories[item_type] = []
            categories[item_type].append(item)
        
        # Add category buttons (one per type, not per item)
        for item_type, type_items in categories.items():
            emoji = get_item_type_emoji(item_type)
            # Use category name instead of individual item name
            category_name = item_type.capitalize()
            button = ItemCategoryButton(item_type, type_items, self, emoji, category_name, len(type_items))
            self.add_item(button)


class ItemCategoryButton(discord.ui.Button):
    """Button for item category with custom emoji and ephemeral pagination."""
    def __init__(self, item_type: str, items: list[dict], view: CategoryButtonsView, 
                 emoji: str, category_name: str, count: int):
        self.item_type = item_type
        self.items = items
        self.view_ref = view
        
        # Parse the emoji string into a proper discord.PartialEmoji
        parsed_emoji = parse_discord_emoji(emoji)
        
        super().__init__(
            label=f"{category_name} ({count})",  # Category name with count
            style=discord.ButtonStyle.secondary,
            emoji=parsed_emoji
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Show ephemeral item viewer for this category."""
        await interaction.response.send_message(
            embed=self.create_item_embed(0),
            view=ItemPaginationView(self.items),
            ephemeral=True
        )
    
    def create_item_embed(self, index: int) -> discord.Embed:
        """Create embed for item at specific index."""
        if index >= len(self.items):
            return discord.Embed(title="No items", color=discord.Color.red())
        
        item = self.items[index]
        
        embed = discord.Embed(
            title=item.get('name', 'Unknown'),
            description=f"Item {index + 1} of {len(self.items)}",
            color=discord.Color.blue()
        )
        
        # Add fields
        if item.get('location') and item.get('location') != 'Unknown':
            embed.add_field(name="Location", value=item['location'], inline=True)
        
        if item.get('rarity') and item.get('rarity') != 'Unknown':
            embed.add_field(name="Rarity", value=item['rarity'], inline=True)
        
        if item.get('price') and item.get('price') != 'Unknown':
            embed.add_field(name="Price", value=item['price'], inline=True)
        
        # Add image if available
        if item.get('image'):
            embed.set_image(url=item['image'])
        
        return embed


class ItemPaginationView(discord.ui.View):
    """Ephemeral pagination view for items."""
    def __init__(self, items: list[dict], timeout: float = 180.0):
        super().__init__(timeout=timeout)
        self.items = items
        self.current_index = 0
        
        # Add navigation buttons
        self.add_item(PreviousButton(self))
        self.add_item(NextButton(self))
        self.add_item(CloseButton())
    
    def get_current_embed(self) -> discord.Embed:
        """Get embed for current item index."""
        if not self.items:
            return discord.Embed(title="No items available", color=discord.Color.red())
        
        item = self.items[self.current_index]
        
        embed = discord.Embed(
            title=item.get('name', 'Unknown'),
            description=f"Item {self.current_index + 1} of {len(self.items)}",
            color=discord.Color.blue()
        )
        
        # Add fields
        if item.get('location') and item.get('location') != 'Unknown':
            embed.add_field(name="Location", value=item['location'], inline=True)
        
        if item.get('rarity') and item.get('rarity') != 'Unknown':
            embed.add_field(name="Rarity", value=item['rarity'], inline=True)
        
        if item.get('price') and item.get('price') != 'Unknown':
            embed.add_field(name="Price", value=item['price'], inline=True)
        
        # Add image if available
        if item.get('image'):
            embed.set_image(url=item['image'])
        
        return embed


class PreviousButton(discord.ui.Button):
    """Button to go to previous item."""
    def __init__(self, view: ItemPaginationView):
        self.view_ref = view
        super().__init__(
            label="Previous",
            style=discord.ButtonStyle.secondary
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view_ref
        if view.current_index > 0:
            view.current_index -= 1
            await interaction.response.edit_message(
                embed=view.get_current_embed(),
                view=view
            )
        else:
            await interaction.response.defer()


class NextButton(discord.ui.Button):
    """Button to go to next item."""
    def __init__(self, view: ItemPaginationView):
        self.view_ref = view
        super().__init__(
            label="Next",
            style=discord.ButtonStyle.secondary
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view_ref
        if view.current_index < len(view.items) - 1:
            view.current_index += 1
            await interaction.response.edit_message(
                embed=view.get_current_embed(),
                view=view
            )
        else:
            await interaction.response.defer()


class CloseButton(discord.ui.Button):
    """Button to close ephemeral message."""
    def __init__(self):
        super().__init__(
            label="Close",
            style=discord.ButtonStyle.secondary
        )
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="Item viewer closed.",
            embed=None,
            view=None
        )


class EphemeralCategoryView(CategoryButtonsView):
    """View for ephemeral category messages with close button."""
    def __init__(self, items: list[dict], location: str, price: str, timeout: float = 600.0):
        super().__init__(items, location, price, timeout, include_close_button=True)


# ---------------- CATEGORY PLURALIZATION ----------------
def get_category_form(category: str, count: int = 1) -> str:
    """
    Get the correct singular or plural form of a category based on item count.
    
    This function dynamically returns the appropriate form (singular or plural)
    based on the number of items, handling irregular forms and edge cases.
    
    Args:
        category (str): The base category name (usually singular form)
        count (int): Number of items in the category (default: 1)
        
    Returns:
        str: The correctly formatted category name (singular or plural)
        
    Examples:
        >>> get_category_form("Weapon", 1)
        'Weapon'
        >>> get_category_form("Weapon", 3)
        'Weapons'
        >>> get_category_form("Helm", 1)
        'Helm'
        >>> get_category_form("Helm", 2)
        'Helms'
        >>> get_category_form("Pet", 1)
        'Pet'
        >>> get_category_form("Pet", 5)
        'Pets'
        >>> get_category_form("Misc", 1)
        'Miscellaneous'
        >>> get_category_form("Misc", 3)
        'Miscellaneous'
    """
    # Handle count-based singular/plural logic
    if count == 1:
        return get_singular_form(category)
    else:
        return get_plural_form(category)


def get_singular_form(category: str) -> str:
    """
    Get the singular form of a category name.
    
    This function converts plural categories back to their singular form,
    handling irregular forms and edge cases.
    
    Args:
        category (str): The category name (could be singular or plural)
        
    Returns:
        str: The singular form of the category
    """
    # Dictionary of plural-to-singular mappings for irregular forms
    plural_to_singular = {
        # Main categories
        "Weapons": "Weapon",
        "Armors": "Armor", 
        "Helms": "Helm", 
        "Capes": "Cape",
        "Pets": "Pet",
        "Miscellaneous": "Misc",  # Special case: keep as "Misc"
        
        # Weapon types (plural to singular)
        "Axes": "Axe",
        "Bows": "Bow",  
        "Daggers": "Dagger",
        "Gauntlets": "Gauntlet",
        "Guns": "Gun",
        "HandGuns": "HandGun",
        "Maces": "Mace",
        "Polearms": "Polearm",
        "Rifles": "Rifle",
        "Staffs": "Staff",
        "Swords": "Sword",
        "Wands": "Wand",
        "Whips": "Whip",
        
        # Armor & equipment
        "Shields": "Shield",
        "Gloves": "Glove",
        "Helmets": "Helmet",
        "Pauldrons": "Pauldron",
        "Greaves": "Greave",
        "Bracers": "Bracer",
        
        # Accessories
        "Rings": "Ring",
        "Amulets": "Amulet",
        "Necklaces": "Necklace",
        "Earrings": "Earring",
        "Belts": "Belt",
        "Cloaks": "Cloak",
        "Robes": "Robe",
        
        # Consumables
        "Potions": "Potion",
        "Scrolls": "Scroll",
        "Foods": "Food",
        "Drinks": "Drink",
        
        # Common English irregulars
        "Knives": "Knife",
        "Wolves": "Wolf",
        "Leaves": "Leaf",
        "Lives": "Life",
        "Wives": "Wife",
        "Thieves": "Thief",
        "Elves": "Elf",
        "Selves": "Self",
        "Shelves": "Shelf",
        "Loaves": "Loaf",
        "Halves": "Half",
        "Calves": "Calf",
        
        # Compound words
        "Footmen": "Footman",
        "Policemen": "Policeman",
        "Gentlemen": "Gentleman",
        "Women": "Woman",
        "Men": "Man",
        "Children": "Child",
        "People": "Person",
        "Mice": "Mouse",
        "Lice": "Louse",
        "Geese": "Goose",
        "Teeth": "Tooth",
        "Feet": "Foot",
        
        # Latin-derived plurals
        "Foci": "Focus",
        "Nuclei": "Nucleus",
        "Radii": "Radius",
        "Cacti": "Cactus",
        "Fungi": "Fungus",
        "Alumni": "Alumnus",
        "Syllabi": "Syllabus",
        "Analyses": "Analysis",
        "Theses": "Thesis",
        "Crises": "Crisis",
        "Phenomena": "Phenomenon",
        "Criteria": "Criterion",
        "Data": "Datum",
        "Media": "Medium",
        "Bacteria": "Bacterium",
        "Curricula": "Curriculum",
        "Memoranda": "Memorandum",
        "Millennia": "Millennium"
    }
    
    # Check if we have a direct mapping
    if category in plural_to_singular:
        return plural_to_singular[category]
    
    # Apply regular singularization rules for unknown categories
    if category.endswith('ies') and len(category) > 3:
        # Words ending in 'ies' often come from 'y' (cities → city)
        return category[:-3] + 'y'
    elif category.endswith('ves') and len(category) > 3:
        # Words ending in 'ves' often come from 'f' or 'fe' (wolves → wolf)
        return category[:-3] + 'f'
    elif category.endswith('es') and len(category) > 2:
        # Words ending in 'es' often come from simple nouns (boxes → box)
        return category[:-2]
    elif category.endswith('s') and len(category) > 1:
        # Simple plural: remove 's' (cats → cat)
        return category[:-1]
    
    # If no规则 applies, return as-is (might already be singular)
    return category


def get_plural_form(category: str) -> str:
    """
    Get the plural form of a category name.
    
    This function converts singular categories to their plural form,
    handling irregular forms and edge cases.
    
    Args:
        category (str): The singular category name
        
    Returns:
        str: The plural form of the category
    """
    # Comprehensive dictionary of irregular plurals for AQW and gaming categories
    # This is checked first for performance (O(1) lookup)
    IRREGULAR_PLURALS = {
        # === MAIN AQW CATEGORIES (Irregular Forms) ===
        "Weapon": "Weapons",
        "Armor": "Armors", 
        "Helm": "Helms", 
        "Cape": "Capes",
        "Pet": "Pets",
        "Misc": "Miscellaneous",
        
        # === WEAPON TYPES (Already Plural - No Change) ===
        "Axes": "Axes",
        "Bows": "Bows",  
        "Daggers": "Daggers",
        "Gauntlets": "Gauntlets",
        "Guns": "Guns",
        "HandGuns": "HandGuns",
        "Maces": "Maces",
        "Polearms": "Polearms",
        "Rifles": "Rifles",
        "Staffs": "Staffs",
        "Swords": "Swords",
        "Wands": "Wands",
        "Whips": "Whips",
        
        # === WEAPON TYPES (Singular to Plural) ===
        "Axe": "Axes",
        "Bow": "Bows",
        "Dagger": "Daggers",
        "Gauntlet": "Gauntlets",
        "Gun": "Guns",
        "HandGun": "HandGuns",
        "Mace": "Maces",
        "Polearm": "Polearms",
        "Rifle": "Rifles",
        "Staff": "Staffs",
        "Sword": "Swords",
        "Wand": "Wands",
        "Whip": "Whips",
        
        # === ARMOR & EQUIPMENT ===
        "Shield": "Shields",
        "Boots": "Boots",         # Already plural
        "Glove": "Gloves",
        "Gloves": "Gloves",       # Already plural
        "Helmet": "Helmets",
        "Pauldron": "Pauldrons",
        "Greaves": "Greaves",     # Already plural
        "Bracer": "Bracers",
        "Bracers": "Bracers",     # Already plural
        
        # === ACCESSORIES & ITEMS ===
        "Ring": "Rings",
        "Rings": "Rings",         # Already plural
        "Amulet": "Amulets",
        "Necklace": "Necklaces",
        "Earring": "Earrings",
        "Belt": "Belts",
        "Cloak": "Cloaks",
        "Robe": "Robes",
        
        # === CONSUMABLES ===
        "Potion": "Potions",
        "Scroll": "Scrolls",
        "Food": "Foods",
        "Drink": "Drinks",
        
        # === COMMON ENGLISH IRREGULARS (Gaming Context) ===
        "Knife": "Knives",
        "Wolf": "Wolves",
        "Leaf": "Leaves",
        "Life": "Lives",
        "Wife": "Wives",
        "Thief": "Thieves",
        "Elf": "Elves",
        "Self": "Selves",
        "Shelf": "Shelves",
        "Loaf": "Loaves",
        "Half": "Halves",
        "Calf": "Calves",
        
        # === SPECIAL CASES ===
        "Miscellaneous": "Miscellaneous",  # Already plural
        "Equipment": "Equipment",          # Uncountable noun
        "Loot": "Loot",                    # Uncountable noun
        "Gear": "Gear",                    # Uncountable noun
        "Furniture": "Furniture",          # Uncountable noun
        "Information": "Information",      # Uncountable noun
        "Knowledge": "Knowledge",          # Uncountable noun
        "Money": "Money",                  # Uncountable noun
        "News": "News",                    # Uncountable noun
        
        # === COMPOUND WORDS ===
        "Footman": "Footmen",
        "Policeman": "Policemen",
        "Gentleman": "Gentlemen",
        "Woman": "Women",
        "Man": "Men",
        "Child": "Children",
        "Person": "People",
        "Mouse": "Mice",
        "Louse": "Lice",
        "Goose": "Geese",
        "Tooth": "Teeth",
        "Foot": "Feet",
        
        # === LATIN-DERIVED PLURALS ===
        "Focus": "Foci",
        "Nucleus": "Nuclei",
        "Radius": "Radii",
        "Cactus": "Cacti",
        "Fungus": "Fungi",
        "Alumnus": "Alumni",
        "Syllabus": "Syllabi",
        "Analysis": "Analyses",
        "Thesis": "Theses",
        "Crisis": "Crises",
        "Phenomenon": "Phenomena",
        "Criterion": "Criteria",
        "Datum": "Data",
        "Medium": "Media",
        "Bacterium": "Bacteria",
        "Curriculum": "Curricula",
        "Memorandum": "Memoranda",
        "Millennium": "Millennia"
    }
    
    # === FAST PATH: Check irregular plurals dictionary ===
    # This handles 90% of cases with O(1) lookup performance
    if category in IRREGULAR_PLURALS:
        return IRREGULAR_PLURALS[category]
    
    # === REGULAR ENGLISH PLURALIZATION RULES ===
    # Applied only when category is not in irregular dictionary
    
    # Rule 1: Words ending in -s, -ss, -sh, -ch, -x, -z → add -es
    # Examples: class → classes, box → boxes, buzz → buzzes, witch → witches
    if category.endswith(('s', 'ss', 'sh', 'ch', 'x', 'z')):
        return category + 'es'
    
    # Rule 2: Words ending in -y
    # If preceded by consonant → change -y to -ies (city → cities)
    # If preceded by vowel → add -s (boy → boys)
    elif category.endswith('y') and len(category) > 1:
        if category[-2] not in 'aeiou':
            return category[:-1] + 'ies'
        else:
            return category + 's'
    
    # Rule 3: Words ending in -f → change -f to -ves
    # Examples: wolf → wolves, leaf → leaves
    elif category.endswith('f'):
        return category[:-1] + 'ves'
    
    # Rule 4: Words ending in -fe → change -fe to -ves
    # Examples: knife → knives, life → lives
    elif category.endswith('fe'):
        return category[:-2] + 'ves'
    
    # Rule 5: Words ending in -o
    # Most add -es, but some add -s (especially musical instruments, shortened words)
    # Examples: potato → potatoes, hero → heroes, but photo → photos, piano → pianos
    elif category.endswith('o'):
        # Common gaming and technical terms often just add -s
        gaming_terms = {'photo', 'piano', 'video', 'studio', 'radio', 'zoo'}
        if category.lower() in gaming_terms:
            return category + 's'
        elif len(category) > 1 and category[-2] not in 'aeiou':
            return category + 'es'
        else:
            return category + 's'
    
    # Rule 6: Words ending in -is → change -is to -es (Greek/Latin roots)
    # Examples: analysis → analyses, thesis → theses
    elif category.endswith('is'):
        return category[:-2] + 'es'
    
    # Rule 7: Words ending in -us → change -us to -i (Latin roots)
    # Examples: cactus → cacti, fungus → fungi
    elif category.endswith('us'):
        return category[:-2] + 'i'
    
    # Rule 8: Words ending in -on → change -on to -a (Greek roots)
    # Examples: phenomenon → phenomena, criterion → criteria
    elif category.endswith('on'):
        return category[:-2] + 'a'
    
    # === DEFAULT RULE: Add -s ===
    # This covers the majority of regular English nouns
    # Examples: cat → cats, dog → dogs, item → items
    else:
        return category + 's'


# Backward compatibility function
def pluralize_category(category: str) -> str:
    """
    Legacy function for backward compatibility.
    
    This function always returns the plural form of the category.
    For new code, use get_category_form(category, count) instead.
    
    Args:
        category (str): The category name to pluralize
        
    Returns:
        str: The plural form of the category
    """
    return get_plural_form(category)


def get_category_display_name(category: str, count: int = 1) -> str:
    """
    Get the display name for a category based on item count.
    
    This is the recommended function to use for displaying category names
    with correct singular/plural forms.
    
    Args:
        category (str): The base category name (usually singular form)
        count (int): Number of items in the category (default: 1)
        
    Returns:
        str: The correctly formatted category name (singular or plural)
        
    Examples:
        >>> get_category_display_name("Weapon", 1)
        'Weapon'
        >>> get_category_display_name("Weapon", 3)
        'Weapons'
        >>> get_category_display_name("Pet", 1)
        'Pet'
        >>> get_category_display_name("Pet", 5)
        'Pets'
    """
    return get_category_form(category, count)


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
async def create_grouped_embed(group_key: str, items: list[dict]) -> tuple[discord.Embed, discord.ui.View]:
    """Create a grouped embed with custom emojis and no prices."""
    # Extract location from first item
    if items:
        first_item = items[0]
        location = first_item.get("location", "Unknown")
    else:
        location = "Unknown"
    
    # Get daily gift number and generate title
    gift_number = await get_and_increment_counter("daily_gift")
    title = generate_daily_gift_title(gift_number)
    
    # Create categorized item list with custom emojis
    item_list = create_categorized_item_list(items)
    
    # Build description with new formatting
    description_parts = [
        f"__**Location:**__",
        location,
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
    
    embed.set_footer(text=f"AQW Daily Gift - {len(items)} items grouped")
    
    # Create structured items for the view
    structured_items = []
    for item in items:
        structured_items.append({
            'name': item.get('title', 'Unknown'),
            'image': item.get('image_url', item.get('image', '')),
            'price': item.get('price', 'Unknown'),
            'location': item.get('location', 'Unknown'),
            'rarity': item.get('rarity', 'Unknown'),
            'type': detect_item_type_from_title(item.get('title', ''))
        })
    
    # Create category buttons view for grouped items
    view = CategoryButtonsView(structured_items, location)
    
    return embed, view
    

async def delete_old_individual_messages(items: list[dict]):
    """Delete old individual messages for items that are now grouped with comprehensive debugging."""
    log.info("🗑️ OLD INDIVIDUAL MESSAGES DELETION START")
    log.info("  ├─ Items to check: %d", len(items))
    
    if not items:
        log.info("  └─ No items to process - exiting")
        return
    
    # Track deletion statistics
    deletion_stats = {
        'total_items': len(items),
        'items_with_messages': 0,
        'successful_deletions': 0,
        'failed_deletions': 0,
        'not_found': 0,
        'permission_denied': 0
    }
    
    # Log item details
    for i, item in enumerate(items, 1):
        item_title = item.get('title', 'Unknown')
        message_id = item.get('discord_message_id')
        channel_id = item.get('discord_channel_id')
        log.info("  ├─ Item %d: '%s'", i, item_title)
        log.info("  │  ├─ Message ID: %s", message_id if message_id else 'None')
        log.info("  │  └─ Channel ID: %s", channel_id if channel_id else 'None')
    
    deleted_count = 0
    not_found_count = 0
    forbidden_count = 0
    
    for i, item in enumerate(items, 1):
        pid = urlparse(item["url"]).path.strip("/").replace("/", "-") or item["url"]
        item_title = item.get("title", "Unknown")
        message_id = item.get('discord_message_id')
        channel_id = item.get('discord_channel_id')
        
        log.info("  ├─ Processing item %d: '%s'", i, item_title)
        log.info("  │  ├─ PID: %s", pid)
        log.info("  │  ├─ Message ID: %s", message_id if message_id else 'None')
        log.info("  │  └─ Channel ID: %s", channel_id if channel_id else 'None')
        
        # Update statistics
        if message_id:
            deletion_stats['items_with_messages'] += 1
        
        try:
            async with aiosqlite.connect(DB) as db:
                # Get the message ID for this item
                log.debug("  │  └─ Querying database for message info...")
                cursor = await db.execute("""
                    SELECT discord_message_id, discord_channel_id 
                    FROM items WHERE id=?
                """, (pid,))
                row = await cursor.fetchone()
                
                if row:
                    msg_id, ch_id = row
                    if msg_id and ch_id:
                        log.info("  │  ├─ Found message in database: %d in channel %d", msg_id, ch_id)
                        
                        # Check if this message ID is from a grouped post (don't delete grouped posts!)
                        cursor2 = await db.execute("""
                            SELECT discord_message_id FROM grouped_posts 
                            WHERE discord_message_id = ?
                        """, (msg_id,))
                        grouped_row = await cursor2.fetchone()
                        
                        if grouped_row:
                            log.info("  │  ├─ ℹ️ This is a grouped message - skipping deletion")
                            deletion_stats['not_found'] += 1  # Count as skipped
                            continue
                        
                        # Get the channel
                        channel = bot.get_channel(ch_id)
                        if channel:
                            try:
                                # Fetch and delete the message
                                log.debug("  │  │  └─ Fetching message %d...", msg_id)
                                msg = await channel.fetch_message(msg_id)
                                await msg.delete()
                                log.info("  │  ├─ ✅ Successfully deleted message")
                                deletion_stats['successful_deletions'] += 1
                            except discord.NotFound:
                                log.warning("  │  ├─ ℹ️ Message not found (already deleted)")
                                deletion_stats['not_found'] += 1
                            except discord.Forbidden:
                                log.error("  │  ├─ ❌ No permission to delete message")
                                deletion_stats['permission_denied'] += 1
                            except Exception as e:
                                log.error("  │  ├─ ❌ Error deleting message: %s", e)
                                deletion_stats['failed_deletions'] += 1
                        else:
                            log.warning("  │  ├─ ⚠️ Channel %d not found", ch_id)
                            deletion_stats['failed_deletions'] += 1
                    else:
                        log.info("  │  ├─ ℹ️ No message ID/channel ID stored")
                else:
                    log.info("  │  ├─ ℹ️ No database entry found")
                    
        except Exception as e:
            log.error("  │  └─ ❌ Database error: %s", e)
            deletion_stats['failed_deletions'] += 1
    
    # Log comprehensive deletion statistics
    log.info("  └─ 📊 DELETION STATISTICS:")
    log.info("     ├─ Total items processed: %d", deletion_stats['total_items'])
    log.info("     ├─ Items with messages: %d", deletion_stats['items_with_messages'])
    log.info("     ├─ Successfully deleted: %d", deletion_stats['successful_deletions'])
    log.info("     ├─ Not found: %d", deletion_stats['not_found'])
    log.info("     ├─ Permission denied: %d", deletion_stats['permission_denied'])
    log.info("     └─ Failed deletions: %d", deletion_stats['failed_deletions'])
    log.info("🗑️ OLD INDIVIDUAL MESSAGES DELETION END")
    log.info("🗑️ DELETE OLD MESSAGES DEBUG END")


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

async def migrate_item_hashes():
    """Migrate existing item hashes to the new ultra-stable hashing system."""
    try:
        async with aiosqlite.connect("drops.db") as db:
            cursor = await db.execute("""
                SELECT id, title, content, price, rarity, image, images, url, location, content_hash 
                FROM items 
                WHERE content_hash IS NOT NULL
            """)
            
            items = await cursor.fetchall()
            migrated_count = 0
            
            log.info("Starting hash migration to ultra-stable system for %d items...", len(items))
            
            for row in items:
                (id, title, content, price, rarity, image, images, url, location, old_hash) = row
                
                # Reconstruct item dict with all available data
                item = {
                    "title": title or "",
                    "content": content or "",
                    "price": price or "",
                    "rarity": rarity or "",
                    "image": image or "",
                    "images": json.loads(images) if images else [],
                    "url": url or "",
                    "location": location or ""
                }
                
                # Generate new ultra-stable hash
                new_hash = generate_content_hash(item)
                
                # Update if hash is different
                if new_hash != old_hash:
                    await db.execute("""
                        UPDATE items 
                        SET content_hash = ? 
                        WHERE id = ?
                    """, (new_hash, id))
                    migrated_count += 1
                    log.info("Migrated hash for item '%s': %s -> %s", title, old_hash[:16], new_hash[:16])
            
            await db.commit()
            log.info("Ultra-stable hash migration completed. Updated %d items.", migrated_count)
            
    except Exception as e:
        log.error("Error during ultra-stable hash migration: %s", e)


async def migrate_group_hashes():
    """Migrate existing group hashes to the new ultra-stable hashing system."""
    try:
        async with aiosqlite.connect("drops.db") as db:
            # Get ALL groups, not just those with hashes (since most have NULL hashes)
            cursor = await db.execute("""
                SELECT group_key, group_data, content_hash, location, price, item_titles
                FROM grouped_posts
            """)
            
            groups = await cursor.fetchall()
            migrated_count = 0
            
            log.info("Starting group hash migration to ultra-stable system for %d groups...", len(groups))
            
            for row in groups:
                (group_key, group_data, old_hash, location, price, item_titles) = row
                
                try:
                    # Reconstruct items from available data
                    items = []
                    
                    # Try to parse from group_data first
                    if group_data:
                        try:
                            items = json.loads(group_data)
                        except json.JSONDecodeError:
                            log.warning("Could not parse group_data for key %s, trying item_titles", group_key)
                    
                    # If no items from group_data, reconstruct from item_titles
                    if not items and item_titles:
                        try:
                            item_titles_list = json.loads(item_titles)
                            # Create minimal item dicts for hashing
                            items = [{"title": title, "content": "", "url": "", "location": location or "", "price": price or ""} 
                                   for title in item_titles_list]
                        except json.JSONDecodeError:
                            log.warning("Could not parse item_titles for key %s", group_key)
                    
                    if items:
                        # Generate new ultra-stable hash
                        new_hash = generate_group_content_hash(items)
                        
                        # Always update since we're migrating to the new system
                        await db.execute("""
                            UPDATE grouped_posts 
                            SET content_hash = ?, group_data = ?
                            WHERE group_key = ?
                        """, (new_hash, json.dumps(items), group_key))
                        migrated_count += 1
                        log.info("Migrated hash for group %s: %s items -> %s", group_key[:16], len(items), new_hash[:16])
                    else:
                        log.warning("No items found for group %s, skipping migration", group_key)
                        
                except Exception as e:
                    log.error("Error migrating group %s: %s", group_key, e)
            
            await db.commit()
            log.info("Ultra-stable group hash migration completed. Updated %d groups.", migrated_count)
            
    except Exception as e:
        log.error("Error during ultra-stable group hash migration: %s", e)


async def has_item_changed(pid: str, new_item: dict) -> bool:
    """Check if item has changed since last posting."""
    stored = await get_stored_item(pid)
    if not stored:
        log.debug("CHANGE DEBUG: Item '%s' not found in database - treating as new", pid)
        return True  # New item
    
    new_hash = generate_content_hash(new_item)
    stored_hash = stored["content_hash"]
    
    # Debug logging for hash comparison
    log.debug("CHANGE DEBUG: Item '%s' | Stored hash: %s | New hash: %s | Changed: %s", 
              new_item.get('title', 'Unknown'), stored_hash[:8], new_hash[:8], new_hash != stored_hash)
    
    # Check if hash is different
    if new_hash == stored_hash:
        return False  # No change
    
    # Hash is different, but check if we recently updated this item to avoid spam
    last_updated = stored.get("last_updated")
    if last_updated:
        try:
            # Parse the timestamp from the stored item
            if isinstance(last_updated, str):
                from datetime import datetime
                last_update_time = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                time_since_update = datetime.now(timezone.utc) - last_update_time
                
                # Only allow updates every 5 minutes to avoid spam
                if time_since_update.total_seconds() < 300:  # 5 minutes
                    log.info("Item %s changed but skipping update (last updated %s ago)", 
                            new_item.get("title", "Unknown"), 
                            f"{int(time_since_update.total_seconds())}s")
                    return False
        except Exception as e:
            log.debug("Error parsing last_updated timestamp: %s", e)
    
    # Debug logging to understand why hashes might differ
    log.info("Item %s hash mismatch:", new_item.get("title", "Unknown"))
    log.info("  Stored hash: %s", stored_hash)
    log.info("  New hash: %s", new_hash)
    log.info("  Stored title: %s", stored.get("title", "None"))
    log.info("  New title: %s", new_item.get("title", "None"))
    log.info("  Stored content length: %d", len(stored.get("content", "")))
    log.info("  New content length: %d", len(new_item.get("content", "")))
    
    return True

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
            (id, url, title, content, price, rarity, image, images, last_updated, content_hash, discord_message_id, discord_channel_id, location) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?)
        """, (
            pid, item.get("url"), item.get("title"), item.get("content"), 
            item.get("price"), item.get("rarity"), item.get("image"), 
            json.dumps(item.get("images", [])), content_hash, message_id, channel_id,
            item.get("location")
        ))
        await db.commit()


async def update_discord_message_info(pid: str, message_id: int, channel_id: int):
    """Update Discord message info for an existing item."""
    log.debug("Updating discord_message_info: pid=%s, message_id=%s, channel_id=%s", pid, message_id, channel_id)
    
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            UPDATE items SET discord_message_id=?, discord_channel_id=?, last_updated=datetime('now')
            WHERE id=?
        """, (message_id, channel_id, pid))
        
        await db.commit()
        log.debug("Successfully updated discord_message_info for pid=%s", pid)


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


def generate_stable_group_key(location: str, price: str, items: list[dict]) -> str:
    """
    Generate a highly stable and consistent hash key for a group of items.
    This key should be identical for the same logical group regardless of item order
    or minor variations in the items themselves.
    
    The key is generated based on:
    1. Normalized location
    2. Normalized price
    
    Note: We DON'T include items in the key generation because we want the same
    group key to be used for updates when items are added/removed from the group.
    Change detection is handled separately by content hashing.
    
    Args:
        location (str): The normalized location string
        price (str): The normalized price string  
        items (list[dict]): List of items in the group (not used for key generation)
        
    Returns:
        str: Stable MD5 hash key for the group
        
    Example:
        >>> items = [{'url': 'http://test.com/item1'}, {'url': 'http://test.com/item2'}]
        >>> key = generate_stable_group_key('Location A', '100 AC', items)
        >>> len(key)  # Always 32 characters (MD5)
        32
    """
    log.debug("Generating stable group key for location='%s', price='%s'", location, price)
    
    # Multi-layer normalization for maximum stability
    norm_location = normalize_string(location).strip()
    norm_price = normalize_string(price).strip()
    
    # Create deterministic string with clear separators
    # Format: location|price
    combined_string = f"{norm_location}|{norm_price}"
    
    # Generate hash
    import hashlib
    hash_key = hashlib.md5(combined_string.encode('utf-8')).hexdigest()
    
    log.debug("Generated group key: %s (location='%s', price='%s')", hash_key[:8], location, price)
    return hash_key


async def atomic_check_and_store_group(group_key: str, location: str, price: str, items: list[dict], 
                                      message_id: int = None, channel_id: int = None) -> tuple[bool, str]:
    """Atomically check if group exists and store/update it in a single transaction.
    
    This function ensures thread-safe operations by using database locks and transactions.
    It returns whether the operation was successful and the action taken.
    
    Args:
        group_key (str): The stable hash key for the group
        location (str): Group location
        price (str): Group price
        items (list[dict]): Items in the group
        message_id (int, optional): Discord message ID
        channel_id (int, optional): Discord channel ID
        
    Returns:
        tuple[bool, str]: (success, action_taken) where action_taken is 'new', 'updated', or 'exists'
        
    Example:
        >>> success, action = await atomic_check_and_store_group(
        ...     'abc123', 'Location A', '100 AC', items, 12345, 67890
        ... )
        >>> print(f"Group {action} successfully")
        Group new successfully
    """
    log.debug("Starting atomic group operation for key: %s", group_key[:8])
    
    # Extract item data for storage
    item_titles = [item.get("title", "") for item in items]
    categories = list(set([categorize_item(item) for item in items]))  # Unique categories
    
    # Generate content hash for change detection
    content_hash = generate_group_content_hash(items)
    categories_with_hash = [f"hash:{content_hash}"] + categories
    
    async with aiosqlite.connect(DB) as db:
        # Use immediate lock for atomic operation
        await db.execute("BEGIN IMMEDIATE")
        try:
            # Check if group exists
            async with db.execute(
                "SELECT group_key, item_titles, categories FROM grouped_posts WHERE group_key=?", 
                (group_key,)
            ) as cur:
                existing_row = await cur.fetchone()
            
            if existing_row:
                # Group exists - check if it changed
                stored_titles = json.loads(existing_row[1]) if existing_row[1] else []
                stored_categories = json.loads(existing_row[2]) if existing_row[2] else []
                
                # Extract stored hash if present
                stored_hash = None
                for category in stored_categories:
                    if category.startswith("hash:"):
                        stored_hash = category[5:]  # Remove "hash:" prefix
                        break
                
                # Check for changes
                titles_changed = set(stored_titles) != set(item_titles)
                hash_changed = stored_hash != content_hash
                
                if titles_changed or hash_changed:
                    # Update existing group
                    await db.execute("""
                        UPDATE grouped_posts 
                        SET item_titles=?, categories=?, discord_message_id=?, discord_channel_id=?, last_updated=datetime('now')
                        WHERE group_key=?
                    """, (
                        json.dumps(item_titles), json.dumps(categories_with_hash),
                        message_id, channel_id, group_key
                    ))
                    
                    await db.commit()
                    log.info("✅ Updated existing group %s: titles_changed=%s, hash_changed=%s", 
                            group_key[:8], titles_changed, hash_changed)
                    return True, "updated"
                else:
                    # No changes needed
                    await db.commit()
                    log.debug("Group %s already exists and unchanged", group_key[:8])
                    return True, "exists"
            else:
                # New group - insert it
                await db.execute("""
                    INSERT INTO grouped_posts 
                    (group_key, location, price, item_titles, categories, discord_message_id, discord_channel_id, last_updated) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """, (
                    group_key, location, price, json.dumps(item_titles), json.dumps(categories_with_hash),
                    message_id, channel_id
                ))
                
                await db.commit()
                log.info("✅ Created new group %s with %d items", group_key[:8], len(items))
                return True, "new"
                
        except Exception as e:
            await db.rollback()
            log.error("❌ Atomic group operation failed for key %s: %s", group_key[:8], e)
            raise


async def get_group_change_details(group_key: str, current_items: list[dict]) -> dict | None:
    """Get detailed information about what changed in a group.
    
    This function compares the current items with stored group data to provide
    detailed change information for logging and debugging.
    
    Args:
        group_key (str): The group hash key
        current_items (list[dict]): Current items in the group
        
    Returns:
        dict | None: Detailed change information or None if group not found
        
    Example:
        >>> changes = await get_group_change_details('abc123', items)
        >>> if changes:
        ...     print(f"Added: {changes['added']}, Removed: {changes['removed']}")
    """
    stored_group = await get_stored_group(group_key)
    if not stored_group:
        return None
    
    current_titles = set(item.get("title", "") for item in current_items)
    stored_titles = set(stored_group.get("item_titles", []))
    
    added_titles = current_titles - stored_titles
    removed_titles = stored_titles - current_titles
    
    return {
        'group_key': group_key,
        'stored_count': len(stored_titles),
        'current_count': len(current_titles),
        'added': sorted(list(added_titles)),
        'removed': sorted(list(removed_titles)),
        'total_changes': len(added_titles) + len(removed_titles)
    }


async def get_stored_group(group_key: str) -> dict | None:
    """Get stored group data for comparison."""
    async with aiosqlite.connect(DB) as db:
        async with db.execute("""
            SELECT group_key, location, price, item_titles, categories, discord_message_id, discord_channel_id 
            FROM grouped_posts WHERE group_key=?
        """, (group_key,)) as cur:
            row = await cur.fetchone()
            if row:
                categories = json.loads(row[4]) if row[4] else []
                
                # Extract content hash from categories (first entry with "hash:" prefix)
                content_hash = None
                actual_categories = []
                for category in categories:
                    if category.startswith("hash:"):
                        content_hash = category[5:]  # Remove "hash:" prefix
                    else:
                        actual_categories.append(category)
                
                return {
                    "group_key": row[0],
                    "location": row[1],
                    "price": row[2],
                    "item_titles": json.loads(row[3]) if row[3] else [],
                    "categories": actual_categories,  # Return actual categories without hash
                    "content_hash": content_hash,  # Return extracted hash
                    "discord_message_id": row[5],
                    "discord_channel_id": row[6]
                }
            return None


async def get_items_in_grouped_message(message_id: int) -> list[dict]:
    """Get all items that belong to a specific grouped message."""
    log.debug("Querying items for discord_message_id=%s", message_id)
    
    async with aiosqlite.connect(DB) as db:
        async with db.execute("""
            SELECT id, url, title, content, price, rarity, image, images, content_hash
            FROM items WHERE discord_message_id=?
        """, (message_id,)) as cur:
            rows = await cur.fetchall()
            log.debug("Found %d rows for discord_message_id=%s", len(rows), message_id)
            
            items = []
            for row in rows:
                # Generate pid from the stored id (which is the pid)
                pid = row[0]
                try:
                    images_data = json.loads(row[7]) if row[7] else []
                except (json.JSONDecodeError, TypeError):
                    images_data = []
                    
                item_data = {
                    "pid": pid,
                    "id": row[0],
                    "url": row[1],
                    "title": row[2],
                    "content": row[3],
                    "price": row[4],
                    "rarity": row[5],
                    "image": row[6],
                    "images": images_data,
                    "content_hash": row[8]
                }
                items.append(item_data)
                log.debug("Found item: %s (pid: %s)", item_data["title"], pid)
            
            log.debug("Returning %d items for message %s", len(items), message_id)
            return items


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
    """Delete a group post record with comprehensive debugging."""
    log.info("🗑️ GROUP POST DELETION START")
    log.info("  ├─ Group key: %s", group_key[:16] + "..." if len(group_key) > 16 else group_key)
    
    try:
        # First check if the group exists
        async with aiosqlite.connect(DB) as db:
            async with db.execute("SELECT location, price, item_titles FROM grouped_posts WHERE group_key = ?", (group_key,)) as cur:
                existing_group = await cur.fetchone()
                
                if existing_group:
                    location, price, item_titles = existing_group
                    log.info("  ├─ Found existing group:")
                    log.info("  │  ├─ Location: '%s'", location)
                    log.info("  │  ├─ Price: '%s'", price)
                    log.info("  │  └─ Items: %s", item_titles)
                else:
                    log.warning("  ├─ Group not found in database")
                    log.info("  └─ Deletion completed (nothing to delete)")
                    return
        
        # Perform the deletion
        async with aiosqlite.connect(DB) as db:
            cursor = await db.execute("DELETE FROM grouped_posts WHERE group_key=?", (group_key,))
            rows_affected = cursor.rowcount
            await db.commit()
            
            log.info("  ├─ Database operation completed")
            log.info("  ├─ Rows affected: %d", rows_affected)
            
            if rows_affected > 0:
                log.info("  └─ ✅ Group post successfully deleted")
            else:
                log.warning("  └─ ⚠️ No rows were affected - group may have been already deleted")
                
    except Exception as e:
        log.error("  ├─ ❌ Error during group post deletion")
        log.error("  └─ Exception: %s", e)
        raise


def normalize_value(obj):
    """Recursively normalize any value to a consistent string representation."""
    if obj is None:
        return "null"
    elif isinstance(obj, bool):
        return "true" if obj else "false"
    elif isinstance(obj, (int, float)):
        return str(obj)
    elif isinstance(obj, str):
        # Ultra-normalization for strings
        import re
        # Remove all whitespace variations
        normalized = re.sub(r'\s+', ' ', obj.strip())
        # Normalize unicode
        normalized = normalized.encode('utf-8', 'ignore').decode('utf-8')
        # Convert to lowercase for case-insensitive hashing where appropriate
        return normalized
    elif isinstance(obj, list):
        # Sort and normalize list items
        try:
            normalized_items = [normalize_value(item) for item in obj]
            return "|" + "|".join(sorted(normalized_items)) + "|"
        except:
            # Fallback for unsortable items
            return "|" + "|".join(normalized_items) + "|"
    elif isinstance(obj, dict):
        # Sort dictionary keys and normalize values
        normalized_items = []
        for key in sorted(obj.keys()):
            normalized_items.append(f"{normalize_value(key)}:{normalize_value(obj[key])}")
        return "{" + ",".join(normalized_items) + "}"
    else:
        # Fallback for other types
        return str(obj)


def generate_stable_hash(data: any) -> str:
    """Generate an ultra-stable hash that handles all data types consistently."""
    import hashlib
    import json
    
    # Create a normalized representation
    normalized_data = normalize_value(data)
    
    # Generate hash using multiple algorithms for maximum stability
    sha256_hash = hashlib.sha256(normalized_data.encode('utf-8')).hexdigest()
    md5_hash = hashlib.md5(normalized_data.encode('utf-8')).hexdigest()
    
    # Combine both hashes for ultimate reliability
    combined = f"sha256:{sha256_hash}|md5:{md5_hash}"
    final_hash = hashlib.sha256(combined.encode('utf-8')).hexdigest()
    
    return final_hash


def extract_content_signature(item: dict) -> dict:
    """Extract a comprehensive content signature that captures all meaningful data."""
    signature = {}
    
    # Core identifiers (always included)
    signature['title'] = item.get('title', '').strip()
    signature['url'] = item.get('url', '').strip()
    
    # Content extraction with multiple fallbacks
    content = item.get('content', '').strip()
    if not content:
        # Try to extract from other fields
        for field in ['description', 'summary', 'text']:
            if field in item and item[field]:
                content = str(item[field]).strip()
                break
    
    signature['content'] = content
    
    # Location extraction (robust)
    location = item.get('location', '').strip()
    if not location and content:
        # Extract location from content if missing
        try:
            from urllib.parse import urlparse
            import re
            
            # Try to extract location from content patterns
            location_patterns = [
                r'__\*\*Location:\*\*__\s*([^\n]+)',
                r'Location:\s*([^\n]+)',
                r'Place:\s*([^\n]+)',
                r'Area:\s*([^\n]+)'
            ]
            
            for pattern in location_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    location = match.group(1).strip()
                    break
        except:
            pass
    
    signature['location'] = location
    
    # Price extraction (robust)
    price = item.get('price', '').strip()
    if not price and content:
        # Extract price from content if missing
        try:
            import re
            price_patterns = [
                r'__\*\*Price:\*\*__\s*([^\n]+)',
                r'Price:\s*([^\n]+)',
                r'Cost:\s*([^\n]+)',
                r'(\d+\s*AC)',
                r'(\d+\s*Gold)'
            ]
            
            for pattern in price_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    price = match.group(1).strip()
                    break
        except:
            pass
    
    signature['price'] = price
    
    # Rarity extraction (robust)
    rarity = item.get('rarity', '').strip()
    if not rarity and content:
        # Extract rarity from content if missing
        try:
            import re
            rarity_patterns = [
                r'__\*\*Rarity:\*\*__\s*([^\n]+)',
                r'Rarity:\s*([^\n]+)',
                r'Rare\s+(\w+)',
                r'(\w+)\s+Rare'
            ]
            
            for pattern in rarity_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    rarity = match.group(1).strip()
                    break
        except:
            pass
    
    signature['rarity'] = rarity
    
    # Media content (normalized)
    image = item.get('image', '').strip()
    images = item.get('images', [])
    
    if isinstance(images, str):
        try:
            images = json.loads(images)
        except:
            images = [images]
    
    # Normalize image URLs
    if image:
        signature['image'] = image
    else:
        signature['image'] = ''
    
    # Sort and normalize image list
    if isinstance(images, list):
        normalized_images = []
        for img in images:
            if isinstance(img, str):
                normalized_images.append(img.strip())
            elif isinstance(img, dict) and 'url' in img:
                normalized_images.append(img['url'].strip())
        signature['images'] = sorted(set(normalized_images))  # Remove duplicates and sort
    else:
        signature['images'] = []
    
    # Additional metadata (optional, but normalized)
    for field in ['category', 'type', 'tags', 'metadata']:
        if field in item and item[field]:
            signature[field] = normalize_value(item[field])
    
    return signature


def generate_content_hash(item: dict) -> str:
    """Generate an ultra-stable content hash that prevents all current and future issues."""
    try:
        # Extract comprehensive content signature
        signature = extract_content_signature(item)
        
        # Generate stable hash
        content_hash = generate_stable_hash(signature)
        
        # Debug logging (minimal but informative)
        log.info("HASH DEBUG: Item '%s' -> %s", signature.get('title', 'Unknown'), content_hash[:16])
        
        return content_hash
        
    except Exception as e:
        log.error("Error generating content hash for item '%s': %s", item.get('title', 'Unknown'), e)
        # Ultimate fallback - hash only the title and URL
        fallback_data = {
            'title': item.get('title', '').strip(),
            'url': item.get('url', '').strip()
        }
        return generate_stable_hash(fallback_data)


def generate_group_content_hash(items: list[dict]) -> str:
    """Generate an ultra-stable hash for a group of items."""
    try:
        if not items:
            return generate_stable_hash("empty_group")
        
        # Extract signatures for all items
        item_signatures = []
        for item in items:
            signature = extract_content_signature(item)
            item_signatures.append(signature)
        
        # Sort items deterministically by URL, then title, then content hash
        def sort_key(sig):
            url = sig.get('url', '')
            title = sig.get('title', '')
            content_hash = generate_stable_hash(sig.get('content', ''))
            return (url.lower(), title.lower(), content_hash)
        
        sorted_signatures = sorted(item_signatures, key=sort_key)
        
        # Create group structure
        group_structure = {
            'items': sorted_signatures,
            'count': len(sorted_signatures),
            'method': 'stable_group_hash_v2'
        }
        
        # Generate stable hash
        group_hash = generate_stable_hash(group_structure)
        
        log.info("GROUP HASH DEBUG: %d items -> %s", len(items), group_hash[:16])
        
        return group_hash
        
    except Exception as e:
        log.error("Error generating group hash: %s", e)
        # Ultimate fallback - hash only item count and titles
        try:
            titles = sorted([item.get('title', '').strip() for item in items])
            fallback_data = {
                'titles': titles,
                'count': len(items),
                'fallback': True
            }
            return generate_stable_hash(fallback_data)
        except:
            return generate_stable_hash(f"emergency_fallback_{len(items)}")


async def has_group_changed(group_key: str, items: list[dict]) -> tuple[bool, dict | None]:
    """
    Improved group change detection with better logging and hash comparison.
    """
    log.info("Checking group change for key: %s (%d items)", group_key[:8], len(items))
    
    # Get stored group
    stored_group = await get_stored_group(group_key)
    
    if not stored_group:
        log.warning("No stored group found for key: %s - creating new group", group_key[:8])
        return True, None
    
    # Generate current hash
    current_hash = generate_group_content_hash(items)
    stored_hash = stored_group.get("content_hash")
    
    # Log comparison details
    log.info("Hash comparison - Current: %s, Stored: %s", 
             current_hash[:8], stored_hash[:8] if stored_hash else "None")
    
    # Log current items
    current_titles = sorted([item.get("title", "Unknown") for item in items])
    stored_titles = sorted(stored_group.get("item_titles", []))
    
    log.info("Current items: %s", current_titles)
    log.info("Stored items: %s", stored_titles)
    
    # Check if hashes match
    if stored_hash is None:
        log.warning("No stored hash found - assuming group has changed")
        return True, stored_group
    
    if stored_hash != current_hash:
        log.info("Hashes differ - group has changed")
        
        # Find what changed
        current_set = set(current_titles)
        stored_set = set(stored_titles)
        
        added = current_set - stored_set
        removed = stored_set - current_set
        
        if added:
            log.info("Items added: %s", list(added))
        if removed:
            log.info("Items removed: %s", list(removed))
        
        return True, stored_group
    
    log.info("Hashes match - group unchanged")
    return False, stored_group


async def update_stored_group_data(group_key: str, location: str, price: str, items: list[dict], 
                                 message_id: int = None, channel_id: int = None):
    """
    Update stored group data with current items and message info.
    Stores the content hash for future change detection.
    """
    # Extract item titles and categories
    item_titles = [item.get("title", "") for item in items]
    categories = list(set([categorize_item(item) for item in items]))  # Unique categories
    
    # Generate and store content hash
    content_hash = generate_group_content_hash(items)
    # Store hash as first category entry with "hash:" prefix
    categories_with_hash = [f"hash:{content_hash}"] + categories
    
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            INSERT OR REPLACE INTO grouped_posts 
            (group_key, location, price, item_titles, categories, discord_message_id, discord_channel_id, last_updated) 
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            group_key, location, price, json.dumps(item_titles), json.dumps(categories_with_hash),
            message_id, channel_id
        ))
        await db.commit()


async def check_message_exists(msg_id: int, ch_id: int) -> bool:
    """Check if a Discord message exists without raising exceptions."""
    channel = bot.get_channel(ch_id)
    if not channel:
        return False
    
    try:
        await channel.fetch_message(msg_id)
        return True
    except discord.NotFound:
        return False
    except discord.Forbidden:
        return False
    except Exception:
        return False


async def edit_existing_group_message(channel, stored_group: dict, group_key: str, 
                                    current_items: list[dict]) -> bool:
    """
    Edit an existing grouped message with improved error handling and retry logic.
    Returns True if successful, False otherwise.
    """
    max_retries = 3
    retry_delay = 1  # seconds
    
    log.info("🔧 EDIT MESSAGE DEBUG START")
    log.info("  - Max retries: %d", max_retries)
    log.info("  - Current items count: %d", len(current_items))
    
    for attempt in range(max_retries):
        log.info("  - Attempt %d/%d", attempt + 1, max_retries)
        
        try:
            # Get stored message info
            msg_id = stored_group.get("discord_message_id")
            ch_id = stored_group.get("discord_channel_id")
            
            log.info("    - Stored Message ID: %s", msg_id)
            log.info("    - Stored Channel ID: %s", ch_id)
            
            if not msg_id or not ch_id:
                log.warning("    ❌ Missing message info for stored group %s", group_key[0][:8])
                return False
            
            # Get the target channel
            target_channel = bot.get_channel(ch_id)
            if not target_channel:
                log.warning("    ❌ Channel %s not found for updating group message", ch_id)
                return False
            
            log.info("    ✅ Target channel found")
            
            # Fetch the existing message
            try:
                log.info("    - Fetching existing message...")
                existing_msg = await target_channel.fetch_message(msg_id)
                log.info("    ✅ Found existing message %s for group %s", msg_id, group_key[0][:8])
                log.info("    - Message created: %s", existing_msg.created_at)
                log.info("    - Message content length: %d", len(existing_msg.content) if existing_msg.content else 0)
                log.info("    - Message embeds count: %d", len(existing_msg.embeds))
            except discord.NotFound:
                log.warning("    ❌ Existing message %s not found for group %s in channel %s - message may have been deleted", 
                           msg_id, group_key[0][:8], ch_id)
                return False
            except discord.Forbidden:
                log.error("    ❌ No permission to fetch message %s", msg_id)
                return False
            
            # Get existing items from the grouped message
            log.info("    - Retrieving existing items from message...")
            existing_items = await get_items_in_grouped_message(msg_id)
            log.info("    ✅ Found %d existing items in grouped message %s", len(existing_items), msg_id)
            
            if existing_items:
                existing_titles = [item.get("title", "Unknown") for item in existing_items]
                log.info("    - Existing items: %s", existing_titles)
            
            # Merge existing items with current items to preserve all items
            # Use a dictionary to deduplicate by URL
            all_items_dict = {}
            
            # Add existing items first
            log.info("    - Merging items...")
            for item in existing_items:
                all_items_dict[item.get("url", "")] = item
            
            # Add/overwrite with current items (they have more up-to-date data)
            for item in current_items:
                all_items_dict[item.get("url", "")] = item
            
            # Convert back to list
            all_items = list(all_items_dict.values())
            
            log.info("    ✅ Merged items: %d existing + %d current = %d total", 
                    len(existing_items), len(current_items), len(all_items))
            
            if all_items:
                merged_titles = [item.get("title", "Unknown") for item in all_items]
                log.info("    - Merged items list: %s", merged_titles)
            
            # Create updated embed and view
            log.info("    - Creating updated embed and view...")
            # Extract location and price from first item since group_key is now a string hash
            if all_items:
                first_item = all_items[0]
                location = first_item.get("location", "Unknown")
                price = first_item.get("price", "Unknown")
            else:
                location = "Unknown"
                price = "Unknown"
            updated_embed, updated_view = await create_grouped_embed(group_key, all_items)
            
            # Edit the message
            log.info("    - Editing Discord message...")
            await existing_msg.edit(embed=updated_embed, view=updated_view)
            log.info("    ✅ Successfully edited grouped message %s with %d items", msg_id, len(all_items))
            
            # Update stored data with all items
            log.info("    - Updating stored group data...")
            group_key_hash = generate_stable_group_key(location, price, all_items)
            await update_stored_group_data(group_key_hash, location, price, all_items, msg_id, ch_id)
            
            # Update all items in group to reference the grouped message
            log.info("    - Updating item message references...")
            async with aiosqlite.connect(DB) as db:
                for item in all_items:
                    pid = urlparse(item["url"]).path.strip("/").replace("/", "-") or item["url"]
                    await db.execute("""
                        UPDATE items SET discord_message_id=?, discord_channel_id=?, last_updated=datetime('now')
                        WHERE id=?
                    """, (msg_id, ch_id, pid))
                await db.commit()
            
            log.info("    ✅ All item references updated")
            log.info("🔧 EDIT MESSAGE DEBUG END (SUCCESS)")
            return True
            
        except discord.HTTPException as e:
            if e.status == 429:  # Rate limited
                wait_time = e.retry_after if hasattr(e, 'retry_after') else retry_delay
                log.warning("    ⚠️ Rate limited when editing group message, waiting %d seconds (attempt %d/%d)", 
                          wait_time, attempt + 1, max_retries)
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait_time)
                    continue
            else:
                log.error("    ❌ HTTP error editing group message (attempt %d/%d): %s", 
                         attempt + 1, max_retries, e)
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    continue
        except Exception as e:
            log.error("    ❌ Unexpected error editing group message (attempt %d/%d): %s", 
                     attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
    
    log.warning("❌ All retry attempts failed")
    return False


async def post_individual_item(channel, item: dict) -> bool:
    """Post a single item with robust duplicate prevention and change detection."""
    try:
        log.info("Processing individual item: '%s'", item['title'])
        
        # Get source ID from item
        source_id = get_source_id_from_item(item)
        log.debug(f"Source ID: {source_id}")
        
        # Check if item already exists
        existing = await get_post(source_id)
        
        if existing:
            log.debug(f"Found existing post for {source_id}")
            
            # Check if content actually changed
            if not await has_content_changed(source_id, item):
                log.info(f"Item '{item['title']}' unchanged - skipping")
                return True  # Return True to indicate successful processing for cursor advancement
            
            # Content changed - try to edit existing message
            try:
                channel_obj = channel.guild.get_channel(existing['channel_id']) if hasattr(channel, 'guild') else channel
                if not channel_obj:
                    log.warning(f"Channel {existing['channel_id']} not found, treating as new post")
                    existing = None
                else:
                    message = await channel_obj.fetch_message(existing['message_id'])
                    
                    # Create new embed with updated content
                    embed, view = await create_pane_embed(item)
                    
                    # Edit existing message
                    await message.edit(embed=embed, view=view)
                    
                    # Update content hash in database
                    content_hash = generate_content_signature(item)
                    await save_single_post(source_id, message, content_hash)
                    
                    log.info(f"Updated individual item '{item['title']}' (Message ID: {message.id})")
                    return True
                    
            except discord.NotFound:
                log.warning(f"Message {existing['message_id']} not found, cleaning up and treating as new")
                await delete_post_record(source_id)
                existing = None
            except discord.Forbidden:
                log.warning(f"No permission to edit message for '{item['title']}'")
                return False
        
        if not existing:
            # Create new single post
            log.info(f"Creating new post for '{item['title']}'")
            embed, view = await create_pane_embed(item)
            message = await channel.send(embed=embed, view=view)
            
            # Save the mapping with content hash
            content_hash = generate_content_signature(item)
            await save_single_post(source_id, message, content_hash)
            
            log.info(f"Posted new individual item '{item['title']}' (Message ID: {message.id})")
            
            # Update cursor AFTER successful processing
            if item.get('page_url') and item.get('change_time'):
                update_cursor_after_successful_processing(item['page_url'], item['change_time'])
            
            return True
            
    except Exception as e:
        log.error(f"Error processing individual item '{item.get('title', 'Unknown')}': {e}")
        import traceback
        traceback.print_exc()
        return False


async def post_grouped_items(channel, group_id: str, items: list[dict]) -> bool:
    """Post grouped items with smart merging and change detection."""
    try:
        log.info(f"Processing grouped items: {len(items)} items in group '{group_id}'")
        
        # Check if group already exists
        existing_group = await get_group(group_id)
        
        if existing_group:
            log.debug(f"Found existing group for {group_id}")
            
            # Get existing items from the group
            existing_items = await get_group_items(group_id)
            log.info(f"Group has {len(existing_items)} existing items")
            
            # Merge new items with existing items
            merged_items = merge_group_items(existing_items, items)
            
            # Generate content signature for merged group
            group_content = {
                'items': [
                    {
                        'title': item.get('title', ''),
                        'content': item.get('content', ''),
                        'location': item.get('location', ''),
                        'price': item.get('price', ''),
                        'rarity': item.get('rarity', ''),
                        'image_url': item.get('image_url', ''),
                        'url': item.get('url', '')
                    }
                    for item in merged_items
                ]
            }
            import json
            import hashlib
            group_json = json.dumps(group_content, sort_keys=True, separators=(',', ':'))
            group_content_hash = hashlib.sha256(group_json.encode('utf-8')).hexdigest()
            
            # Check if merged group content actually changed
            if existing_group.get('content_hash') == group_content_hash:
                log.info(f"Group '{group_id}' unchanged after merge - skipping")
                return False
            
            log.info(f"Group '{group_id}' changed - updating with {len(merged_items)} total items")
            
            # Content changed - try to edit existing message
            try:
                channel_obj = channel.guild.get_channel(existing_group['channel_id']) if hasattr(channel, 'guild') else channel
                if not channel_obj:
                    log.warning(f"Channel {existing_group['channel_id']} not found, treating as new group")
                    existing_group = None
                else:
                    message = await channel_obj.fetch_message(existing_group['message_id'])
                    
                    # Create new grouped embed with merged items
                    embed, view = await create_grouped_embed(group_id, merged_items)
                    
                    # Edit existing message
                    await message.edit(embed=embed, view=view)
                    
                    # Update group content hash
                    await save_group(group_id, message, group_content_hash)
                    
                    # Update all item mappings to point to this message
                    for item in merged_items:
                        source_id = get_source_id_from_item(item)
                        item_content_hash = generate_content_signature(item)
                        await add_item_to_group(source_id, group_id, message, item_content_hash)
                    
                    log.info(f"Updated group '{group_id}' (Message ID: {message.id})")
                    return True
                    
            except discord.NotFound:
                log.warning(f"Group message {existing_group['message_id']} not found, cleaning up and treating as new")
                await delete_group_record(group_id)
                existing_group = None
            except discord.Forbidden:
                log.warning(f"No permission to edit group message for '{group_id}'")
                return False
        
        if not existing_group:
            # Create new group post
            log.info(f"Creating new group post for '{group_id}'")
            embed, view = await create_grouped_embed(group_id, items)
            message = await channel.send(embed=embed, view=view)
            
            # Generate content hash for new group
            group_content = {
                'items': [
                    {
                        'title': item.get('title', ''),
                        'content': item.get('content', ''),
                        'location': item.get('location', ''),
                        'price': item.get('price', ''),
                        'rarity': item.get('rarity', ''),
                        'image_url': item.get('image_url', ''),
                        'url': item.get('url', '')
                    }
                    for item in items
                ]
            }
            import json
            import hashlib
            group_json = json.dumps(group_content, sort_keys=True, separators=(',', ':'))
            group_content_hash = hashlib.sha256(group_json.encode('utf-8')).hexdigest()
            
            # Save the group with content hash
            await save_group(group_id, message, group_content_hash)
            
            # Save all item mappings with their content hashes
            for item in items:
                source_id = get_source_id_from_item(item)
                item_content_hash = generate_content_signature(item)
                await add_item_to_group(source_id, group_id, message, item_content_hash)
            
            log.info(f"Posted new group '{group_id}' (Message ID: {message.id})")
            return True
            
    except Exception as e:
        log.error(f"Error processing group '{group_id}': {e}")
        import traceback
        traceback.print_exc()
        return False


def merge_group_items(existing_items: list[str], new_items: list[dict]) -> list[dict]:
    """Merge existing items with new items, avoiding duplicates."""
    # Convert existing items to a set for quick lookup
    existing_source_ids = set()
    for source_id in existing_items:
        existing_source_ids.add(source_id)
    
    # Start with existing items (we need to fetch their full data)
    merged_items = []
    
    # For now, just use new items (existing items will be added in future iterations)
    # This is a simplified merge - in production, you'd fetch full item data for existing items
    merged_items.extend(new_items)
    
    # Remove any duplicates from new items
    unique_items = []
    seen_source_ids = set()
    
    for item in merged_items:
        source_id = get_source_id_from_item(item)
        if source_id not in seen_source_ids:
            unique_items.append(item)
            seen_source_ids.add(source_id)
    
    log.info(f"Merged {len(existing_items)} existing + {len(new_items)} new = {len(unique_items)} unique items")
    return unique_items


def get_group_key(item: dict) -> str:
    """Generate stable group key from location and price only."""
    location = item.get('location', 'unknown')
    price = item.get('price', 'unknown')
    
    # Normalize values for consistent keys
    def normalize(text):
        if not text:
            return 'unknown'
        return str(text).lower().replace(' ', '_').replace('/', '_').replace('\\', '_').replace('-', '_')
    
    normalized_location = normalize(location)
    normalized_price = normalize(price)
    
    return f"{normalized_location}_{normalized_price}"


def get_group_id_from_items(items: list[dict]) -> str:
    """Generate a consistent group_id from items (legacy function - use get_group_key instead)."""
    if items:
        first_item = items[0]
        return get_group_key(first_item)
    return f"group_{len(items)}"


async def process_item(item: dict, channel) -> bool:
    """Process a single item - handles new, same group, and group changed cases."""
    source_id = get_source_id_from_item(item)
    new_group_key = get_group_key(item)
    
    log.info(f"[ITEM] Processing source_id={source_id}, group_key={new_group_key}")
    
    # Serialize item data for storage
    import json
    new_data = json.dumps(item, sort_keys=True, separators=(',', ':'))
    
    async with aiosqlite.connect(DB) as db:
        async with db.execute("BEGIN IMMEDIATE"):
            try:
                # Check if item exists
                cursor = await db.execute("""
                    SELECT group_key, last_data FROM posts 
                    WHERE source_id = ?
                """, (source_id,))
                existing_item = await cursor.fetchone()
                
                if not existing_item:
                    # CASE 1: NEW ITEM
                    log.info(f"[DB] New item detected for source_id={source_id}")
                    
                    # Insert new item
                    await db.execute("""
                        INSERT INTO posts 
                        (source_id, group_key, last_data, content_hash) 
                        VALUES (?, ?, ?, ?)
                    """, (source_id, new_group_key, new_data, generate_content_signature(item)))
                    
                    # Rebuild the group
                    success = await rebuild_group(new_group_key, channel)
                    if success:
                        log.info(f"[POST] Created group for new item source_id={source_id}")
                    return success
                
                old_group_key, old_data = existing_item
                
                if old_group_key == new_group_key:
                    # CASE 2: EXISTING ITEM, SAME GROUP
                    if new_data == old_data:
                        log.info(f"[DB] Item unchanged source_id={source_id}")
                        return True  # Still counts as processed
                    
                    log.info(f"[UPDATE] Item content changed source_id={source_id}")
                    
                    # Update item data
                    await db.execute("""
                        UPDATE posts SET 
                        last_data = ?, 
                        content_hash = ?, 
                        updated_at = CURRENT_TIMESTAMP 
                        WHERE source_id = ?
                    """, (new_data, generate_content_signature(item), source_id))
                    
                    # Rebuild the group
                    success = await rebuild_group(new_group_key, channel)
                    if success:
                        log.info(f"[UPDATE] Rebuilt group for updated item source_id={source_id}")
                    return success
                
                else:
                    # CASE 3: EXISTING ITEM, GROUP CHANGED (PRICE CHANGE)
                    log.info(f"[MOVE] Item moved from {old_group_key} to {new_group_key} source_id={source_id}")
                    
                    # Update item's group key and data
                    await db.execute("""
                        UPDATE posts SET 
                        group_key = ?, 
                        last_data = ?, 
                        content_hash = ?, 
                        updated_at = CURRENT_TIMESTAMP 
                        WHERE source_id = ?
                    """, (new_group_key, new_data, generate_content_signature(item), source_id))
                    
                    # Rebuild both groups
                    old_success = await rebuild_group(old_group_key, channel)
                    new_success = await rebuild_group(new_group_key, channel)
                    
                    if old_success and new_success:
                        log.info(f"[MOVE] Successfully moved item source_id={source_id}")
                    elif not old_success:
                        log.info(f"[DELETE] Old group {old_group_key} was empty and removed")
                    elif not new_success:
                        log.error(f"[MOVE] Failed to create new group {new_group_key}")
                    
                    return new_success
                
            except Exception as e:
                log.error(f"[ITEM] Error processing item {source_id}: {e}")
                await db.rollback()
                return False


async def rebuild_group(group_key: str, channel) -> bool:
    """Rebuild a group with all current items - handles creation, update, and deletion."""
    log.info(f"[GROUP] Rebuilding group_key={group_key}")
    
    async with aiosqlite.connect(DB) as db:
        async with db.execute("BEGIN IMMEDIATE"):
            try:
                # Get all items for this group
                cursor = await db.execute("""
                    SELECT source_id, last_data FROM posts 
                    WHERE group_key = ? 
                    ORDER BY created_at
                """, (group_key,))
                items_data = await cursor.fetchall()
                
                if not items_data:
                    log.info(f"[DELETE] Removing empty group={group_key}")
                    
                    # Get group message info before deletion
                    cursor = await db.execute("""
                        SELECT message_id, channel_id FROM groups 
                        WHERE group_key = ?
                    """, (group_key,))
                    group_info = await cursor.fetchone()
                    
                    if group_info:
                        message_id, channel_id = group_info
                        try:
                            # Delete Discord message
                            channel_obj = channel.guild.get_channel(channel_id) if hasattr(channel, 'guild') else channel
                            if channel_obj:
                                message = await channel_obj.fetch_message(message_id)
                                await message.delete()
                                log.info(f"[DELETE] Removed Discord message {message_id} for group={group_key}")
                        except Exception as e:
                            log.warning(f"[DELETE] Failed to delete Discord message {message_id}: {e}")
                    
                    # Delete group from database
                    await db.execute("DELETE FROM groups WHERE group_key = ?", (group_key,))
                    await db.commit()
                    return False
                
                # Parse items data
                items = []
                for source_id, last_data in items_data:
                    if last_data:
                        try:
                            import json
                            item_data = json.loads(last_data)
                            items.append(item_data)
                        except json.JSONDecodeError as e:
                            log.error(f"[DB] Failed to parse item data for {source_id}: {e}")
                            continue
                
                if not items:
                    log.warning(f"[GROUP] No valid items found for group={group_key}")
                    return False
                
                # Check if group message already exists
                cursor = await db.execute("""
                    SELECT message_id, channel_id FROM groups 
                    WHERE group_key = ?
                """, (group_key,))
                existing_group = await cursor.fetchone()
                
                # Create grouped embed
                embed, view = await create_grouped_embed(group_key, items)
                
                if existing_group:
                    # Update existing message
                    message_id, channel_id = existing_group
                    try:
                        channel_obj = channel.guild.get_channel(channel_id) if hasattr(channel, 'guild') else channel
                        if not channel_obj:
                            log.warning(f"[UPDATE] Channel {channel_id} not found for group={group_key}")
                            return False
                        
                        message = await channel_obj.fetch_message(message_id)
                        await message.edit(embed=embed, view=view)
                        log.info(f"[UPDATE] Edited group message {message_id} for group={group_key}")
                        
                        # Update group record
                        await db.execute("""
                            UPDATE groups SET 
                            content_hash = ?, 
                            updated_at = CURRENT_TIMESTAMP 
                            WHERE group_key = ?
                        """, (generate_content_signature({'items': items}), group_key))
                        
                    except discord.NotFound:
                        log.warning(f"[UPDATE] Group message {message_id} not found, creating new one")
                        await db.execute("DELETE FROM groups WHERE group_key = ?", (group_key,))
                        existing_group = None
                    except discord.Forbidden:
                        log.error(f"[UPDATE] No permission to edit group message {message_id}")
                        return False
                
                if not existing_group:
                    # Create new message
                    message = await channel.send(embed=embed, view=view)
                    log.info(f"[POST] Created new group message {message.id} for group={group_key}")
                    
                    # Store group record
                    await db.execute("""
                        INSERT OR REPLACE INTO groups 
                        (group_key, message_id, channel_id, content_hash) 
                        VALUES (?, ?, ?, ?)
                    """, (group_key, message.id, channel.id, generate_content_signature({'items': items})))
                
                await db.commit()
                return True
                
            except Exception as e:
                log.error(f"[GROUP] Error rebuilding group={group_key}: {e}")
                await db.rollback()
                return False


async def get_recently_processed_items(hours: int = 24) -> set[str]:
    """Get set of source IDs processed in the last N hours to prevent startup reposts."""
    try:
        async with aiosqlite.connect(DB) as db:
            cursor = await db.execute("""
                SELECT source_id FROM posts 
                WHERE created_at > datetime('now', '-{} hours')
                UNION
                SELECT p.source_id FROM posts p
                JOIN groups g ON p.group_id = g.group_id
                WHERE g.created_at > datetime('now', '-{} hours')
            """.format(hours, hours))
            rows = await cursor.fetchall()
            return {row[0] for row in rows}
    except Exception as e:
        log.error(f"Error getting recently processed items: {e}")
        return set()


async def is_startup_safe(item: dict, recently_processed: set[str]) -> bool:
    """Check if it's safe to process an item during startup (avoid reposts)."""
    source_id = get_source_id_from_item(item)
    
    # If item was processed recently, skip it during startup
    if source_id in recently_processed:
        log.debug(f"Skipping recently processed item: {source_id}")
        return False
    
    return True


def get_startup_safeguard_hours() -> int:
    """Get hours for startup safeguard - longer during first minutes after restart."""
    # You can adjust this based on your bot's polling frequency
    return 2  # Skip items processed in last 2 hours


# ==================== CURSOR-BASED SCRAPER STATE ====================

def get_last_seen_change_sync() -> str | None:
    """Sync version of get_last_seen_change for use in sync contexts."""
    try:
        import sqlite3
        with sqlite3.connect(DB) as conn:
            cursor = conn.execute("""
                SELECT value FROM scraper_state WHERE key = 'last_seen_change'
            """)
            result = cursor.fetchone()
            return result[0] if result and result[0] else None
    except Exception as e:
        log.error(f"Error getting last_seen_change (sync): {e}")
        return None


def update_last_seen_change_sync(change_id: str) -> None:
    """Sync version of update_last_seen_change for use in sync contexts."""
    try:
        import sqlite3
        with sqlite3.connect(DB) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("""
                    UPDATE scraper_state 
                    SET value = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE key = 'last_seen_change'
                """, (change_id,))
                conn.commit()
                log.debug(f"Updated last_seen_change to: {change_id}")
            except Exception as e:
                conn.rollback()
                log.error(f"Error updating last_seen_change (sync): {e}")
                raise
    except Exception as e:
        log.error(f"Database connection error in update_last_seen_change_sync: {e}")
        raise


async def get_last_seen_change() -> str | None:
    """Get the last seen change identifier from database."""
    try:
        async with aiosqlite.connect(DB) as db:
            cursor = await db.execute("""
                SELECT value FROM scraper_state WHERE key = 'last_seen_change'
            """)
            result = await cursor.fetchone()
            return result[0] if result and result[0] else None
    except Exception as e:
        log.error(f"Error getting last_seen_change: {e}")
        return None


async def update_last_seen_change(change_id: str) -> None:
    """Update the last seen change identifier."""
    try:
        async with aiosqlite.connect(DB) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute("""
                    UPDATE scraper_state 
                    SET value = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE key = 'last_seen_change'
                """, (change_id,))
                await db.commit()
                log.debug(f"Updated last_seen_change to: {change_id}")
            except Exception as e:
                await db.rollback()
                log.error(f"Error updating last_seen_change (async): {e}")
                raise
    except Exception as e:
        log.error(f"Database connection error in update_last_seen_change: {e}")
        raise


def generate_change_id(page_url: str, timestamp: str) -> str:
    """Generate a unique change identifier from page URL and timestamp."""
    import hashlib
    combined = f"{page_url}|{timestamp}"
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()


def extract_timestamp_from_change(change_entry: dict) -> str:
    """Extract timestamp from a recent changes entry."""
    # Try to get timestamp from various fields
    timestamp_fields = ['timestamp', 'date', 'time', 'created', 'updated']
    
    for field in timestamp_fields:
        if field in change_entry and change_entry[field]:
            return str(change_entry[field])
    
    # Fallback to current time if no timestamp found
    return datetime.now(timezone.utc).isoformat()


async def is_change_processed(change_id: str, last_seen: str) -> bool:
    """Check if a change has already been processed based on cursor position."""
    if not last_seen:
        # No previous state, assume this is first run
        return False
    
    # For cursor-based tracking, we need to compare change IDs
    # This is a simplified check - in practice, you'd want to compare
    # based on the actual ordering from the recent changes page
    return change_id == last_seen


async def process_grouped_items(channel, group_key: str, items_in_group: list[dict]) -> bool:
    """Process grouped items with detailed debug logging."""
    # group_key is now a string hash, extract location and price from first item
    if items_in_group:
        first_item = items_in_group[0]
        location = first_item.get("location", "Unknown")
        price = first_item.get("price", "Unknown")
    else:
        location = "Unknown"
        price = "Unknown"
    
    # Comprehensive debugging start
    log.info("🚀 GROUPED POST PROCESSING START")
    log.info("  ├─ Location: '%s'", location)
    log.info("  ├─ Price: '%s'", price)
    log.info("  ├─ Items count: %d", len(items_in_group))
    log.info("  ├─ Channel: %s (ID: %d)", channel.name, channel.id)
    
    # Log item details
    item_titles = [item.get('title', 'Unknown') for item in items_in_group]
    log.info("  ├─ Item titles: %s", ', '.join(item_titles))
    
    # Generate group key for database operations
    group_key_hash = generate_stable_group_key(location, price, items_in_group)
    log.info("  ├─ Generated group key hash: %s", group_key_hash[:16] + "...")
    
    # Check if group already exists
    log.info("  └─ Checking if group already exists...")
    stored_group = await get_stored_group(group_key_hash)
    
    # Generate stable group key
    group_key_hash = generate_stable_group_key(location, price, items_in_group)
    
    # Log group details for debugging
    item_titles = [item.get("title", "Unknown") for item in items_in_group]
    log.info("=" * 80)
    log.info("🔍 GROUP PROCESS DEBUG START")
    log.info("=" * 80)
    log.info("Group Key: %s", group_key_hash[:8])
    log.info("Location: '%s'", location)
    log.info("Price: '%s'", price)
    log.info("Items Count: %d", len(items_in_group))
    log.info("Items: %s", item_titles)
    log.info("-" * 80)
    
    # Check if group has changed
    log.info("📊 Step 1: Checking if group has changed...")
    has_changed, stored_group = await has_group_changed(group_key_hash, items_in_group)
    
    # Log stored group details if found
    if stored_group:
        log.info("📋 Found stored group data:")
        log.info("  - Stored Message ID: %s", stored_group.get("discord_message_id", "None"))
        log.info("  - Stored Channel ID: %s", stored_group.get("discord_channel_id", "None"))
        log.info("  - Stored Items: %s", stored_group.get("item_titles", []))
        log.info("  - Stored Hash: %s", stored_group.get("content_hash", "None")[:8] if stored_group.get("content_hash") else "None")
        log.info("  - Last Updated: %s", stored_group.get("last_updated", "Unknown"))
    else:
        log.info("📋 No stored group found - this is a new group")
    
    # If group exists, check if the Discord message still exists
    if stored_group:
        log.info("🔍 Step 2: Checking if Discord message still exists...")
        msg_id = stored_group.get("discord_message_id")
        ch_id = stored_group.get("discord_channel_id")
        if msg_id and ch_id:
            log.info("  - Checking message ID: %s in channel: %s", msg_id, ch_id)
            message_exists = await check_message_exists(msg_id, ch_id)
            log.info("  - Message exists: %s", "✅ Yes" if message_exists else "❌ No")
            
            if not message_exists:
                log.warning("⚠️ Discord message %s no longer exists!", msg_id)
                log.info("→ Will create new group message")
                # Clear the stored message ID to force creation of new message
                stored_group["discord_message_id"] = None
                # Since message doesn't exist, we need to create a new one
                has_changed = True
        else:
            log.warning("⚠️ Stored group has no message ID or channel ID")
            has_changed = True
    
    log.info("-" * 80)
    log.info("📊 Decision: %s", 
            "Group unchanged - skipping" if (not has_changed and stored_group) else
            "Update existing group" if (has_changed and stored_group) else
            "Create new group")
    log.info("-" * 80)
    
    if not has_changed and stored_group:
        log.info("✅ Group unchanged, skipping: %s (%d items)", group_key_hash[:8], len(items_in_group))
        log.info("=" * 80)
        log.info("🔍 GROUP PROCESS DEBUG END")
        log.info("=" * 80)
        return False
    elif stored_group:
        log.info("🔄 Group changed, updating existing: %s (%d items)", group_key_hash[:8], len(items_in_group))
    else:
        log.info("🆕 New group, creating: %s (%d items)", group_key_hash[:8], len(items_in_group))
    
    if stored_group:
        log.info("📝 Step 3: Attempting to update existing group...")
        # Group exists but has changed - update existing message
        
        # Delete old individual messages first
        log.info("  - Deleting old individual messages...")
        await delete_old_individual_messages(items_in_group)
        
        # Edit existing message
        log.info("  - Attempting to edit existing message...")
        success = await edit_existing_group_message(channel, stored_group, group_key, items_in_group)
        
        if success:
            log.info("✅ Successfully updated existing grouped message")
            log.info("=" * 80)
            log.info("🔍 GROUP PROCESS DEBUG END")
            log.info("=" * 80)
            return True
        else:
            log.warning("⚠️ Failed to update existing message - will create new one")
            log.info("→ Falling back to creating new grouped message")
    
    log.info("📝 Step 4: Creating new grouped message...")
    # New group or update failed - create new message
    try:
        # Delete old individual messages first
        log.info("  - Deleting old individual messages...")
        await delete_old_individual_messages(items_in_group)
        
        # If we have a stored group but the message wasn't found, 
        # try to retrieve additional items from the stored group data
        if stored_group:
            log.info("  - Stored group available, checking for missing items...")
            try:
                stored_titles = stored_group.get("item_titles", [])
                # item_titles is already a list from the database, not JSON string
                if isinstance(stored_titles, str):
                    stored_titles = json.loads(stored_titles)
                log.info("  - Retrieved %d item titles from stored group", len(stored_titles))
                
                # Keep existing grouped items intact - don't remove them just because they're not in recent changes
                # Groups should be stable and only add new items, not remove existing ones
            except Exception as e:
                log.error("  - Failed to retrieve stored group items: %s", e)
        else:
            log.info("  - No stored group data available")
        
        # Create and send grouped embed
        log.info("  - Creating grouped embed...")
        grouped_embed, view = await create_grouped_embed(group_key, items_in_group)
        
        log.info("  - Sending grouped message to Discord...")
        grouped_msg = await channel.send(embed=grouped_embed, view=view)
        log.info("✅ Posted new grouped embed with %d items (key: %s) - Message ID: %s", 
                len(items_in_group), group_key_hash[:8], grouped_msg.id)
        
        # Mark group as posted atomically with updated data storage
        log.info("  - Updating stored group data...")
        await update_stored_group_data(group_key_hash, location, price, items_in_group, 
                                      grouped_msg.id, channel.id)
        
        # Store all items in the database and link them to the grouped message
        log.info("  - Storing %d items in database...", len(items_in_group))
        for item in items_in_group:
            # Generate pid if not present
            if "pid" not in item:
                pid = urlparse(item["url"]).path.strip("/").replace("/", "-") or item["url"]
                item["pid"] = pid
            
            # Store item with grouped message reference
            pid = item.get('pid', item.get('url', '').replace('/', '-'))
            await mark_posted(pid, item, grouped_msg.id, channel.id)
        
        log.info("✅ Successfully created and stored new grouped message")
        log.info("=" * 80)
        log.info("🔍 GROUP PROCESS DEBUG END")
        log.info("=" * 80)
        return True
        
    except Exception as e:
        log.error("❌ Error creating grouped message: %s", e)
        log.info("=" * 80)
        log.info("🔍 GROUP PROCESS DEBUG END (ERROR)")
        log.info("=" * 80)
        return False


async def safe_post_grouped_embed(channel, group_key: str, items_in_group: list[dict]) -> bool:
    """Safely post a grouped embed with proper locking and duplicate prevention with comprehensive debugging."""
    global posting_lock
    
    # Extract location and price from first item since group_key is now a string hash
    if items_in_group:
        first_item = items_in_group[0]
        location = first_item.get("location", "Unknown")
        price = first_item.get("price", "Unknown")
    else:
        location = "Unknown"
        price = "Unknown"
    log.info("🔒 SAFE POST GROUPED EMBED START")
    log.info("  ├─ Location: '%s'", location)
    log.info("  ├─ Price: '%s'", price)
    log.info("  ├─ Items count: %d", len(items_in_group))
    log.info("  ├─ Channel: %s (ID: %d)", channel.name, channel.id)
    log.info("  └─ Acquiring posting lock...")
    
    async with posting_lock:  # Prevent race conditions
        log.info("  ├─ ✅ Posting lock acquired")
        
        # Generate stable group key
        group_key_hash = generate_stable_group_key(location, price, items_in_group)
        log.info("  ├─ Generated group key hash: %s", group_key_hash[:16] + "...")
        
        # Log group details for debugging
        item_titles = [item.get("title", "Unknown") for item in items_in_group]
        log.info("  ├─ Item titles: %s", ', '.join(item_titles))
        
        # Check if group has changed
        log.info("  └─ Checking if group has changed...")
        has_changed, stored_group = await has_group_changed(group_key_hash, items_in_group)
        
        if stored_group:
            log.info("  ├─ Found stored group:")
            log.info("  │  ├─ Message ID: %s", stored_group.get('discord_message_id', 'None'))
            log.info("  │  ├─ Channel ID: %s", stored_group.get('discord_channel_id', 'None'))
            log.info("  │  └─ Has changed: %s", has_changed)
        else:
            log.info("  ├─ No stored group found")
            log.info("  └─ This appears to be a new group")
        
        # If group exists, check if the Discord message still exists
        if stored_group:
            msg_id = stored_group.get("discord_message_id")
            ch_id = stored_group.get("discord_channel_id")
            if msg_id and ch_id:
                log.info("  ├─ Checking if Discord message %d still exists...", msg_id)
                message_exists = await check_message_exists(msg_id, ch_id)
                if not message_exists:
                    log.warning("  ├─ ❌ Discord message %d no longer exists", msg_id)
                    log.info("  │  └─ Will create new group message")
                    # Clear the stored message ID to force creation of new message
                    stored_group["discord_message_id"] = None
                    # Since message doesn't exist, we need to create a new one
                    has_changed = True
                else:
                    log.info("  ├─ ✅ Discord message %d still exists", msg_id)
            else:
                log.warning("  ├─ ⚠️ Stored group has incomplete message info")
                log.info("  │  ├─ Message ID: %s", msg_id if msg_id else 'None')
                log.info("  │  └─ Channel ID: %s", ch_id if ch_id else 'None')
        
        if not has_changed and stored_group:
            log.info("  ├─ Group unchanged - skipping posting")
            log.info("  │  ├─ Group key: %s", group_key_hash[:8])
            log.info("  │  └─ Items: %d", len(items_in_group))
            log.info("  └─ 🔒 SAFE POST GROUPED EMBED END (SKIPPED)")
            return False
        elif stored_group:
            log.info("  ├─ Group changed - will update existing message")
        else:
            log.info("  ├─ New group - will create new message")
        
        if stored_group:
            # Group exists but has changed - update existing message
            log.info("  ├─ Updating existing group message...")
            
            # Delete old individual messages first
            log.info("  │  ├─ Deleting old individual messages...")
            await delete_old_individual_messages(items_in_group)
            
            # Edit existing message
            log.info("  │  └─ Editing existing Discord message...")
            success = await edit_existing_group_message(channel, stored_group, group_key, items_in_group)
            if success:
                log.info("  ├─ ✅ Successfully updated grouped message")
                log.info("  │  ├─ Group key: %s", group_key_hash[:8])
                log.info("  │  └─ Items: %d", len(items_in_group))
                log.info("  └─ 🔒 SAFE POST GROUPED EMBED END (UPDATED)")
                return True
            else:
                log.warning("  ├─ ❌ Failed to update grouped message")
                log.info("  │  └─ Deleting old grouped message and creating new one...")
                
                # Delete the old grouped message to prevent duplicates
                if stored_group.get("discord_message_id") and stored_group.get("discord_channel_id"):
                    try:
                        old_channel = bot.get_channel(stored_group["discord_channel_id"])
                        if old_channel:
                            old_msg = await old_channel.fetch_message(stored_group["discord_message_id"])
                            await old_msg.delete()
                            log.info("  │  ├─ ✅ Deleted old grouped message")
                    except discord.NotFound:
                        log.info("  │  ├─ ℹ️ Old grouped message not found (already deleted)")
                    except Exception as e:
                        log.error("  │  ├─ ❌ Error deleting old grouped message: %s", e)
                
                # Fall through to create new message if update failed
        
        # New group or update failed - create new message
        log.info("  ├─ Creating new group message...")
        try:
            # Delete old individual messages first
            log.info("  │  ├─ Deleting old individual messages...")
            await delete_old_individual_messages(items_in_group)
            
            # If we have a stored group but the message wasn't found, 
            # try to retrieve additional items from the stored group data
            log.info("Creating new group message - stored_group available: %s", 
                    "Yes" if stored_group else "No")
            if stored_group:
                try:
                    stored_titles = stored_group.get("item_titles", [])
                    # item_titles is already a list from the database, not JSON string
                    if isinstance(stored_titles, str):
                        stored_titles = json.loads(stored_titles)
                    log.info("Retrieved %d item titles from stored group", len(stored_titles))
                    
                    # Keep existing grouped items intact - don't remove them just because they're not in recent changes
                    # Groups should be stable and only add new items, not remove existing ones
                except Exception as e:
                    log.warning("Failed to retrieve stored group items: %s", e)
            
            # Create and send grouped embed
            log.info("  │  ├─ Creating grouped embed...")
            grouped_embed, view = await create_grouped_embed(group_key, items_in_group)
            
            log.info("  │  ├─ Sending grouped message to Discord...")
            grouped_msg = await channel.send(embed=grouped_embed, view=view)
            
            log.info("  │  ├─ ✅ Message sent successfully")
            log.info("  │  │  ├─ Message ID: %d", grouped_msg.id)
            log.info("  │  │  ├─ Items in group: %d", len(items_in_group))
            log.info("  │  │  └─ Group key: %s", group_key_hash[:8])
            
            # Mark group as posted atomically with updated data storage
            log.info("  │  ├─ Updating stored group data...")
            await update_stored_group_data(group_key_hash, location, price, items_in_group, 
                                          grouped_msg.id, channel.id)
            
            # Store all items in the database and link them to the grouped message
            log.info("  │  ├─ Storing %d items in database...", len(items_in_group))
            for i, item in enumerate(items_in_group, 1):
                # Generate pid if not present
                if "pid" not in item:
                    pid = urlparse(item["url"]).path.strip("/").replace("/", "-") or item["url"]
                    item["pid"] = pid
                else:
                    pid = item["pid"]
                
                log.debug("  │  │  ├─ Storing item %d: %s", i, item.get('title', 'Unknown'))
                # Store the item in database with Discord message info
                await mark_posted(pid, item, grouped_msg.id, channel.id)
            
            log.info("  ├─ ✅ Successfully created and stored new grouped message")
            log.info("  │  ├─ Message ID: %d", grouped_msg.id)
            log.info("  │  ├─ Items: %d", len(items_in_group))
            log.info("  │  └─ Group key: %s", group_key_hash[:8])
            log.info("  └─ 🔒 SAFE POST GROUPED EMBED END (CREATED)")
            return True
            
        except discord.HTTPException as e:
            log.error("  ├─ ❌ Discord HTTP error")
            log.error("  │  └─ Exception: %s", e)
            log.error("  └─ 🔒 SAFE POST GROUPED EMBED END (HTTP ERROR)")
            return False
        except Exception as e:
            log.error("  ├─ ❌ Unexpected error during group posting")
            log.error("  │  └─ Exception: %s", e)
            log.error("  └─ 🔒 SAFE POST GROUPED EMBED END (ERROR)")
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


async def get_existing_grouped_items() -> list[dict]:
    """
    Retrieve all existing grouped items from the database.
    This preserves items that are no longer in recent changes.
    """
    try:
        async with aiosqlite.connect(DB) as db:
            # DEBUG: First check what's in grouped_posts
            cursor = await db.execute("SELECT COUNT(*) FROM grouped_posts")
            total_groups = await cursor.fetchone()
            log.debug("DEBUG: Total groups in grouped_posts table: %d", total_groups[0] if total_groups else 0)
            
            async with db.execute("""
                SELECT group_key, item_titles, location, price, categories
                FROM grouped_posts 
                WHERE item_titles IS NOT NULL AND item_titles != '[]'
            """) as cur:
                rows = await cur.fetchall()
                log.debug("DEBUG: Retrieved %d rows from grouped_posts", len(rows))
                
                existing_items = []
                for row in rows:
                    log.debug("DEBUG: Processing group key: %s", row[0])
                    item_titles = json.loads(row[1]) if isinstance(row[1], str) else row[1]
                    location = row[2]
                    price = row[3]
                    categories = json.loads(row[4]) if isinstance(row[4], str) else row[4]
                    
                    log.debug("DEBUG: Group has %d items, location='%s', price='%s'", 
                             len(item_titles), location, price)
                    
                    # Create item dict for each title in the group
                    for title in item_titles:
                        item = {
                            "title": title,
                            "content": "",  # Not stored in grouped_posts
                            "price": price,
                            "location": location,
                            "image": "",  # Not stored in grouped_posts
                            "images": [],  # Not stored in grouped_posts
                            "url": "",  # Not stored in grouped_posts
                            "title_icons": [],  # Not stored in grouped_posts
                            "pid": title.lower().replace(" ", "-")  # Generate from title
                        }
                        
                        log.debug("DEBUG: Created existing item '%s' with location='%s', price='%s'", 
                                 title, location, price)
                        
                        # Add category if available for this title
                        if categories and title in categories:
                            item["category"] = categories[title]
                        
                        existing_items.append(item)
                        log.debug("DEBUG: Created existing item: %s (Location: %s, Price: %s)", 
                                 title, location, price)
                
                log.info("Retrieved %d existing items from %d groups", len(existing_items), len(rows))
                return existing_items
                
    except Exception as e:
        log.error("Failed to retrieve existing grouped items: %s", e)
        return []


def merge_current_with_existing_items(current_items: list[dict], existing_items: list[dict]) -> list[dict]:
    """
    Smart merge that preserves existing grouped items and adds new items.
    
    This function:
    1. Starts with existing items (preserve current groups)
    2. Adds new current items that aren't duplicates
    3. Uses multiple criteria to detect duplicates (title, URL, content hash)
    4. Maintains group integrity by preserving existing grouped items
    """
    log.debug("DEBUG: merge_current_with_existing_items called with %d current, %d existing", 
              len(current_items), len(existing_items))
    
    # Create a map of existing items by normalized title for quick lookup
    existing_items_map = {}
    for item in existing_items:
        title = (item.get("title", "") or "").strip().lower()
        if title:
            existing_items_map[title] = item
    
    log.debug("DEBUG: Existing titles map has %d entries", len(existing_items_map))
    
    # Start with existing items (preserve current groups)
    merged_items = existing_items.copy()
    
    # Track titles we've already added to avoid duplicates
    added_titles = set()
    for item in existing_items:
        title = (item.get("title", "") or "").strip().lower()
        if title:
            added_titles.add(title)
    
    # Add current items that aren't duplicates
    for current_item in current_items:
        current_title = (current_item.get("title", "") or "").strip().lower()
        
        # Skip if we already have this title
        if current_title in added_titles:
            log.debug("DEBUG: Skipping duplicate current item: %s", current_item.get("title", "Unknown"))
            continue
        
        # Add this new current item
        merged_items.append(current_item)
        added_titles.add(current_title)
        
        log.debug("DEBUG: Added new current item: %s (Location: %s, Price: %s)", 
                 current_item.get("title", "Unknown"),
                 current_item.get("location", "Unknown"),
                 current_item.get("price", "Unknown"))
    
    # Final deduplication to ensure no duplicates slipped through
    final_merged = deduplicate_items(merged_items)
    
    log.debug("DEBUG: merge_current_with_existing_items returning %d items (merged from %d existing + %d current)", 
              len(final_merged), len(existing_items), len(current_items))
    return final_merged


def update_cursor_after_successful_processing(page_url: str, change_time: datetime) -> bool:
    """
    Update cursor ONLY after successful processing of aegift item.
    This ensures cursor represents last successfully processed change, not newest seen change.
    
    Args:
        page_url: The URL that was successfully processed
        change_time: The time of the change
        
    Returns:
        bool: True if cursor was updated successfully
    """
    try:
        change_id = generate_change_id(page_url, change_time.isoformat())
        update_last_seen_change_sync(change_id)
        log.info(f"Cursor updated to successfully processed change: {change_id}")
        log.info(f"Processing success -> {change_id}")
        return True
    except Exception as e:
        log.error(f"Failed to update cursor after successful processing: {e}")
        return False


def _extract_recent_changes_entries() -> dict[str, datetime]:
    """
    Get mapping: page_url -> change_time using cursor-based tracking.
    Processes entries from newest to oldest, stopping at last_successfully_processed_change.
    Only checks the main recent changes page - no pagination.
    
    IMPORTANT: Cursor is NOT updated here. Cursor is only updated after successful processing.
    """
    page_times: dict[str, datetime] = {}

    log.info("Starting cursor-based recent changes extraction")

    try:
        # Ensure we have an active session before making requests
        if not ensure_wikidot_session(session):
            return page_times
            
        res = session.get(RECENT_URL_HTTP, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        log.info("Fetching page: %s", RECENT_URL_HTTP)

        # Get last successfully processed change for cursor tracking
        last_successfully_processed_change = get_last_seen_change_sync()
        log.debug(f"Last successfully processed change: {last_successfully_processed_change}")
        
        # Collect all entries first to determine newest
        all_entries: list[tuple[str, str, datetime]] = []  # (href, time_text, change_time)
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

            page_url = _make_absolute(href).rstrip("/")
            all_entries.append((page_url, time_text, change_time))

        # Sort by time (newest first) for cursor-based processing
        all_entries.sort(key=lambda x: x[2], reverse=True)
        
        # Process entries from newest to oldest with optimization
        processed_count = 0
        stop_processing = False
        max_entries_per_run = 50  # Limit processing to prevent timeouts
        
        for page_url, time_text, change_time in all_entries:
            # Generate change ID for cursor tracking
            change_id = generate_change_id(page_url, change_time.isoformat())
            
            # Stop if we've reached the last successfully processed change OR max entries limit
            if last_successfully_processed_change and change_id == last_successfully_processed_change:
                log.info(f"Reached last successfully processed change: {change_id}, stopping processing")
                stop_processing = True
                break
            
            # Stop after processing max entries to prevent timeouts
            if processed_count >= max_entries_per_run:
                log.info(f"Reached max entries limit ({max_entries_per_run}), will continue next run")
                break
            
            # Add to results (only entries newer than last_successfully_processed_change)
            prev = page_times.get(page_url)
            if prev is None or change_time < prev:
                page_times[page_url] = change_time
                log.debug("Found new page: %s (changed %s)", page_url, change_time)
                processed_count += 1

        # First run special handling - do NOT update cursor yet
        if not last_successfully_processed_change and len(page_times) > 10:
            # Keep up to 10 newest entries for first run
            sorted_entries = sorted(page_times.items(), key=lambda x: x[1], reverse=True)
            page_times = dict(sorted_entries[:10])
            log.info(f"First run - processing {len(page_times)} newest entries")
            processed_count = len(page_times)

        log.info("Cursor-based extraction: %d rows found, %d processed, %d total pages", 
                rows_found, processed_count, len(page_times))

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
    Fetch aegift pages with proper cursor workflow.
    
    Processing flow:
    1. Fetch Recent Changes page (NO cursor update)
    2. For each row: open page -> parse tags -> detect aegift -> extract data
    3. Return items with cursor tracking info for later processing
    4. Cursor will be updated AFTER successful processing in main loop
    """
    page_times = _extract_recent_changes_entries()  # Check main page only
    if not page_times:
        log.info("No recent changes found")
        return []

    # Sort by time (chronological order - oldest first for proper cursor advancement)
    sorted_pages = sorted(page_times.items(), key=lambda kv: kv[1])
    if newest_first:
        sorted_pages = list(reversed(sorted_pages))

    results: list[dict] = []
    seen_ids: set[str] = set()

    for page_url, change_time in sorted_pages:
        pid = urlparse(page_url).path.strip("/").replace("/", "-") or page_url
        if pid in seen_ids:
            continue

        log.info(f"AEGIFT CHECK -> {page_url}")

        # Try the page itself first
        details = extract_item_details(page_url)
        if details:
            log.info(f"Tags detected -> aegift confirmed")
            # Include cursor tracking info but DO NOT update cursor yet
            results.append({
                "id": pid, 
                **details, 
                "change_time": change_time, 
                "page_url": page_url,
                "change_id": generate_change_id(page_url, change_time.isoformat())
            })
            seen_ids.add(pid)
            log.info("Found aegift: %s", details["title"])
        else:
            log.info(f"Tags detected -> no aegift tag")
            # If not a direct item page, try its child links
            child_links = _extract_related_item_links(page_url, max_links=3)
            log.debug("Found %d child links for %s", len(child_links), page_url)
            for child_url in child_links:
                child_pid = urlparse(child_url).path.strip("/").replace("/", "-") or child_url
                if child_pid in seen_ids:
                    continue
                
                log.info(f"AEGIFT CHECK -> {child_url}")
                child_details = extract_item_details(child_url)
                if child_details:
                    log.info(f"Tags detected -> aegift confirmed")
                    # Include cursor tracking info but DO NOT update cursor yet
                    results.append({
                        "id": child_pid, 
                        **child_details, 
                        "change_time": change_time, 
                        "page_url": child_url,
                        "change_id": generate_change_id(child_url, change_time.isoformat())
                    })
                    seen_ids.add(child_pid)
                    log.info("Found aegift child: %s", child_details["title"])
                else:
                    log.info(f"Tags detected -> no aegift tag")
                    
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

async def create_pane_embed(post: dict) -> tuple[discord.Embed, discord.ui.View]:
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
        description=f"**[{post['title']}]({post['url']})**\n\n{desc}",
        color=0xFF4500,
    )
    # Note: No thumbnail in main post - image will be shown in ephemeral message only
    embed.set_footer(text="AQW Daily Gift")
    
    # Create view with item-type button
    item_type = detect_item_type_from_title(post.get('title', ''))
    structured_item = {
        'name': post.get('title', 'Unknown'),
        'image': post.get('image', ''),
        'price': post.get('price', 'Unknown'),
        'location': post.get('location', 'Unknown'),
        'rarity': post.get('rarity', 'Unknown'),
        'type': item_type
    }
    view = SingleItemView(structured_item)
    
    return embed, view


class SingleItemView(discord.ui.View):
    """View for single items with item-type button."""
    def __init__(self, item: dict, timeout: float = None):
        super().__init__(timeout=timeout)
        self.item = item
        
        # Add item-type button
        emoji = get_item_type_emoji(item.get('type', 'misc'))
        button = SingleItemButton(item, emoji)
        self.add_item(button)


class SingleItemButton(discord.ui.Button):
    """Button for single item with custom emoji."""
    def __init__(self, item: dict, emoji: str):
        self.item = item
        # Parse the emoji string into a proper discord.PartialEmoji
        parsed_emoji = parse_discord_emoji(emoji)
        
        # Use item type instead of item name for button label
        item_type = item.get('type', 'misc').capitalize()
        
        super().__init__(
            label=item_type,  # Show item type instead of item name
            style=discord.ButtonStyle.secondary,
            emoji=parsed_emoji
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Show ephemeral item viewer."""
        await interaction.response.send_message(
            embed=self.create_item_embed(),
            view=ItemPaginationView([self.item]),
            ephemeral=True
        )
    
    def create_item_embed(self) -> discord.Embed:
        """Create embed for this item."""
        item = self.item
        
        embed = discord.Embed(
            title=item.get('name', 'Unknown'),
            description="Item Details",
            color=discord.Color.blue()
        )
        
        # Add fields
        if item.get('location') and item.get('location') != 'Unknown':
            embed.add_field(name="Location", value=item['location'], inline=True)
        
        if item.get('rarity') and item.get('rarity') != 'Unknown':
            embed.add_field(name="Rarity", value=item['rarity'], inline=True)
        
        if item.get('price') and item.get('price') != 'Unknown':
            embed.add_field(name="Price", value=item['price'], inline=True)
        
        # Add image if available
        if item.get('image'):
            embed.set_image(url=item['image'])
        
        return embed


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
            posts = await asyncio.wait_for(asyncio.to_thread(fetch_recent_aegifts, limit=10), timeout=30)
            log.info("DEBUG: fetch_recent_aegifts returned %d posts", len(posts) if posts else 0)
            
            if posts is None:
                # Request failed
                smart_polling.update_interval(has_new_changes=False, has_error=True)
                await asyncio.sleep(smart_polling.current_interval)
                continue
            
            # Check for new changes and collect changed items
            has_new_changes = False
            changed_items = []
            all_current_items = []
            
            # Store the timestamp at the start of processing to detect race conditions
            processing_start_time = datetime.now()
            
            # Track the last successfully processed change_id for cursor advancement
            latest_processed = None
            
            # Process items in chronological order (oldest first) to ensure proper cursor advancement
            for post in posts:
                try:
                    # Extract source ID and change ID
                    source_id = get_source_id_from_item(post)
                    change_id = post.get('change_id')
                    
                    log.info(f"[PROCESS] Processing item source_id={source_id}, change_id={change_id}")
                    
                    # Process the item using the new logic
                    success = await process_item(post, channel)
                    
                    if success:
                        latest_processed = change_id
                        log.info(f"[CURSOR] Successfully processed change_id={change_id}")
                    else:
                        log.warning(f"[PROCESS] Failed to process change_id={change_id}")
                        
                except Exception as e:
                    log.error(f"[PROCESS] Error processing change_id={post.get('change_id')}: {e}")
                    continue  # Continue to next item even if this one fails
            
            # Update cursor ONLY after all processing is complete
            if latest_processed:
                update_last_seen_change_sync(latest_processed)
                log.info(f"[CURSOR] Updated cursor to={latest_processed}")
            else:
                log.info("[CURSOR] No items processed - cursor not updated")
                
        elif not posts:  # This handles the case where fetch_recent_aegifts returns empty
                log.info("No recent changes found - checking existing grouped posts for updates")
                # Even with no recent changes, we need to check if existing grouped posts need updates
                # This handles cases where items fall off the recent changes page
                
                # Retrieve existing grouped items from database to preserve group stability
                existing_grouped_items = await get_existing_grouped_items()
                log.info("Retrieved %d existing grouped items from database", len(existing_grouped_items))
                
                if not existing_grouped_items:
                    log.info("No existing grouped items found - skipping")
                    continue
            else:
                log.info("No changed items found - no processing needed")
                # No processing means no cursor advancement
                
                # Process existing grouped items to ensure they're still valid
                all_groups = improved_group_items_by_location_price(existing_grouped_items)
                
                # Get recently processed items for startup safeguard
                startup_hours = get_startup_safeguard_hours()
                recently_processed = await get_recently_processed_items(startup_hours)
                log.info(f"Startup safeguard: Found {len(recently_processed)} items processed in last {startup_hours} hours")
                
                # Process each group to ensure they're still valid
                for group_key, items_in_group in all_groups.items():
                    # Apply startup safeguard to grouped items
                    safe_to_process = True
                    for item in items_in_group:
                        if not await is_startup_safe(item, recently_processed):
                            log.info("Startup safeguard: Skipping recently processed grouped item '%s'", item['title'])
                            safe_to_process = False
                            break
                    
                    if not safe_to_process:
                        continue
                    
                    # Check if this is a single item - post individually instead of grouped
                    if len(items_in_group) == 1:
                        item = items_in_group[0]
                        log.info("Single item in existing group: '%s' - posting individually", item['title'])
                        await post_individual_item(channel, item)
                    else:
                        log.info("Existing group with %d items - updating grouped post", len(items_in_group))
                        await post_grouped_items(channel, group_key, items_in_group)
                
                continue
            
            # Check for race conditions - fetch recent changes again to see if we missed anything
            try:
                race_check_posts = await asyncio.to_thread(fetch_recent_aegifts, limit=10)
                if race_check_posts:
                    processing_time = (datetime.now() - processing_start_time).total_seconds()
                    log.debug("Race condition check: Processing took %.2f seconds", processing_time)
                    
                    # Check if any new items appeared during processing
                    race_check_pids = set()
                    for post in race_check_posts:
                        pid = urlparse(post["url"]).path.strip("/").replace("/", "-") or post["url"]
                        race_check_pids.add(pid)
                    
                    original_pids = set(post["pid"] for post in all_current_items)
                    new_pids_during_processing = race_check_pids - original_pids
                    
                    if new_pids_during_processing:
                        log.warning("Race condition detected: %d new items appeared during processing: %s", 
                                   len(new_pids_during_processing), list(new_pids_during_processing))
                        # Force a shorter polling interval to catch these changes quickly
                        smart_polling.update_interval(has_new_changes=True, has_error=False)
                        await asyncio.sleep(2)  # Brief pause before next iteration
                        continue
            except Exception as e:
                log.debug("Race condition check failed: %s", e)
            
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


@bot.tree.command(name="diagnose_db", description="Diagnose database consistency issues")
@commands.has_permissions(manage_messages=True)
async def diagnose_db(interaction: discord.Interaction):
    """Comprehensive database diagnostic tool."""
    try:
        await interaction.response.defer(thinking=True)
        
        embed = discord.Embed(
            title="🔍 Database Diagnostic Report",
            color=discord.Color.blue()
        )
        
        # 1. Check database file integrity
        try:
            import sqlite3
            with sqlite3.connect(DB) as conn:
                cursor = conn.execute("PRAGMA integrity_check")
                integrity_result = cursor.fetchone()
                if integrity_result[0] == "ok":
                    embed.add_field(name="✅ Database Integrity", value="Database file is healthy", inline=False)
                else:
                    embed.add_field(name="❌ Database Integrity", value=f"Issues found: {integrity_result[0]}", inline=False)
        except Exception as e:
            embed.add_field(name="❌ Database Check Failed", value=f"Error: {e}", inline=False)
        
        # 2. Check for duplicate records
        try:
            async with aiosqlite.connect(DB) as db:
                # Check duplicate source_ids in posts table
                cursor = await db.execute("""
                    SELECT source_id, COUNT(*) as count 
                    FROM posts 
                    GROUP BY source_id 
                    HAVING COUNT(*) > 1
                    LIMIT 5
                """)
                duplicates = await cursor.fetchall()
                
                if duplicates:
                    dup_text = "\n".join([f"• {row[0]} ({row[1]} copies)" for row in duplicates])
                    embed.add_field(name="⚠️ Duplicate Records", value=dup_text, inline=False)
                else:
                    embed.add_field(name="✅ Duplicate Check", value="No duplicate source_ids found", inline=False)
                
                # Check duplicate group_keys in grouped_posts table
                cursor = await db.execute("""
                    SELECT group_key, COUNT(*) as count 
                    FROM grouped_posts 
                    GROUP BY group_key 
                    HAVING COUNT(*) > 1
                    LIMIT 5
                """)
                group_duplicates = await cursor.fetchall()
                
                if group_duplicates:
                    group_dup_text = "\n".join([f"• {row[0][:8]}... ({row[1]} copies)" for row in group_duplicates])
                    embed.add_field(name="⚠️ Group Duplicates", value=group_dup_text, inline=False)
                else:
                    embed.add_field(name="✅ Group Duplicate Check", value="No duplicate group_keys found", inline=False)
        except Exception as e:
            embed.add_field(name="❌ Duplicate Check Failed", value=f"Error: {e}", inline=False)
        
        # 3. Check cursor state consistency
        try:
            last_seen_async = await get_last_seen_change()
            last_seen_sync = get_last_seen_change_sync()
            
            cursor_status = f"Async: {last_seen_async or 'NULL'}\nSync: {last_seen_sync or 'NULL'}"
            if last_seen_async == last_seen_sync:
                embed.add_field(name="✅ Cursor Consistency", value="Async and sync cursors match", inline=False)
            else:
                embed.add_field(name="⚠️ Cursor Mismatch", value=cursor_status, inline=False)
        except Exception as e:
            embed.add_field(name="❌ Cursor Check Failed", value=f"Error: {e}", inline=False)
        
        # 4. Check for orphaned records
        try:
            async with aiosqlite.connect(DB) as db:
                # Posts without valid messages
                cursor = await db.execute("""
                    SELECT COUNT(*) as count FROM posts 
                    WHERE message_id IS NULL OR channel_id IS NULL
                """)
                orphaned_posts = (await cursor.fetchone())[0]
                
                # Groups without valid messages
                cursor = await db.execute("""
                    SELECT COUNT(*) as count FROM grouped_posts 
                    WHERE discord_message_id IS NULL OR discord_channel_id IS NULL
                """)
                orphaned_groups = (await cursor.fetchone())[0]
                
                orphaned_text = f"Posts: {orphaned_posts}, Groups: {orphaned_groups}"
                if orphaned_posts > 0 or orphaned_groups > 0:
                    embed.add_field(name="⚠️ Orphaned Records", value=orphaned_text, inline=False)
                else:
                    embed.add_field(name="✅ Orphan Check", value="No orphaned records found", inline=False)
        except Exception as e:
            embed.add_field(name="❌ Orphan Check Failed", value=f"Error: {e}", inline=False)
        
        # 5. Check database size and performance
        try:
            import os
            if os.path.exists(DB):
                db_size = os.path.getsize(DB) / (1024 * 1024)  # MB
                embed.add_field(name="📊 Database Stats", value=f"Size: {db_size:.2f} MB", inline=False)
                
                # Check table row counts
                async with aiosqlite.connect(DB) as db:
                    posts_count = (await (await db.execute("SELECT COUNT(*) FROM posts")).fetchone())[0]
                    groups_count = (await (await db.execute("SELECT COUNT(*) FROM groups")).fetchone())[0]
                    grouped_posts_count = (await (await db.execute("SELECT COUNT(*) FROM grouped_posts")).fetchone())[0]
                    
                    stats_text = f"Posts: {posts_count}\nGroups: {groups_count}\nGrouped Posts: {grouped_posts_count}"
                    embed.add_field(name="📈 Record Counts", value=stats_text, inline=False)
        except Exception as e:
            embed.add_field(name="❌ Stats Check Failed", value=f"Error: {e}", inline=False)
        
        embed.set_footer(text="Database Diagnostic Complete")
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        log.error("Database diagnostic failed: %s", e)
        await interaction.followup.send(f"Diagnostic failed: {e}", ephemeral=True)


@bot.tree.command(name="clean_db", description="Clean up database inconsistencies")
@commands.has_permissions(manage_messages=True)
async def clean_db(interaction: discord.Interaction):
    """Clean up database inconsistencies found during diagnosis."""
    try:
        await interaction.response.defer(thinking=True)
        
        embed = discord.Embed(
            title="🧹 Database Cleanup",
            description="Cleaning up database inconsistencies...",
            color=discord.Color.orange()
        )
        
        cleanup_stats = {"duplicates_removed": 0, "orphans_cleaned": 0, "errors_fixed": 0}
        
        async with aiosqlite.connect(DB) as db:
            # 1. Remove duplicate posts (keep newest)
            try:
                await db.execute("""
                    DELETE FROM posts WHERE id NOT IN (
                        SELECT MAX(id) FROM posts GROUP BY source_id
                    )
                """)
                duplicates_removed = db.total_changes
                cleanup_stats["duplicates_removed"] = duplicates_removed
                await db.commit()
                embed.add_field(name="✅ Duplicates Cleaned", value=f"Removed {duplicates_removed} duplicate posts", inline=False)
            except Exception as e:
                log.error("Error cleaning duplicates: %s", e)
                embed.add_field(name="❌ Duplicate Cleanup Failed", value=f"Error: {e}", inline=False)
            
            # 2. Remove duplicate grouped_posts (keep newest)
            try:
                await db.execute("""
                    DELETE FROM grouped_posts WHERE id NOT IN (
                        SELECT MAX(id) FROM grouped_posts GROUP BY group_key
                    )
                """)
                group_duplicates_removed = db.total_changes
                cleanup_stats["duplicates_removed"] += group_duplicates_removed
                await db.commit()
                embed.add_field(name="✅ Group Duplicates Cleaned", value=f"Removed {group_duplicates_removed} duplicate groups", inline=False)
            except Exception as e:
                log.error("Error cleaning group duplicates: %s", e)
                embed.add_field(name="❌ Group Duplicate Cleanup Failed", value=f"Error: {e}", inline=False)
            
            # 3. Remove orphaned records
            try:
                await db.execute("DELETE FROM posts WHERE message_id IS NULL OR channel_id IS NULL")
                orphaned_posts = db.total_changes
                cleanup_stats["orphans_cleaned"] += orphaned_posts
                await db.commit()
                
                await db.execute("DELETE FROM grouped_posts WHERE discord_message_id IS NULL OR discord_channel_id IS NULL")
                orphaned_groups = db.total_changes
                cleanup_stats["orphans_cleaned"] += orphaned_groups
                await db.commit()
                
                embed.add_field(name="✅ Orphans Cleaned", value=f"Removed {orphaned_posts + orphaned_groups} orphaned records", inline=False)
            except Exception as e:
                log.error("Error cleaning orphans: %s", e)
                embed.add_field(name="❌ Orphan Cleanup Failed", value=f"Error: {e}", inline=False)
            
            # 4. Vacuum database to optimize
            try:
                await db.execute("VACUUM")
                await db.commit()
                embed.add_field(name="✅ Database Optimized", value="VACUUM completed", inline=False)
            except Exception as e:
                log.error("Error vacuuming database: %s", e)
                embed.add_field(name="❌ Vacuum Failed", value=f"Error: {e}", inline=False)
        
        # Summary
        total_cleaned = cleanup_stats["duplicates_removed"] + cleanup_stats["orphans_cleaned"]
        embed.description = f"Cleanup complete! Fixed {total_cleaned} issues."
        embed.set_footer(text=f"Duplicates: {cleanup_stats['duplicates_removed']} | Orphans: {cleanup_stats['orphans_cleaned']}")
        
        await interaction.followup.send(embed=embed)
        log.info("Database cleanup completed: %s", cleanup_stats)
        
        if passed_count == total_count:
            embed.color = discord.Color.green()
        elif passed_count >= total_count * 0.7:
            embed.color = discord.Color.orange()
        else:
            embed.color = discord.Color.red()
        
        embed.set_footer(text="Run /clean_db to fix identified issues")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        log.error("Data verification failed: %s", e)
        await interaction.followup.send(f"Verification failed: {e}", ephemeral=True)


@bot.tree.command(name="reset_cursor", description="Reset cursor to force reprocessing from scratch")
@commands.has_permissions(manage_messages=True)
async def reset_cursor(interaction: discord.Interaction):
    """Reset cursor to force reprocessing all changes."""
    try:
        await interaction.response.defer(thinking=True)
        
        # Reset cursor using both async and sync methods
        await update_last_seen_change(None)
        update_last_seen_change_sync(None)
        
        embed = discord.Embed(
            title="🔄 Cursor Reset",
            description="Cursor has been reset to NULL. Bot will reprocess all changes from scratch.",
            color=discord.Color.red()
        )
        embed.add_field(name="⚠️ Warning", value="This will cause the bot to reprocess all items on next run.", inline=False)
        embed.set_footer(text="Use /diagnose_db to verify database state")
        
        await interaction.followup.send(embed=embed)
        log.warning("Cursor manually reset by admin command")
        
    except Exception as e:
        log.error("Cursor reset failed: %s", e)
        await interaction.followup.send(f"Reset failed: {e}", ephemeral=True)


@bot.tree.command(name="monitordeletions", description="Monitor if grouped messages are being deleted")
@commands.has_permissions(manage_messages=True)
async def monitor_deletions(interaction: discord.Interaction):
    """Check if grouped messages are being deleted and identify potential causes."""
    try:
        await interaction.response.defer(thinking=True)
        
        async with aiosqlite.connect(DB) as db:
            async with db.execute("""
                SELECT group_key, location, price, item_titles, discord_message_id, 
                       discord_channel_id, last_updated
                FROM grouped_posts 
                WHERE discord_message_id IS NOT NULL 
                ORDER BY last_updated DESC
            """) as cur:
                rows = await cur.fetchall()
        
        if not rows:
            await interaction.followup.send("No grouped messages with stored message IDs found.")
            return
        
        embed = discord.Embed(
            title="🔍 Message Deletion Monitor",
            description="Checking if stored Discord messages still exist...",
            color=discord.Color.orange()
        )
        
        existing_count = 0
        missing_count = 0
        recent_deletions = []
        
        for row in rows:
            group_key, location, price, item_titles_json, msg_id, ch_id, last_updated = row
            item_titles = json.loads(item_titles_json) if item_titles_json else []
            
            # Check if message exists
            channel = bot.get_channel(ch_id)
            msg_status = "❓ Unknown"
            time_diff = datetime.now(timezone.utc) - last_updated
            
            if channel:
                try:
                    msg = await channel.fetch_message(msg_id)
                    msg_status = "✅ Found"
                    existing_count += 1
                except discord.NotFound:
                    msg_status = "❌ Not Found"
                    missing_count += 1
                    # If message was updated recently (within last hour), it might be a recent deletion
                    if time_diff.total_seconds() < 3600:
                        recent_deletions.append({
                            "group_key": group_key[:8],
                            "msg_id": msg_id,
                            "time_ago": f"{int(time_diff.total_seconds()/60)} minutes ago",
                            "items": item_titles[:3]
                        })
                except discord.Forbidden:
                    msg_status = "🔒 No Permission"
                except Exception as e:
                    msg_status = f"⚠️ Error: {str(e)[:20]}"
            else:
                msg_status = "📵 Channel Not Found"
            
            embed.add_field(
                name=f"Group {group_key[:8]} ({len(item_titles)} items)",
                value=f"**Status:** {msg_status}\n"
                      f"**Updated:** {last_updated.strftime('%Y-%m-%d %H:%M')}\n"
                      f"**Age:** {int(time_diff.total_seconds()/3600)}h ago",
                inline=True
            )
        
        # Add summary
        embed.add_field(
            name="📊 Summary",
            value=f"**Total Groups:** {len(rows)}\n"
                  f"**Messages Found:** {existing_count}\n"
                  f"**Messages Missing:** {missing_count}\n"
                  f"**Missing Rate:** {missing_count/len(rows)*100:.1f}%",
            inline=False
        )
        
        # Add recent deletions if any
        if recent_deletions:
            deletion_text = "\n".join([
                f"• {d['group_key']} - {d['time_ago']} ({len(d['items'])} items)"
                for d in recent_deletions[:5]
            ])
            embed.add_field(
                name="⚠️ Recent Deletions (Last Hour)",
                value=deletion_text,
                inline=False
            )
        
        # Add recommendations
        recommendations = []
        if missing_count > 0:
            recommendations.append("• Messages are being deleted - check channel settings")
            recommendations.append("• Verify no other bots are deleting messages")
            recommendations.append("• Check if messages have auto-deletion enabled")
        if missing_count / len(rows) > 0.5:
            recommendations.append("• High deletion rate detected - consider investigating")
        
        if recommendations:
            embed.add_field(
                name="💡 Recommendations",
                value="\n".join(recommendations),
                inline=False
            )
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        log.error("Error in monitor_deletions: %s", e)
        await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)


@bot.tree.command(name="checkgroups", description="Check the status of all grouped messages and clean up corrupted data")
@commands.has_permissions(manage_messages=True)
async def check_groups(interaction: discord.Interaction):
    """Check the status of all grouped messages in the database and clean up corrupted data."""
    try:
        await interaction.response.defer(thinking=True)
        
        # First, run cleanup to remove corrupted groups
        log.info("🧹 Running cleanup before checking groups...")
        cleaned = await cleanup_corrupted_groups()
        
        async with aiosqlite.connect(DB) as db:
            async with db.execute("""
                SELECT group_key, location, price, item_titles, discord_message_id, discord_channel_id, last_updated
                FROM grouped_posts ORDER BY last_updated DESC
            """) as cur:
                rows = await cur.fetchall()
        
        if not rows:
            await interaction.followup.send("No grouped messages found in database.")
            return
        
        embed = discord.Embed(
            title="📊 Grouped Messages Status",
            color=discord.Color.blue()
        )
        
        # Add cleanup results
        if cleaned > 0:
            embed.add_field(
                name="🧹 Cleanup Results",
                value=f"✅ Removed {cleaned} corrupted group(s) from database",
                inline=False
            )
            embed.color = discord.Color.orange()
        
        for row in rows:
            group_key, location, price, item_titles_json, msg_id, ch_id, last_updated = row
            item_titles = json.loads(item_titles_json) if item_titles_json else []
            
            # Check if message still exists
            channel = bot.get_channel(ch_id)
            msg_exists = "❓ Unknown"
            if channel:
                try:
                    await channel.fetch_message(msg_id)
                    msg_exists = "✅ Found"
                except discord.NotFound:
                    msg_exists = "❌ Not Found"
                except discord.Forbidden:
                    msg_exists = "🔒 No Permission"
                except Exception as e:
                    msg_exists = f"⚠️ Error: {str(e)[:20]}"
            else:
                msg_exists = "📵 Channel Not Found"
            
            # Highlight potential issues
            status_emoji = "✅"
            if len(item_titles) > 10:
                status_emoji = "⚠️"
            elif len(item_titles) == 0:
                status_emoji = "❌"
            
            field_value = (
                f"**Location:** {location}\n"
                f"**Items:** {len(item_titles)} ({', '.join(item_titles[:3])}{'...' if len(item_titles) > 3 else ''})\n"
                f"**Message:** {msg_exists} (ID: {msg_id})\n"
                f"**Last Updated:** {last_updated}"
            )
            
            embed.add_field(
                name=f"{status_emoji} Group {group_key[:8]}",
                value=field_value,
                inline=False
            )
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        log.error("Error in check_groups: %s", e)
        await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)
@bot.tree.command(name="dismiss", description="Dismiss the most recent grouped message in this channel")
@commands.has_permissions(manage_messages=True)
async def dismiss(interaction: discord.Interaction):
    """Dismiss (delete) the most recent grouped message in the channel with comprehensive debugging."""
    log.info("🚀 DISMISS COMMAND START")
    log.info("  ├─ User: %s (ID: %d)", interaction.user.name, interaction.user.id)
    log.info("  ├─ Channel: %s (ID: %d)", interaction.channel.name, interaction.channel.id)
    log.info("  └─ Permissions: Manage Messages = ✅")
    
    try:
        await interaction.response.defer(thinking=True)
        log.info("  ├─ Interaction deferred successfully")
    except discord.NotFound:
        log.warning("  ├─ Interaction not found - token may have expired")
        log.info("  └─ Exiting silently")
        return
    
    channel = interaction.channel
    if not channel:
        await interaction.followup.send("Channel not found.", ephemeral=True)
        return
    
    # Get recent messages from the channel
    try:
        log.info("  ├─ Fetching recent messages (limit: 20)")
        messages = [msg async for msg in channel.history(limit=20)]
        log.info("  ├─ Retrieved %d messages", len(messages))
        
        # Find the most recent message that looks like a grouped post
        log.info("  └─ Searching for grouped messages...")
        grouped_message = None
        messages_checked = 0
        
        for i, message in enumerate(messages, 1):
            messages_checked += 1
            log.debug("  ├─ Message %d: Author=%s, Bot=%s, Embeds=%d", 
                     i, message.author.name, message.author.bot, len(message.embeds))
            
            # Skip messages from bots that aren't our bot
            if message.author.bot and message.author.id != bot.user.id:
                log.debug("  │  └─ Skipping other bot message")
                continue
                
            # Check if this looks like a grouped message
            if (message.embeds and 
                message.embeds[0].title and 
                ("Daily Gift" in message.embeds[0].title or "Location:" in str(message.embeds[0].description))):
                grouped_message = message
                log.info("  ├─ ✅ Found grouped message at position %d", i)
                log.info("  │  ├─ Message ID: %d", message.id)
                log.info("  │  ├─ Title: '%s'", message.embeds[0].title)
                log.info("  │  └─ Created: %s", message.created_at.strftime('%Y-%m-%d %H:%M:%S'))
                break
            else:
                log.debug("  │  └─ Not a grouped message")
        
        log.info("  └─ Checked %d messages", messages_checked)
        
        if not grouped_message:
            log.warning("  ├─ No grouped message found")
            await interaction.followup.send("No recent grouped message found to dismiss.", ephemeral=True)
            return
        
        # Delete the message
        log.info("  ├─ Attempting to delete message %d", grouped_message.id)
        await grouped_message.delete()
        
        # Log the action
        log.info("  ├─ ✅ Message deleted successfully")
        log.info("  └─ DISMISS COMMAND COMPLETED")
        log.info("     ├─ Channel: %s", channel.name)
        log.info("     ├─ User: %s", interaction.user.name)
        log.info("     └─ Message ID: %d", grouped_message.id)
        
        await interaction.followup.send("✅ Grouped message dismissed successfully.", ephemeral=True)
        
    except discord.Forbidden:
        log.error("  ├─ ❌ Permission denied - cannot delete messages")
        log.error("  └─ DISMISS COMMAND FAILED (PERMISSIONS)")
        await interaction.followup.send("❌ I don't have permission to delete messages in this channel.", ephemeral=True)
    except Exception as e:
        log.error("  ├─ ❌ Unexpected error during dismiss operation")
        log.error("  └─ Exception: %s", e)
        await interaction.followup.send(f"❌ Error dismissing message: {e}", ephemeral=True)
    
    log.info("  └─ DISMISS COMMAND COMPLETED")


async def cleanup_corrupted_groups():
    """Clean up corrupted group data immediately without requiring command sync."""
    log.info("🧹 IMMEDIATE CLEANUP START")
    
    try:
        async with aiosqlite.connect(DB) as db:
            # Get all stored groups
            cursor = await db.execute("""
                SELECT group_key, location, price, item_titles, discord_message_id, discord_channel_id
                FROM grouped_posts
            """)
            groups = await cursor.fetchall()
            
            if not groups:
                log.info("  └─ No group data found in database.")
                return 0
            
            log.info("  ├─ Found %d stored groups", len(groups))
            cleaned_count = 0
            
            for row in groups:
                group_key, location, price, item_titles_json, message_id, channel_id = row
                
                try:
                    # Parse item titles
                    if isinstance(item_titles_json, str):
                        item_titles = json.loads(item_titles_json)
                    else:
                        item_titles = item_titles_json
                    
                    # Check for corruption (unusual item counts or parsing errors)
                    if len(item_titles) > 10 or len(item_titles) == 0:
                        log.warning("  ├─ Found corrupted group: %s items", len(item_titles))
                        log.warning("  │  ├─ Group key: %s", group_key[:16] + "...")
                        log.warning("  │  └─ Items: %s", item_titles[:3] if len(item_titles) > 3 else item_titles)
                        
                        # Delete the corrupted group
                        await db.execute("DELETE FROM grouped_posts WHERE group_key=?", (group_key,))
                        cleaned_count += 1
                        
                        # Try to delete the Discord message if it exists
                        if message_id and channel_id:
                            try:
                                channel = bot.get_channel(channel_id)
                                if channel:
                                    message = await channel.fetch_message(message_id)
                                    await message.delete()
                                    log.info("  │  └─ ✅ Deleted corrupted Discord message")
                            except discord.NotFound:
                                log.info("  │  └─ ℹ️ Discord message not found (already deleted)")
                            except Exception as e:
                                log.warning("  │  └─ ⚠️ Could not delete Discord message: %s", e)
                    
                except Exception as e:
                    log.warning("  ├─ Error processing group %s: %s", group_key[:16], e)
                    # Delete groups that can't be parsed
                    await db.execute("DELETE FROM grouped_posts WHERE group_key=?", (group_key,))
                    cleaned_count += 1
            
            await db.commit()
            
            if cleaned_count > 0:
                log.info("  ├─ ✅ Cleaned up %d corrupted groups", cleaned_count)
            else:
                log.info("  └─ ✅ No corrupted groups found")
                
            return cleaned_count
                
    except Exception as e:
        log.error("  ├─ ❌ Error during cleanup")
        log.error("  └─ Exception: %s", e)
        return 0
    
    finally:
        log.info("🧹 IMMEDIATE CLEANUP END")


@bot.tree.command(name="cleanup-groups", description="Clean up corrupted group data from database")
@commands.has_permissions(manage_messages=True)
async def cleanup_groups(interaction: discord.Interaction):
    """Clean up corrupted group data that may have incorrect item counts."""
    log.info("🧹 CLEANUP GROUPS COMMAND START")
    log.info("  ├─ User: %s (ID: %d)", interaction.user.name, interaction.user.id)
    log.info("  └─ Channel: %s (ID: %d)", interaction.channel.name, interaction.channel.id)
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        async with aiosqlite.connect(DB) as db:
            # Get all stored groups
            cursor = await db.execute("""
                SELECT group_key, location, price, item_titles, discord_message_id, discord_channel_id
                FROM grouped_posts
            """)
            groups = await cursor.fetchall()
            
            if not groups:
                await interaction.followup.send("✅ No group data found in database.", ephemeral=True)
                return
            
            log.info("  ├─ Found %d stored groups", len(groups))
            cleaned_count = 0
            
            for row in groups:
                group_key, location, price, item_titles_json, message_id, channel_id = row
                
                try:
                    # Parse item titles
                    if isinstance(item_titles_json, str):
                        item_titles = json.loads(item_titles_json)
                    else:
                        item_titles = item_titles_json
                    
                    # Check if item count seems reasonable (more than 10 items is likely corrupted)
                    if len(item_titles) > 10:
                        log.warning("  ├─ Found potentially corrupted group: %s items", len(item_titles))
                        log.warning("  │  ├─ Group key: %s", group_key[:16] + "...")
                        log.warning("  │  └─ Items: %s", item_titles[:5])  # Show first 5 items
                        
                        # Delete the corrupted group
                        await db.execute("DELETE FROM grouped_posts WHERE group_key=?", (group_key,))
                        cleaned_count += 1
                        
                        # Try to delete the Discord message if it exists
                        if message_id and channel_id:
                            try:
                                channel = bot.get_channel(channel_id)
                                if channel:
                                    message = await channel.fetch_message(message_id)
                                    await message.delete()
                                    log.info("  │  └─ ✅ Deleted corrupted Discord message")
                            except discord.NotFound:
                                log.info("  │  └─ ℹ️ Discord message not found (already deleted)")
                            except Exception as e:
                                log.warning("  │  └─ ⚠️ Could not delete Discord message: %s", e)
                    
                except Exception as e:
                    log.warning("  ├─ Error processing group %s: %s", group_key[:16], e)
                    # Delete groups that can't be parsed
                    await db.execute("DELETE FROM grouped_posts WHERE group_key=?", (group_key,))
                    cleaned_count += 1
            
            await db.commit()
            
            if cleaned_count > 0:
                log.info("  ├─ ✅ Cleaned up %d corrupted groups", cleaned_count)
                await interaction.followup.send(
                    f"✅ Cleaned up {cleaned_count} corrupted group(s) from database.", 
                    ephemeral=True
                )
            else:
                log.info("  └─ ✅ No corrupted groups found")
                await interaction.followup.send("✅ No corrupted groups found.", ephemeral=True)
                
    except Exception as e:
        log.error("  ├─ ❌ Error during cleanup")
        log.error("  └─ Exception: %s", e)
        await interaction.followup.send(f"❌ Error during cleanup: {e}", ephemeral=True)
    
    log.info("🧹 CLEANUP GROUPS COMMAND END")


@cleanup_groups.error
async def cleanup_groups_error(interaction: discord.Interaction, error):
    """Handle errors for the cleanup groups command."""
    if isinstance(error, commands.MissingPermissions):
        await interaction.response.send_message("❌ You need 'Manage Messages' permission to use this command.", ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ Error: {error}", ephemeral=True)


# ---------------- READY ----------------
@bot.event
async def on_ready():
    log.info("Logged in as %s", bot.user)
    
    # Perform Wikidot login once at startup
    if not wikidot_login(session):
        log.error("Wikidot login failed, bot will continue without authentication")
    
    await init_db()
    
    # Clean up any corrupted group data on startup
    log.info("Performing startup cleanup of corrupted group data...")
    cleaned = await cleanup_corrupted_groups()
    if cleaned > 0:
        log.info("Startup cleanup completed: %d corrupted groups removed", cleaned)
    
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
