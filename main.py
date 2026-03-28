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
    
    # Ensure required fields exist, preserving all original fields including pid
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
        normalized_item['content'] = "No content available"
    
    return normalized_item


def deduplicate_items(items: list[dict]) -> list[dict]:
    """Remove duplicate items based on URL, keeping the most complete version.
    
    This function deduplicates items by URL, preferring items with more complete data.
    If multiple items have the same URL, the one with the most non-empty fields is kept.
    
    Args:
        items (list[dict]): List of items to deduplicate
        
    Returns:
        list[dict]: Deduplicated list of items
        
    Example:
        >>> items = [
        ...     {'url': 'http://test.com/item', 'title': 'Item', 'content': ''},
        ...     {'url': 'http://test.com/item', 'title': 'Item', 'content': 'Full content'}
        ... ]
        >>> deduped = deduplicate_items(items)
        >>> len(deduped)
        1
        >>> deduped[0]['content']
        'Full content'
    """
    log.info("Deduplicating %d items", len(items))
    
    # Group items by URL
    url_groups = {}
    for item in items:
        url = item.get("url", "").strip()
        if not url:
            log.warning("Item without URL: %s", item.get("title", "Unknown"))
            continue
            
        if url not in url_groups:
            url_groups[url] = []
        url_groups[url].append(item)
    
    # Select best item for each URL
    deduplicated = []
    duplicates_removed = 0
    
    for url, duplicate_items in url_groups.items():
        if len(duplicate_items) == 1:
            deduplicated.append(duplicate_items[0])
        else:
            # Find the most complete item (most non-empty fields)
            best_item = max(duplicate_items, key=lambda x: sum(1 for v in x.values() if v and str(v).strip()))
            deduplicated.append(best_item)
            duplicates_removed += len(duplicate_items) - 1
            
            log.debug("Deduplicated URL '%s': kept item with %d fields, discarded %d items", 
                     url[:50], sum(1 for v in best_item.values() if v and str(v).strip()), 
                     len(duplicate_items) - 1)
    
    log.info("Deduplication complete: %d items -> %d items (removed %d duplicates)", 
             len(items), len(deduplicated), duplicates_removed)
    
    return deduplicated


def improved_group_items_by_location_price(items: list[dict]) -> dict[str, list[dict]]:
    """Improved grouping function with deduplication and stable hash generation.
    
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
        location = "Unknown"
        price = "Unknown"
        
        # Extract location with robust parsing
        try:
            location = extract_location_from_content(content)
            if location != "Unknown":
                extraction_stats['location_success'] += 1
                log.debug("✓ Location extracted: '%s' for %s", location, item['title'])
            else:
                extraction_stats['location_failed'] += 1
                log.warning("✗ Failed to extract location for %s", item['title'])
        except Exception as e:
            extraction_stats['location_failed'] += 1
            log.error("✗ Error extracting location for %s: %s", item['title'], e)
        
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
        
        # Store extracted data
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
        'unknown_groups': 0
    }
    
    for i, data in enumerate(item_data):
        location = data['location']
        price = data['price']
        item_title = data['item']['title']
        
        # Create grouping key
        key = (location, price)
        
        # Log grouping decision
        if location == "Unknown" or price == "Unknown":
            log.debug("Item '%s' has unknown location/price: Location='%s', Price='%s'", 
                     item_title, location, price)
            grouping_stats['unknown_groups'] += 1
        
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
    log.info("Grouping results - Total groups: %d, Items grouped: %d, Unknown groups: %d",
             grouping_stats['total_groups'], grouping_stats['items_grouped'], 
             grouping_stats['unknown_groups'])
    
    # Step 5: Generate stable hash keys for each group
    final_groups = {}
    hash_generation_stats = {
        'successful': 0,
        'failed': 0
    }
    
    for (location, price), items_in_group in groups_by_location_price.items():
        try:
            # Generate stable hash key for the entire group
            group_key_hash = generate_stable_group_key(location, price, items_in_group)
            final_groups[group_key_hash] = items_in_group
            hash_generation_stats['successful'] += 1
            
            log.debug("✓ Generated stable hash key for group: Location='%s', Price='%s', Items=%d, Hash=%s",
                     location, price, len(items_in_group), group_key_hash[:8])
            
            # Log item titles in this group for debugging
            item_titles = [item['title'] for item in items_in_group]
            log.debug("Group items: %s", item_titles)
            
        except Exception as e:
            hash_generation_stats['failed'] += 1
            log.error("✗ Failed to generate hash key for group (Location='%s', Price='%s'): %s",
                     location, price, e)
    
    # Log final statistics
    log.info("Final grouping results - Hash keys generated: %d successful, %d failed",
             hash_generation_stats['successful'], hash_generation_stats['failed'])
    log.info("Total groups created: %d from %d deduplicated items", 
             len(final_groups), len(deduplicated_items))
    
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
            category_items = categorized[category]
            sections.append(f"__**{get_category_form(category, len(category_items))}:**__")
            for item in category_items:
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
    """Dynamic view with category buttons for grouped items."""
    def __init__(self, items: list[dict], location: str, price: str, timeout: float = None, 
                 include_close_button: bool = False):
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
        
        # Add close button only if explicitly requested (for ephemeral messages)
        if include_close_button:
            self.add_item(ClosePaneButton())


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
                    FROM items WHERE pid=?
                """, (pid,))
                row = await cursor.fetchone()
                
                if row:
                    msg_id, ch_id = row
                    if msg_id and ch_id:
                        log.info("  │  ├─ Found message in database: %d in channel %d", msg_id, ch_id)
                        
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


def generate_group_content_hash(items: list[dict]) -> str:
    """Generate a more reliable hash for a group of items.
    Uses item URLs for ordering and includes all item data.
    """
    if not items:
        return hashlib.md5(b"empty_group").hexdigest()
    
    # Sort items by URL for consistent ordering
    sorted_items = sorted(items, key=lambda x: x.get("url", ""))
    
    # Generate hash for each item
    item_hashes = []
    for item in sorted_items:
        item_hash = generate_content_hash(item)
        item_hashes.append(item_hash)
    
    # Create group hash
    combined = "||".join(item_hashes)
    group_hash = hashlib.md5(combined.encode('utf-8')).hexdigest()
    
    log.debug("Generated group hash: %s from %d items", group_hash[:8], len(items))
    
    return group_hash


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


async def edit_existing_group_message(channel, stored_group: dict, group_key: tuple[str, str], 
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
            location, price = group_key
            updated_embed, updated_view = await create_grouped_embed(group_key, all_items)
            
            # Edit the message
            log.info("    - Editing Discord message...")
            await existing_msg.edit(embed=updated_embed, view=updated_view)
            log.info("    ✅ Successfully edited grouped message %s with %d items", msg_id, len(all_items))
            
            # Update stored data with all items
            log.info("    - Updating stored group data...")
            group_key_hash = generate_stable_group_key(location, price, all_items)
            await update_stored_group_data(group_key_hash, location, price, all_items, msg_id, ch_id)
            
            # Update all items in the group to reference the updated message
            log.info("    - Updating item message references...")
            for item in all_items:
                pid = urlparse(item["url"]).path.strip("/").replace("/", "-") or item["url"]
                await update_item_message_info(pid, msg_id, ch_id)
            
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
    log.info("🔧 EDIT MESSAGE DEBUG END (FAILED)")
    return False


async def process_grouped_items(channel, group_key: tuple[str, str], items_in_group: list[dict]) -> bool:
    """Process grouped items with detailed debug logging."""
    location, price = group_key
    
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
                
                # Find items in current items that match stored titles
                current_titles = {item["title"] for item in items_in_group}
                missing_titles = set(stored_titles) - current_titles
                
                if missing_titles:
                    log.info("  - Missing items from current fetch: %s", list(missing_titles))
                    # Try to find these items in the database
                    log.info("  - Retrieving missing items from database...")
                    async with aiosqlite.connect(DB) as db:
                        missing_items = []
                        for title in missing_titles:
                            cursor = await db.execute("""
                                SELECT id, url, title, content, price, rarity, image, images, content_hash
                                FROM items WHERE title=?
                            """, (title,))
                            row = await cursor.fetchone()
                            if row:
                                images_data = json.loads(row[7]) if row[7] else []
                                item_data = {
                                    "pid": row[0],
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
                                missing_items.append(item_data)
                                log.info("    ✅ Found missing item: %s", title)
                            else:
                                log.warning("    ❌ Missing item not found in database: %s", title)
                        
                        if missing_items:
                            log.info("  - Adding %d missing items from database to group", len(missing_items))
                            items_in_group.extend(missing_items)
                            log.info("  - New total items: %d", len(items_in_group))
                            log.info("  - Final items: %s", [item.get("title", "Unknown") for item in items_in_group])
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
            await store_item(item, grouped_msg.id, channel.id)
        
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


async def safe_post_grouped_embed(channel, group_key: tuple[str, str], items_in_group: list[dict]) -> bool:
    """Safely post a grouped embed with proper locking and duplicate prevention with comprehensive debugging."""
    global posting_lock
    
    # Comprehensive debugging start
    location, price = group_key
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
                log.info("  │  └─ Will create new message instead")
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
                    
                    # Find items in current items that match stored titles
                    current_titles = {item["title"] for item in items_in_group}
                    missing_titles = set(stored_titles) - current_titles
                    
                    if missing_titles:
                        log.info("Missing items from current fetch: %s", list(missing_titles))
                        # Try to find these items in the database
                        async with aiosqlite.connect(DB) as db:
                            missing_items = []
                            for title in missing_titles:
                                cursor = await db.execute("""
                                    SELECT id, url, title, content, price, rarity, image, images, content_hash
                                    FROM items WHERE title=?
                                """, (title,))
                                row = await cursor.fetchone()
                                if row:
                                    images_data = json.loads(row[7]) if row[7] else []
                                    item_data = {
                                        "pid": row[0],
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
                                    missing_items.append(item_data)
                            
                            if missing_items:
                                log.info("Adding %d missing items from database to group", len(missing_items))
                                items_in_group.extend(missing_items)
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
            all_current_items = []
            
            for post in posts:
                pid = urlparse(post["url"]).path.strip("/").replace("/", "-") or post["url"]
                
                # Store all current items for group checking
                post["pid"] = pid
                all_current_items.append(post)
                
                if await has_item_changed(pid, post):
                    has_new_changes = True
                    changed_items.append(post)
            
            # Always check all current items for potential group updates
            if changed_items or all_current_items:
                log.info("Checking groups - Changed items: %d, All current items: %d", 
                         len(changed_items), len(all_current_items))
                
                # Group ALL current items by Location and Price to check for group updates
                all_groups = improved_group_items_by_location_price(all_current_items)
                
                # Also group changed items separately for new group creation
                changed_groups = improved_group_items_by_location_price(changed_items) if changed_items else {}
                
                # Combine both: process all groups but prioritize changed ones
                # Note: all_groups should take precedence to avoid single-item changed groups
                # overriding multi-item groups
                groups = {**changed_groups, **all_groups}
                
                log.info("Total groups to process: %d", len(groups))
                
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
                        
                        # Check if this item is already part of a grouped message
                        async with aiosqlite.connect(DB) as db:
                            cursor = await db.execute("""
                                SELECT gp.group_key, gp.discord_message_id, gp.discord_channel_id
                                FROM grouped_posts gp
                                WHERE EXISTS (
                                    SELECT 1 
                                    FROM json_each(gp.item_titles) 
                                    WHERE value = ?
                                )
                                LIMIT 1
                            """, (item["title"],))
                            group_result = await cursor.fetchone()
                            
                            if group_result:
                                log.info("Item %s is already part of grouped message %s, skipping individual update", 
                                        item["title"], group_result[0][:8])
                                continue
                        
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

    # Create test embed with pluralized categories
    categories = get_categories_from_items(sample_items)
    category_list = ", ".join([f"{pluralize_category(cat)} ({len(items)})" for cat, items in categories.items()])
    
    embed = discord.Embed(
        title="🧪 Category Buttons Test",
        description=f"This is a test of the dynamic category buttons system with proper pluralization.\n\n**Available Categories:** {category_list}\n\nClick any category button below to see items from that category!",
        color=discord.Color.purple()
    )
    
    embed.add_field(name="Sample Items", value=f"• {sample_items[0]['title']}\n• {sample_items[1]['title']}\n• {sample_items[2]['title']}", inline=False)
    embed.set_footer(text="AQW Daily Gift - Test Command")
    
    # Create category buttons view
    view = CategoryButtonsView(sample_items, "Dragon Lair", "1000 AC")
    
    await interaction.followup.send(embed=embed, view=view)


@bot.tree.command(name="testpluralization", description="Test dynamic category pluralization")
async def testpluralization(interaction: discord.Interaction):
    """Test command to demonstrate dynamic singular/plural category forms."""
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        return

    # Test categories with different item counts
    test_scenarios = [
        # (category, count, expected_behavior)
        ("Weapon", 1, "Should be singular"),
        ("Weapon", 3, "Should be plural"),
        ("Pet", 1, "Should be singular"),
        ("Pet", 5, "Should be plural"),
        ("Helm", 1, "Should be singular"),
        ("Helm", 2, "Should be plural"),
        ("Misc", 1, "Should always be 'Miscellaneous'"),
        ("Misc", 10, "Should always be 'Miscellaneous'"),
        ("Axe", 1, "Should be singular"),
        ("Axe", 4, "Should be plural"),
        ("Knife", 1, "Should be singular (irregular)"),
        ("Knife", 3, "Should be 'Knives' (irregular)"),
        ("Box", 1, "Should be singular"),
        ("Box", 2, "Should be 'Boxes' (regular rule)"),
        ("City", 1, "Should be singular"),
        ("City", 3, "Should be 'Cities' (y→ies rule)"),
        ("Wolf", 1, "Should be singular"),
        ("Wolf", 2, "Should be 'Wolves' (f→ves rule)"),
    ]
    
    embed = discord.Embed(
        title="🔤 Dynamic Category Pluralization Test",
        description="Demonstrating singular/plural forms based on item count:",
        color=discord.Color.gold()
    )
    
    # Create sections for different types of categories
    main_categories = []
    weapon_types = []
    irregular_forms = []
    regular_rules = []
    special_cases = []
    
    for category, count, behavior in test_scenarios:
        result = get_category_form(category, count)
        scenario = f"**{category}** ({count} item{'s' if count != 1 else ''}) → **{result}**\n*{behavior}*"
        
        if category in ["Weapon", "Armor", "Helm", "Cape", "Pet"]:
            main_categories.append(scenario)
        elif category in ["Axe", "Sword", "Bow", "Dagger"]:
            weapon_types.append(scenario)
        elif category in ["Knife", "Wolf"]:
            irregular_forms.append(scenario)
        elif category in ["Box", "City"]:
            regular_rules.append(scenario)
        elif category in ["Misc"]:
            special_cases.append(scenario)
    
    # Add sections
    sections = [
        ("📋 Main Categories", main_categories),
        ("⚔️ Weapon Types", weapon_types),
        ("🎯 Irregular Forms", irregular_forms),
        ("📝 Regular Rules", regular_rules),
        ("⭐ Special Cases", special_cases)
    ]
    
    for section_name, scenarios in sections:
        if scenarios:
            embed.add_field(
                name=section_name,
                value="\n\n".join(scenarios),
                inline=False
            )
    
    # Add usage examples
    embed.add_field(
        name="💡 Usage Examples",
        value="```python\n# Get correct form based on item count\ncategory_name = get_category_form('Weapon', item_count)\n\n# In your code:\nbutton_label = f'{get_category_form('Sword', len(swords))} ({len(swords)})'\n# Results: 'Sword (1)' or 'Swords (5)'\n\nembed_title = f'{get_category_form('Pet', len(pets))} Collection'\n# Results: 'Pet Collection' or 'Pets Collection'\n```",
        inline=False
    )
    
    embed.set_footer(text="AQW Daily Gift - Dynamic Pluralization Test")
    
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="testgrouping", description="Test improved item grouping functionality")
async def testgrouping(interaction: discord.Interaction):
    """Test command to demonstrate the improved grouping system with debugging."""
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        return

    # Create test items with various content formats and edge cases
    test_items = [
        {
            "title": "Dragon Sword of Fire",
            "url": "https://example.com/dragon-sword",
            "content": "__**Location:**__\nDragon Lair\n\n__**Price:**__\n1500 AC\n\n__**Rarity:**__\nEpic\n\nA powerful sword forged in dragon fire.",
            "images": ["https://i.imgur.com/dragon-sword.jpg"]
        },
        {
            "title": "Dragon Shield",
            "url": "https://example.com/dragon-shield",
            "content": "**Location:** Dragon Lair\n\n**Price:** 1500 AC\n\n**Rarity:** Epic\n\nA sturdy shield made from dragon scales.",
            "images": ["https://i.imgur.com/dragon-shield.jpg"]
        },
        {
            "title": "Flame Bow",
            "url": "https://example.com/flame-bow", 
            "content": "Location: Volcano Peak\n\nPrice: 1200 AC\n\nRarity: Rare\n\nA bow that shoots flaming arrows.",
            "images": ["https://i.imgur.com/flame-bow.jpg"]
        },
        {
            "title": "Mystic Staff",
            "url": "https://example.com/mystic-staff",
            "content": "Found in the Tower of Magic\n\nCost: 1000 Gold Coins\n\nAn enchanted staff with mystical powers.",
            "images": ["https://i.imgur.com/mystic-staff.jpg"]
        },
        {
            "title": "Steel Helmet",
            "url": "https://example.com/steel-helmet",
            "content": "Reward from the Guard Captain Quest\n\nPrice: N/A\n\nA sturdy steel helmet for protection.",
            "images": ["https://i.imgur.com/steel-helmet.jpg"]
        },
        {
            "title": "Lucky Pet",
            "url": "https://example.com/lucky-pet",
            "content": "From: Lucky Draw Event\n\nPrice: Free\n\nA cute companion that brings good luck.",
            "images": ["https://i.imgur.com/lucky-pet.jpg"]
        }
    ]

    # Test the grouping function
    log.info("Testing improved grouping function with %d items", len(test_items))
    
    try:
        groups = improved_group_items_by_location_price(test_items)
        
        embed = discord.Embed(
            title="🧪 Improved Grouping Test Results",
            description=f"Successfully grouped {len(test_items)} items into {len(groups)} groups",
            color=discord.Color.green()
        )
        
        # Add group details
        for i, (group_key_hash, items_in_group) in enumerate(groups.items(), 1):
            # Extract location and price from first item
            first_item = items_in_group[0]
            content = first_item.get("content", "")
            
            # Use the improved extraction functions
            location = extract_location_from_content(content)
            price = extract_price_from_content(content)
            
            # Get item titles
            item_titles = [item['title'] for item in items_in_group]
            
            embed.add_field(
                name=f"Group {i}: {location} - {price}",
                value=f"Items: {len(items_in_group)}\nTitles: {', '.join(item_titles)}\nHash: {group_key_hash[:8]}",
                inline=False
            )
        
        # Add statistics
        embed.add_field(
            name="📊 Statistics",
            value=f"• Total items processed: {len(test_items)}\n• Groups created: {len(groups)}\n• Average items per group: {len(test_items)/len(groups):.1f}",
            inline=False
        )
        
        embed.set_footer(text="AQW Daily Gift - Improved Grouping Test")
        
        await interaction.followup.send(embed=embed)
        
        # Log results
        log.info("Grouping test completed successfully: %d groups created from %d items", len(groups), len(test_items))
        
    except Exception as e:
        log.error("Grouping test failed: %s", e)
        embed = discord.Embed(
            title="❌ Grouping Test Failed",
            description=f"Error during grouping test: {e}",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)


@bot.tree.command(name="testgroupupdate", description="Test group update functionality")
async def testgroupupdate(interaction: discord.Interaction):
    """Test command to demonstrate group update detection and editing."""
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        return

    # Create initial sample items
    initial_items = [
        {
            "title": "Test Sword",
            "url": "https://example.com/test-sword",
            "price": "500 AC",
            "content": "__**Location:**__\nTest Area\n\n__**Price:**__\n500 AC\n\n__**Rarity:**__\nRare",
            "images": ["https://i.imgur.com/sword1.jpg"]
        },
        {
            "title": "Test Shield", 
            "url": "https://example.com/test-shield",
            "price": "500 AC",
            "content": "__**Location:**__\nTest Area\n\n__**Price:**__\n500 AC\n\n__**Rarity:**__\nRare",
            "images": ["https://i.imgur.com/shield1.jpg"]
        }
    ]

    # Create initial group message
    embed = discord.Embed(
        title="🧪 Group Update Test - Initial",
        description="This is the initial group message. Use the command again to see updates!",
        color=discord.Color.blue()
    )
    
    view = CategoryButtonsView(initial_items, "Test Area", "500 AC")
    
    await interaction.followup.send(embed=embed, view=view)
    
    # Log for demonstration
    log.info("Test group created - run command again to test updates")


@bot.tree.command(name="simulategroupchange", description="Simulate a change in existing group items")
async def simulategroupchange(interaction: discord.Interaction):
    """Simulate changing items in a group to test update detection."""
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        return

    # Modified items (simulating changes)
    changed_items = [
        {
            "title": "Test Sword",  # Same title
            "url": "https://example.com/test-sword",
            "price": "600 AC",  # Changed price
            "content": "__**Location:**__\nTest Area\n\n__**Price:**__\n600 AC\n\n__**Rarity:**__\nEpic",  # Changed rarity
            "images": ["https://i.imgur.com/sword2.jpg"]  # Changed image
        },
        {
            "title": "Test Shield",
            "url": "https://example.com/test-shield", 
            "price": "600 AC",  # Changed price
            "content": "__**Location:**__\nTest Area\n\n__**Price:**__\n600 AC\n\n__**Rarity:**__\nEpic",  # Changed rarity
            "images": ["https://i.imgur.com/shield2.jpg"]  # Changed image
        },
        {
            "title": "Test Helmet",  # New item added
            "url": "https://example.com/test-helmet",
            "price": "600 AC",
            "content": "__**Location:**__\nTest Area\n\n__**Price:**__\n600 AC\n\n__**Rarity:**__\nEpic",
            "images": ["https://i.imgur.com/helmet1.jpg"]
        }
    ]

    embed = discord.Embed(
        title="🔄 Simulated Group Changes",
        description="Simulating changes to existing group:\n\n• Price changed: 500 AC → 600 AC\n• Rarity changed: Rare → Epic\n• Images updated\n• New item added: Test Helmet",
        color=discord.Color.orange()
    )
    
    embed.add_field(name="Changes Detected", value="The bot should now update the existing group message instead of creating a new one.", inline=False)
    
    await interaction.followup.send(embed=embed)
    
    # Log for demonstration
    log.info("Simulated group changes - bot should update existing message on next check")


@bot.tree.command(name="debuggroup", description="Debug a specific group by key")
@commands.has_permissions(manage_messages=True)
async def debug_group(interaction: discord.Interaction, group_key: str = None):
    """Debug a specific group or show all groups with detailed information."""
    try:
        await interaction.response.defer(thinking=True)
        
        if not group_key:
            # Show all groups
            async with aiosqlite.connect(DB) as db:
                async with db.execute("""
                    SELECT group_key, location, price, item_titles, categories, 
                           discord_message_id, discord_channel_id, last_updated
                    FROM grouped_posts ORDER BY last_updated DESC LIMIT 10
                """) as cur:
                    rows = await cur.fetchall()
            
            if not rows:
                await interaction.followup.send("No grouped messages found in database.")
                return
            
            embed = discord.Embed(
                title="🔍 Recent Groups (Last 10)",
                description="Use `/debuggroup key:<group_key>` for detailed info",
                color=discord.Color.blue()
            )
            
            for row in rows:
                group_key_full, location, price, item_titles_json, categories_json, msg_id, ch_id, last_updated = row
                item_titles = json.loads(item_titles_json) if item_titles_json else []
                
                # Check if message exists
                channel = bot.get_channel(ch_id)
                msg_status = "❓ Unknown"
                if channel:
                    try:
                        await channel.fetch_message(msg_id)
                        msg_status = "✅ Found"
                    except discord.NotFound:
                        msg_status = "❌ Not Found"
                    except discord.Forbidden:
                        msg_status = "🔒 No Permission"
                    except Exception:
                        msg_status = "⚠️ Error"
                else:
                    msg_status = "📵 Channel Not Found"
                
                embed.add_field(
                    name=f"Group {group_key_full[:8]}",
                    value=f"**Location:** {location}\n"
                          f"**Items:** {len(item_titles)}\n"
                          f"**Message:** {msg_status}\n"
                          f"**Last Updated:** {last_updated}",
                    inline=False
                )
            
            await interaction.followup.send(embed=embed)
        else:
            # Debug specific group
            stored_group = await get_stored_group(group_key)
            
            if not stored_group:
                await interaction.followup.send(f"Group `{group_key[:8]}` not found in database.")
                return
            
            # Create detailed debug embed
            embed = discord.Embed(
                title=f"🔍 Group Debug: {group_key[:8]}",
                color=discord.Color.orange()
            )
            
            # Basic info
            embed.add_field(
                name="📊 Basic Info",
                value=f"**Location:** {stored_group.get('location', 'Unknown')}\n"
                      f"**Price:** {stored_group.get('price', 'Unknown')}\n"
                      f"**Message ID:** {stored_group.get('discord_message_id', 'Unknown')}\n"
                      f"**Channel ID:** {stored_group.get('discord_channel_id', 'Unknown')}\n"
                      f"**Content Hash:** {stored_group.get('content_hash', 'None')[:8] if stored_group.get('content_hash') else 'None'}",
                inline=False
            )
            
            # Items
            item_titles = stored_group.get('item_titles', [])
            items_text = "\n".join([f"• {title}" for title in item_titles[:10]])
            if len(item_titles) > 10:
                items_text += f"\n... and {len(item_titles) - 10} more"
            
            embed.add_field(
                name=f"📦 Items ({len(item_titles)})",
                value=items_text or "No items",
                inline=False
            )
            
            # Categories
            categories = stored_group.get('categories', [])
            categories_text = ", ".join(categories) if categories else "No categories"
            
            embed.add_field(
                name="🏷️ Categories",
                value=categories_text,
                inline=False
            )
            
            # Check message status
            msg_id = stored_group.get('discord_message_id')
            ch_id = stored_group.get('discord_channel_id')
            if msg_id and ch_id:
                channel = bot.get_channel(ch_id)
                if channel:
                    try:
                        msg = await channel.fetch_message(msg_id)
                        embed.add_field(
                            name="✅ Message Status",
                            value=f"Message exists and was created {msg.created_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                  f"Jump: [Click to view]({msg.jump_url})",
                            inline=False
                        )
                    except discord.NotFound:
                        embed.add_field(
                            name="❌ Message Status",
                            value="Message not found - it may have been deleted",
                            inline=False
                        )
                    except discord.Forbidden:
                        embed.add_field(
                            name="🔒 Message Status",
                            value="No permission to view message",
                            inline=False
                        )
                else:
                    embed.add_field(
                        name="📵 Message Status",
                        value="Channel not found",
                        inline=False
                    )
            
            await interaction.followup.send(embed=embed)
            
    except Exception as e:
        log.error("Error in debug_group: %s", e)
        await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)


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


@bot.tree.command(name="checkgroups", description="Check the status of all grouped messages")
@commands.has_permissions(manage_messages=True)
async def check_groups(interaction: discord.Interaction):
    """Check the status of all grouped messages in the database."""
    try:
        await interaction.response.defer(thinking=True)
        
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
            
            field_value = (
                f"**Location:** {location}\n"
                f"**Items:** {len(item_titles)} ({', '.join(item_titles[:3])}{'...' if len(item_titles) > 3 else ''})\n"
                f"**Message:** {msg_exists} (ID: {msg_id})\n"
                f"**Last Updated:** {last_updated}"
            )
            
            embed.add_field(
                name=f"Group {group_key[:8]}",
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
        log.error("  └─ DISMISS COMMAND FAILED (ERROR)")
        await interaction.followup.send(f"❌ Error dismissing message: {e}", ephemeral=True)


@dismiss.error
async def dismiss_error(interaction: discord.Interaction, error):
    """Handle errors for the dismiss command."""
    if isinstance(error, commands.MissingPermissions):
        await interaction.response.send_message("❌ You need 'Manage Messages' permission to use this command.", ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ An error occurred: {error}", ephemeral=True)


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
