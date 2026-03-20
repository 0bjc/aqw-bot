from __future__ import annotations

import asyncio
import logging
import os
import re
import textwrap
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import aiosqlite
import requests
from bs4 import BeautifulSoup

import discord
from discord.ext import commands, tasks

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1484113318095622315"))

WIKI_BASE = "https://aqwwiki.wikidot.com"
RECENT_URL_HTTP = "http://aqwwiki.wikidot.com/system:recent-changes"
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
async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS posted (id TEXT PRIMARY KEY)")
        await db.commit()


async def is_posted(pid: str) -> bool:
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT 1 FROM posted WHERE id=?", (pid,)) as cur:
            return await cur.fetchone() is not None


async def mark_posted(pid: str):
    async with aiosqlite.connect(DB) as db:
        await db.execute("INSERT OR IGNORE INTO posted VALUES (?)", (pid,))
        await db.commit()


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
        return datetime(year, month, day, hour, minute, second)

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
        return datetime(year, month, day, hour, minute, second)

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
    return False


def _wrap_lines(text: str, width: int = WRAP_WIDTH) -> str:
    out: list[str] = []
    for line in text.splitlines():
        line = line.rstrip()
        if not line.strip():
            out.append("")
            continue
        wrapped = textwrap.wrap(
            line,
            width=width,
            replace_whitespace=False,
            break_long_words=False,
        )
        out.extend(wrapped if wrapped else [line])
    return "\n".join(out).strip()


def _extract_imgur_image(content_el: BeautifulSoup) -> str | None:
    # Prefer actual item image (usually i.imgur.com / imgur.com) but skip thumbnails/icons.
    for img in content_el.select("img[src]"):
        src = img.get("src")
        if not src:
            continue
        s = src.lower()
        if any(x in s for x in ("pixel", "spacer", "icon", "thumb")):
            continue
        full = _make_absolute(src, None)
        if "imgur.com" in full.lower():
            return full
    return None


def _clean_item_text(raw_text: str) -> tuple[str, str]:
    """
    Returns: (cleaned_description_text, price)

    Removes:
    - Sellback lines
    - Thanks to line
    - Also see section
    - Tag UI (already removed from HTML, but this keeps it safe)

    Keeps the item flavor by removing label prefixes (Description:, Rarity Description:)
    and leaving the text after the prefix.
    """
    lines = [ln.strip() for ln in raw_text.splitlines()]
    lines = [ln for ln in lines if ln]

    price = "N/A"
    keep: list[str] = []
    skip_also = False

    for ln in lines:
        low = ln.lower()

        if skip_also:
            # end also-see when hitting section headers or end
            if (
                low.startswith("notes")
                or low.startswith("thanks to")
                or low.startswith("location:")
                or low.startswith("price:")
            ):
                skip_also = False
            else:
                continue

        if low.startswith("sellback:"):
            continue
        if low.startswith("thanks to"):
            continue
        if low.startswith("also see"):
            skip_also = True
            continue

        # Extract price and remove it from the main text.
        if low.startswith("price:"):
            price = ln.split(":", 1)[1].strip()
            continue

        # Remove label prefixes but keep the content
        ln = re.sub(r"^(description:)\s*", "", ln, flags=re.IGNORECASE)
        ln = re.sub(r"^(rarity description:)\s*", "", ln, flags=re.IGNORECASE)

        # Sometimes tag list text leaks as many tag tokens; drop obvious tag list line.
        if "system:page-tags/tag/" in ln.lower():
            continue
        if re.fullmatch(r"([a-z0-9_-]+\s*)+", ln.lower()) and "aegift" in raw_text.lower():
            # very defensive; reduces chances of tag token spam
            pass

        keep.append(ln)

    cleaned = "\n".join(keep).strip()
    return cleaned, price


def extract_item_details(page_url: str) -> dict | None:
    try:
        r = requests.get(
            page_url,
            timeout=15,
            headers={"User-Agent": "aqw-wiki-bot/1.0"},
        )
        r.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch %s: %s", page_url, e)
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    if not page_has_aegift(soup):
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

    # Remove tag UI and page-info blocks
    for el in content_el.select(".page-tags, .page-info-bottom, .page-info"):
        el.decompose()
    for a in content_el.select("a[href*='/system:page-tags/tag/']"):
        a.decompose()
    for el in content_el.select("script, style"):
        el.decompose()

    raw_text = content_el.get_text(separator="\n", strip=True)
    cleaned, price = _clean_item_text(raw_text)

    img_url = _extract_imgur_image(content_el)

    if len(cleaned) > MAX_DESC_LENGTH:
        cleaned = cleaned[: MAX_DESC_LENGTH - 3] + "..."

    return {
        "title": title or "Untitled",
        "content": cleaned or "No item info available.",
        "price": price,
        "image": img_url,
        "url": page_url,
    }


def _extract_recent_changes_entries(max_pages: int = 6) -> dict[str, datetime]:
    """
    Get mapping: page_url -> earliest change_time within CHECK_DAYS.
    Follows the 'next' pagination in recent-changes to avoid missing older entries.
    """
    cutoff = datetime.utcnow() - timedelta(days=CHECK_DAYS)
    page_times: dict[str, datetime] = {}

    url: str | None = RECENT_URL_HTTP
    visited: set[str] = set()

    for _ in range(max_pages):
        if not url or url in visited:
            break
        visited.add(url)

        res = requests.get(url, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        any_in_window = False
        for row in soup.select("table tr"):
            cols = row.find_all("td")
            if len(cols) < 3:
                continue

            link = cols[0].find("a")
            if not link:
                continue

            href = link.get("href", "")
            if not href or href.startswith("#"):
                continue

            time_text = cols[2].get_text(strip=True)
            change_time = parse_wiki_time(time_text)
            if not change_time:
                continue

            if change_time < cutoff:
                continue

            any_in_window = True
            page_url = _make_absolute(href).rstrip("/")
            prev = page_times.get(page_url)
            if prev is None or change_time < prev:
                page_times[page_url] = change_time

        if not any_in_window:
            break

        # Locate pagination next
        next_href = None
        next_link = (
            soup.select_one("a[rel='next']")
            or soup.select_one(".wiki-pagination a.next")
            or soup.select_one("li.next a")
            or soup.select_one("a[aria-label*='next']")
        )
        if next_link and next_link.get("href"):
            next_href = next_link.get("href")
        else:
            for a in soup.select("a[href]"):
                label = a.get_text(" ", strip=True).lower()
                if label.startswith("next") and a.get("href"):
                    next_href = a.get("href")
                    break

        if not next_href:
            break
        url = _make_absolute(next_href).rstrip("/")

    return page_times


def fetch_recent_aegifts() -> list[dict]:
    """
    Fetch all aegift pages modified in the last CHECK_DAYS.
    Sorted oldest -> newest.
    """
    page_times = _extract_recent_changes_entries(max_pages=8)
    if not page_times:
        return []

    sorted_pages = sorted(page_times.items(), key=lambda kv: kv[1])

    results: list[dict] = []
    seen_ids: set[str] = set()

    for page_url, _t in sorted_pages:
        pid = urlparse(page_url).path.strip("/").replace("/", "-") or page_url
        if pid in seen_ids:
            continue

        details = extract_item_details(page_url)
        if not details:
            continue

        results.append({"id": pid, **details})
        seen_ids.add(pid)

        if len(results) >= MAX_POSTS_PER_RUN:
            break

    return results


def create_embed(post: dict) -> discord.Embed:
    wrapped_content = _wrap_lines(post["content"])
    price_line = f"Price: {post.get('price', 'N/A')}"

    desc = f"{wrapped_content}\n\n{price_line}\n\n[View on Wiki]({post['url']})"
    if len(desc) > 4096:
        desc = desc[:4090] + "..."

    embed = discord.Embed(
        title=post["title"],
        description=desc,
        url=post["url"],
        color=0xFF4500,
    )
    if post.get("image"):
        embed.set_image(url=post["image"])
    embed.set_footer(text="AQW AE Gift Tracker")
    return embed


# ---------------- LOOP ----------------
@tasks.loop(minutes=10)
async def check_posts():
    await bot.wait_until_ready()

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        log.warning("Channel %s not found", CHANNEL_ID)
        return

    posts = await asyncio.to_thread(fetch_recent_aegifts)
    for post in posts:
        if await is_posted(post["id"]):
            continue
        try:
            await channel.send(embed=create_embed(post))
            await mark_posted(post["id"])
            log.info("Posted %s", post["title"])
        except discord.DiscordException as e:
            log.error("Failed to post %s: %s", post["id"], e)


# ---------------- COMMAND ----------------
@bot.tree.command(name="latestdrops", description="Check latest AE gift pages")
async def latestdrops(interaction: discord.Interaction):
    await interaction.response.defer()

    posts = await asyncio.to_thread(fetch_recent_aegifts)
    if not posts:
        await interaction.followup.send("No recent AE gifts found in the last 7 days.")
        return

    # posts are oldest->newest; newest is the last one
    await interaction.followup.send(embed=create_embed(posts[-1]))


# ---------------- READY ----------------
@bot.event
async def on_ready():
    log.info("Logged in as %s", bot.user)
    await init_db()
    if not check_posts.is_running():
        check_posts.start()
    await bot.tree.sync()
    log.info("Commands synced.")


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
