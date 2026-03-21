from __future__ import annotations

import asyncio
import logging
import os
import re
import textwrap
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse, parse_qs

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

    Output format (exact labels, all label parts in bold):
    - Location:
    - Price OR Dropped by / Merge the following (when Price is N/A)
    - Rarity:
    - Note: (only if note exists)
    """
    text = raw_text.replace("\r\n", "\n").replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    # Remove unwanted Sellback section entirely.
    # Some pages use "Sellback:" while others may include spacing like "Sell back:".
    text = re.sub(
        r"Sell\s*back\s*:\s*.+?(?=(?:Rarity:\s*)|(?:Description:\s*)|(?:Notes:\s*)|(?:Also see:\s*)|(?:Thanks to\s*)|$)",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    def _norm(val: str) -> str:
        val = re.sub(r"system:page-tags/tag/[^ \n]+", "", val, flags=re.IGNORECASE)
        # Keep newlines so multi-value fields (like multiple locations) can be listed.
        val = re.sub(r"[ \t]+", " ", val)
        val = re.sub(r"\n{3,}", "\n\n", val)
        val = val.strip()
        return val

    def _format_list(val: str) -> str:
        """
        Convert a multi-value field into Discord-friendly line items.
        - If the value already contains newlines, keep one entry per non-empty line.
        - Otherwise, split on commas as a fallback.
        """
        v = (val or "").strip()
        if not v or v.upper() == "N/A":
            return "N/A"

        # Normalize whitespace but preserve line breaks.
        v = re.sub(r"[ \t]+", " ", v).strip()
        lines = [ln.strip() for ln in v.split("\n") if ln.strip()]
        if len(lines) > 1:
            return "\n".join(lines)

        # Fallback: comma-separated values.
        if "," in v:
            parts = [p.strip() for p in v.split(",") if p.strip()]
            return "\n".join(parts) if parts else v

        return v

    # Capture fields between wikidot label markers.
    # These patterns are based on the AQW wiki item pages format.
    loc = "N/A"
    price = "N/A"
    rarity = "N/A"
    dropped_by = None
    merge_following = None
    note = None

    # Label matching on Wikidot can vary slightly (e.g. "Location" vs "Locations",
    # whitespace before ":" etc.), so we keep the patterns tolerant.
    m_loc = re.search(
        r"Locations?\s*:?\s*(?P<val>.+?)\s*(?=(?:Price\s*:?)|(?:Dropped by\s*:?)|(?:Rarity\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_loc:
        loc = _norm(m_loc.group("val"))

    m_price = re.search(
        r"Price\s*:?\s*(?P<val>.+?)\s*(?=(?:Rarity\s*:?)|(?:Dropped by\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_price:
        price = _norm(m_price.group("val"))

    # These labels appear on some item pages when Price is "N/A".
    m_dropped = re.search(
        r"Dropped by\s*:?\s*(?P<val>.+?)\s*(?=(?:Merge the following\s*:?)|(?:Rarity\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_dropped:
        candidate = _norm(m_dropped.group("val"))
        if candidate and candidate.lower() not in {"n/a", "na"}:
            dropped_by = candidate

    m_merge = re.search(
        r"Merge the following\s*:?\s*(?P<val>.+?)\s*(?=(?:Rarity\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_merge:
        candidate = _norm(m_merge.group("val"))
        if candidate and candidate.lower() not in {"n/a", "na"}:
            merge_following = candidate

    m_rarity = re.search(
        r"Rarity\s*:?\s*(?P<val>.+?)\s*(?=(?:Rarity Description\s*:?)|(?:Description\s*:?)|(?:Notes\s*:?)|(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_rarity:
        rarity = _norm(m_rarity.group("val"))

    m_note = re.search(
        r"Notes?\s*:?\s*(?P<val>.+?)(?=(?:Also see\s*:?)|(?:Thanks to\s*:?)|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_note:
        candidate = _norm(m_note.group("val"))
        if candidate and candidate.lower() not in {"n/a", "na"}:
            note = candidate

    def _price_is_na(p: str) -> bool:
        p_norm = (p or "").strip()
        return p_norm.upper() == "N/A" or p_norm.upper().startswith("N/A")

    # Assemble structured Discord description.
    parts: list[str] = [
        f"**Locations:**\n{_format_list(loc)}",
    ]

    if _price_is_na(price):
        # User preference: when Price is N/A, replace it with Dropped by / Merge the following (if present).
        if dropped_by:
            parts.append(f"**Dropped by:**\n{_format_list(dropped_by)}")
        if merge_following:
            parts.append(f"**Merge the following:**\n{_format_list(merge_following)}")
        # Fallback in case the page doesn't actually include either label.
        if not dropped_by and not merge_following:
            parts.append(f"**Price:**\n{_format_list(price)}")
    else:
        parts.append(f"**Price:**\n{_format_list(price)}")

    parts.append(f"**Rarity:**\n{_format_list(rarity)}")

    if note:
        parts.append(f"**Note:**\n{_format_list(note)}")

    structured = "\n\n".join(parts).strip()
    return structured, price


def extract_item_details(page_url: str) -> dict | None:
    try:
        r = requests.get(
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

    img_url = _extract_imgur_image(content_el)

    if len(cleaned) > MAX_DESC_LENGTH:
        cleaned = cleaned[: MAX_DESC_LENGTH - 3] + "..."

    return {
        "title": title or "Untitled",
        "content": cleaned or "No item info available.",
        "price": price,
        "image": img_url,
        "url": page_url,
        "title_icons": title_icons,
    }


def _extract_recent_changes_entries(max_pages: int = 30) -> dict[str, datetime]:
    """
    Get mapping: page_url -> earliest change_time within CHECK_DAYS.
    Uses Wikidot AJAX pagination to fetch the previous 30 pages.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=CHECK_DAYS)
    page_times: dict[str, datetime] = {}

    log.info("Starting recent changes extraction, cutoff: %s", cutoff)

    # Fetch first page normally
    module_id = None
    try:
        res = requests.get(RECENT_URL_HTTP, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        log.info("Fetching page: %s", RECENT_URL_HTTP)

        # Extract moduleId from the page
        module_id = None
        
        # Debug: Log all div IDs to see what's available
        all_divs = soup.select("div[id]")
        div_ids = [div.get("id") for div in all_divs if div.get("id")]
        log.debug("Found div IDs: %s", div_ids[:10])  # First 10 to avoid spam
        
        # First try: Look for div with id containing "module"
        for div in soup.select("div[id*='module']"):
            module_id = div.get("id")
            log.info("Found module-related div: %s", module_id)
            if module_id and "module" in module_id.lower():
                break
        
        # Second try: Look for any element with data attributes
        if not module_id:
            for element in soup.select("[data-module], [data-module-id], [class*='module']"):
                module_id = element.get("data-module") or element.get("data-module-id") or element.get("class")
                if module_id:
                    if isinstance(module_id, list):
                        module_id = " ".join(module_id)
                    log.info("Found module via attribute: %s", module_id)
                    break
        
        # Third try: Parse from JavaScript - look for recent changes specific patterns
        if not module_id:
            for script in soup.select("script"):
                script_text = script.get_text()
                # Look for recent changes module patterns
                patterns = [
                    r'recent.*changes.*module.*?(\w+)',
                    r'changes.*module.*?(\w+)',
                    r'wikidot.*module.*?(\w+)',
                    r'module.*?id.*?(\w+)',
                    r'id["\']?\s*[:=]\s*["\']?(\w+)',
                ]
                for pattern in patterns:
                    match = re.search(pattern, script_text, re.IGNORECASE)
                    if match:
                        potential_id = match.group(1)
                        if len(potential_id) > 3:  # Avoid short matches
                            module_id = f"wikidot-module-{potential_id}"
                            log.info("Found moduleId in script: %s", module_id)
                            break
                if module_id:
                    break
        
        # Last resort: Generate a common moduleId for recent changes
        if not module_id:
            module_id = "wikidot-module-2788766"  # Common recent changes module ID
            log.info("Using fallback moduleId: %s", module_id)
        
        log.info("Using moduleId: %s", module_id)

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

        log.info("Page 1 (offset 0): %d rows found, %d in window, %d total pages", rows_found, any_in_window, len(page_times))

    except Exception as e:
        log.warning("Failed to fetch first page: %s", e)
        return page_times

    # Fetch remaining pages using AJAX pagination
    for page_num in range(2, max_pages + 1):
        offset = (page_num - 1) * 20  # Each page has 20 rows
        
        # Add 1 second delay between requests
        if page_num > 2:
            time.sleep(1)
        
        log.info("Fetching page %d (offset %d)", page_num, offset)
        
        try:
            ajax_url = f"{WIKI_BASE}/ajax-module-connector.php"
            ajax_data = {
                'moduleName': 'changes/ChangesModule',
                'moduleId': module_id.replace('wikidot-module-', ''),  # Remove prefix for Wikidot
                'offset': str(offset),
                'wikidot_token': 'recent_changes'  # Add token for authentication
            }
            
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "http://aqwwiki.wikidot.com",
                "Referer": "http://aqwwiki.wikidot.com/system:recent-changes",
                "Connection": "keep-alive"
            }
            log.info("AJAX request data: %s", ajax_data)
            res = requests.post(ajax_url, data=ajax_data, timeout=15, headers=headers)
            res.raise_for_status()
            
            log.info("AJAX response status: %s", res.status_code)
            response_json = res.json()
            html_body = response_json.get("body", "")
            
            log.info("AJAX response keys: %s", list(response_json.keys()))
            if "body" in response_json:
                log.info("Body length: %d chars", len(html_body))
                if len(html_body) > 0:
                    log.info("Body preview: %s", html_body[:200])
            
            if not html_body.strip():
                log.info("Empty response body at page %d, stopping", page_num)
                break
            
            # Parse the HTML fragment using the same parser
            soup = BeautifulSoup(html_body, "html.parser")
            
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

            log.info("Page %d (offset %d): %d rows found, %d in window, %d total pages", page_num, offset, rows_found, any_in_window, len(page_times))

            if not any_in_window:
                break

        except Exception as e:
            log.warning("Failed to fetch page %d: %s", page_num, e)
            break

    log.info("Recent changes extraction complete: %d pages found", len(page_times))
    return page_times


def _extract_related_item_links(page_url: str, max_links: int = 25) -> list[str]:
    """
    From a recent-changes page, extract likely internal item page links.
    Skips system pages and returns absolute URLs.
    """
    try:
        r = requests.get(page_url, timeout=15, headers={"User-Agent": "aqw-wiki-bot/1.0"})
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


def fetch_recent_aegifts_fast(limit: int = MAX_POSTS_PER_RUN, newest_first: bool = False, max_pages: int = 5) -> list[dict]:
    """
    Fast version for slash commands - checks fewer pages to avoid timeouts.
    """
    page_times = _extract_recent_changes_entries(max_pages=5)  # Check more pages
    if not page_times:
        log.info("No recent changes found")
        return []

    sorted_pages = sorted(page_times.items(), key=lambda kv: kv[1])
    if newest_first:
        sorted_pages = list(reversed(sorted_pages))

    results: list[dict] = []
    seen_ids: set[str] = set()
    pages_checked = 0

    for page_url, _t in sorted_pages[:max_pages]:  # Limit to max_pages
        pid = urlparse(page_url).path.strip("/").replace("/", "-") or page_url
        if pid in seen_ids:
            continue

        pages_checked += 1
        log.info("Checking page %d/%d: %s", pages_checked, max_pages, page_url)

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
    Fetch aegift pages from the previous 30 pages of recent changes.
    """
    page_times = _extract_recent_changes_entries(max_pages=30)  # Check 30 pages
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
    icons_line = post.get("title_icons")
    if icons_line:
        desc = f"{icons_line}\n\n{wrapped_content}\n\n[View on Wiki]({post['url']})"
    else:
        desc = f"{wrapped_content}\n\n[View on Wiki]({post['url']})"
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

    posts = await asyncio.to_thread(fetch_recent_aegifts, limit=10)  # Check more pages for background
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
    try:
        await interaction.response.defer(thinking=True)
    except discord.NotFound:
        # Interaction token expired / no longer valid (common right after redeploy)
        return

    try:
        # Check the first 5 most recent pages for speed
        posts = await asyncio.wait_for(
            asyncio.to_thread(fetch_recent_aegifts_fast, 1, True, max_pages=5),
            timeout=20  # Longer timeout for more pages
        )
        if not posts:
            await interaction.followup.send("No recent AE gifts found in the last 30 pages.")
            return

        await interaction.followup.send(embed=create_embed(posts[0]))
    except asyncio.TimeoutError:
        await interaction.followup.send("Timed out fetching latest drops. Please try again in a few seconds.")
    except Exception as e:
        log.exception("latestdrops failed: %s", e)
        await interaction.followup.send("Something went wrong while fetching recent AE gifts.")

# ... (rest of the code remains the same)
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
