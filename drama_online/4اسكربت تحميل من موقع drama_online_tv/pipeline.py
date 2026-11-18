"""
================================================================================
  Drama Online Master Pipeline - Automated Media & Server Ingestion System
  Full Pipeline: Sitemap -> Series Details -> Banner -> Episodes -> Embed Servers
================================================================================
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

# Windows Console & VT100 UTF-8 Configuration
if sys.platform == "win32":
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        hStdOut = kernel32.GetStdHandle(-11)
        mode = ctypes.c_ulong()
        kernel32.GetConsoleMode(hStdOut, ctypes.byref(mode))
        mode.value |= 0x0004  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
        kernel32.SetConsoleMode(hStdOut, mode)
    except Exception:
        pass
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:
    from curl_cffi import requests
    HAS_CURL_CFFI = True
except ImportError:
    import requests
    HAS_CURL_CFFI = False

from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
)

# Safe styling for Windows CMD & PowerShell
BOX_STYLE = box.ASCII if sys.platform == "win32" else box.ROUNDED

# ==============================================================================
# Configuration Loader (config.json) with utf-8-sig BOM support
# ==============================================================================
CURRENT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = CURRENT_DIR / "config.json"

DEFAULT_CONFIG = {
    "_comment": "Drama Online Pipeline Configuration - Modify concurrency, delays, and behaviors here.",
    "concurrency": {
        "series_workers": 3,
        "server_workers": 6,
        "sitemap_workers": 10
    },
    "delays_and_timeouts": {
        "inter_episode_delay_seconds": 0.8,
        "request_timeout_seconds": 25,
        "max_retries": 5
    },
    "pipeline_options": {
        "default_mode": "all",
        "test_series_count": 1,
        "skip_servers": False,
        "force_sitemap_refresh": False
    },
    "storage_paths": {
        "series_download_dir": "series_download",
        "sitemap_file": "sitemap.json",
        "logs_dir": "logs",
        "log_file": "pipeline.log"
    },
    "network": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
}


def load_config() -> dict:
    """Load configuration from config.json with support for UTF-8 with BOM."""
    if not CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(DEFAULT_CONFIG, f, indent=2, ensure_ascii=False)
        except Exception:
            pass
        return DEFAULT_CONFIG

    try:
        # Use utf-8-sig to automatically handle Windows Notepad UTF-8 BOM
        with open(CONFIG_FILE, "r", encoding="utf-8-sig") as f:
            user_config = json.load(f)
        merged = DEFAULT_CONFIG.copy()
        for k, v in user_config.items():
            if isinstance(v, dict) and k in merged and isinstance(merged[k], dict):
                merged[k] = {**merged[k], **v}
            else:
                merged[k] = v
        return merged
    except Exception as e:
        print(f"[!] Notice: Could not parse config.json ({e}). Using default configuration.")
        return DEFAULT_CONFIG


CONFIG = load_config()

# Extract settings from CONFIG
CONCURRENCY = CONFIG.get("concurrency", {})
DELAYS = CONFIG.get("delays_and_timeouts", {})
OPTIONS = CONFIG.get("pipeline_options", {})
PATHS = CONFIG.get("storage_paths", {})
NETWORK = CONFIG.get("network", {})

SERIES_DOWNLOAD_DIR = CURRENT_DIR / PATHS.get("series_download_dir", "series_download")
SITEMAP_FILE = CURRENT_DIR / PATHS.get("sitemap_file", "sitemap.json")
LOGS_DIR = CURRENT_DIR / PATHS.get("logs_dir", "logs")
LOG_FILE = LOGS_DIR / PATHS.get("log_file", "pipeline.log")

DEFAULT_SERIES_WORKERS = int(CONCURRENCY.get("series_workers", 1))
DEFAULT_SERVER_WORKERS = int(CONCURRENCY.get("server_workers", 1))
DEFAULT_SITEMAP_WORKERS = int(CONCURRENCY.get("sitemap_workers", 6))

INTER_SERVER_DELAY = float(DELAYS.get("inter_server_delay_seconds", 0.4))
INTER_EPISODE_DELAY = float(DELAYS.get("inter_episode_delay_seconds", 1.2))
INTER_SERIES_DELAY = float(DELAYS.get("inter_series_delay_seconds", 1.5))
REQUEST_TIMEOUT = int(DELAYS.get("request_timeout_seconds", 25))
MAX_RETRIES = int(DELAYS.get("max_retries", 5))

USER_AGENT = NETWORK.get("user_agent", DEFAULT_CONFIG["network"]["user_agent"])

BASE_URL = "https://drama-online.tv"
FIRST_PAGE_URL = "https://drama-online.tv/drama-list/"
PAGE_URL_TEMPLATE = "https://drama-online.tv/drama-list/page/{page}/"
AJAX_URL = "https://drama-online.tv/wp-admin/admin-ajax.php"

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ar,en;q=0.9",
}

console = Console(highlight=False, safe_box=True)

# Initialize logging system
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)
logger = logging.getLogger("drama_pipeline")


# ==============================================================================
# Robust HTTP Request with Anti-Ban Adaptive Circuit Breaker
# ==============================================================================
_CONSECUTIVE_RATE_LIMITS = 0

def smart_get(url: str, headers: dict = None, timeout: int = REQUEST_TIMEOUT, max_retries: int = MAX_RETRIES):
    """GET request with Anti-Ban Adaptive Circuit Breaker, automatic 429 backoff, and jitter."""
    global _CONSECUTIVE_RATE_LIMITS
    req_headers = headers or DEFAULT_HEADERS
    for attempt in range(1, max_retries + 1):
        try:
            if HAS_CURL_CFFI:
                resp = requests.get(url, headers=req_headers, impersonate="chrome", timeout=timeout)
            else:
                resp = requests.get(url, headers=req_headers, timeout=timeout)

            resp_text = resp.text if hasattr(resp, "text") else ""
            is_429 = (resp.status_code == 429)
            if not is_429 and ("rate limit exceeded" in resp_text.lower() or "too many requests" in resp_text.lower()):
                is_429 = True

            if is_429:
                _CONSECUTIVE_RATE_LIMITS += 1
                wait_seconds = 35  # Safe base wait
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = int(retry_after) + 3
                else:
                    m = re.search(r"wait\s+(\d+)\s+seconds", resp_text, re.I)
                    if m:
                        wait_seconds = int(m.group(1)) + 3
                    else:
                        wait_seconds = max(35, 20 * attempt)

                # Adaptive Escalation: Add extra padding for consecutive strikes to ensure 0 bans
                if _CONSECUTIVE_RATE_LIMITS > 1:
                    escalation = min((_CONSECUTIVE_RATE_LIMITS - 1) * 20, 80)
                    wait_seconds += escalation

                logger.warning(f"[Anti-Ban] Rate limited on {url}. Strike {_CONSECUTIVE_RATE_LIMITS}. Pausing {wait_seconds}s (Attempt {attempt}/{max_retries})")
                console.print(f"[bold yellow]🛡️ [Anti-Ban Shield] Rate limit detected. Pausing {wait_seconds}s to protect your IP from firewall penalties (Strike {_CONSECUTIVE_RATE_LIMITS})...[/bold yellow]")
                time.sleep(wait_seconds)
                time.sleep(random.uniform(1.0, 3.0))  # Human jitter
                continue

            if resp.status_code == 200:
                if _CONSECUTIVE_RATE_LIMITS > 0:
                    _CONSECUTIVE_RATE_LIMITS = max(0, _CONSECUTIVE_RATE_LIMITS - 1)
                return resp
            elif resp.status_code == 404:
                return resp
            else:
                time.sleep(attempt * 2.0 + random.uniform(0.5, 1.5))
        except Exception:
            time.sleep(attempt * 2.0 + random.uniform(0.5, 1.5))

    return None


# ==============================================================================
# General Utilities
# ==============================================================================
def sanitize_filename(name: str) -> str:
    """Sanitize directory and file names across all operating systems."""
    clean = re.sub(r'[\\/*?:"<>|]', "", name).strip().rstrip(".")
    clean = re.sub(r"\s+", " ", clean)
    return clean or "Unnamed_Series"


def clean_image_url(url: str) -> str:
    """Extract clean, high-resolution original image URL from relative or doubled paths."""
    if not url:
        return ""
    url = url.strip()

    if "image.tmdb.org" in url:
        m = re.search(r"https://image\.tmdb\.org/t/p/(?:w\d+|original)/([a-zA-Z0-9_-]+\.(?:jpg|png|webp))", url)
        if m:
            return f"https://image.tmdb.org/t/p/w500/{m.group(1)}"

    if "_files/" in url or url.startswith("."):
        filename = url.split("/")[-1]
        filename = re.sub(r"\(\d+\)", "", filename)
        name_only = filename.split(".")[0]
        if len(name_only) >= 15 and re.match(r"^[a-zA-Z0-9_-]+$", name_only):
            return f"https://image.tmdb.org/t/p/w500/{filename}"
        return urljoin(BASE_URL, f"/wp-content/uploads/{filename}")

    if url.startswith("/"):
        return urljoin(BASE_URL, url)

    return url


def clean_embed_url(url: str) -> str:
    """Normalize video embed URLs for direct playback (Google Drive, Mega, etc.)."""
    if not url:
        return ""
    url = url.strip()

    # Strip stray hashes
    url = re.sub(r"^https?://#+", "https://", url)

    # Convert Mega file links to direct embed format
    if "mega.nz" in url or "mega.io" in url:
        if "/file/" in url:
            url = url.replace("/file/", "/embed/")
        elif "/embed/" not in url:
            url = url.replace("mega.nz/", "mega.nz/embed/").replace("mega.io/", "mega.io/embed/")

    # Convert Google Drive view links to direct preview format
    if "drive.google.com" in url and "/preview" not in url:
        url = re.sub(r"/view.*$", "/preview", url)
        url = re.sub(r"/edit.*$", "/preview", url)

    if url.startswith("http://"):
        url = url.replace("http://", "https://", 1)

    return url


def download_banner(image_url: str, output_folder: Path) -> str:
    """Download cover poster art locally as banner.jpg inside series directory."""
    if not image_url:
        return ""

    try:
        ext = ".jpg"
        clean_u = image_url.split("?")[0].lower()
        for e in [".png", ".webp", ".jpeg", ".jpg"]:
            if clean_u.endswith(e):
                ext = e
                break

        banner_filename = f"banner{ext}"
        dest_path = output_folder / banner_filename

        # If already cached and valid size (> 1KB)
        if dest_path.exists() and dest_path.stat().st_size > 1000:
            return banner_filename

        if os.path.exists(image_url) and os.path.isfile(image_url):
            import shutil
            shutil.copy2(image_url, dest_path)
            return banner_filename

        resp = smart_get(image_url, timeout=20)
        if resp and resp.status_code == 200 and resp.content:
            with open(dest_path, "wb") as f:
                f.write(resp.content)
            logger.info(f"Downloaded banner for {output_folder.name}: {banner_filename}")
            return banner_filename
    except Exception as e:
        logger.warning(f"Failed to download banner for {output_folder.name}: {e}")

    return ""


# ==============================================================================
# Stage 1: Sitemap Catalog Extraction
# ==============================================================================
def parse_sitemap_cards(html_text: str) -> list[dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    items = []
    for card in soup.find_all("a", class_="movie-card"):
        href = card.get("href")
        if not href:
            continue

        title = card.get("title")
        if not title:
            t_el = card.find(class_=re.compile(r"movie-title|title", re.I))
            title = t_el.get_text(strip=True) if t_el else card.get_text(strip=True)

        items.append({
            "title": (title or "").strip(),
            "url": href.strip()
        })
    return items


def fetch_sitemap_page(page: int) -> tuple[int, list[dict]]:
    url = PAGE_URL_TEMPLATE.format(page=page) if page > 1 else FIRST_PAGE_URL
    resp = smart_get(url, timeout=REQUEST_TIMEOUT)
    if resp and resp.status_code == 200:
        return page, parse_sitemap_cards(resp.text)
    return page, []


def step_1_sitemap(force: bool = False, workers: int = DEFAULT_SITEMAP_WORKERS) -> list[dict]:
    console.print(Panel(
        "[bold cyan]STAGE 1: Catalog Discovery & Sitemap Verification[/bold cyan]",
        border_style="cyan",
        box=BOX_STYLE
    ))
    logger.info("--- Starting Step 1: Sitemap ---")

    if SITEMAP_FILE.exists() and not force:
        try:
            with open(SITEMAP_FILE, "r", encoding="utf-8-sig") as f:
                all_series = json.load(f)
            console.print(f"[green][OK] Verified local sitemap.json with [bold]{len(all_series)}[/bold] series indexed.[/green]\n")
            logger.info(f"Loaded {len(all_series)} series from existing sitemap.json")
            return all_series
        except Exception as e:
            logger.warning(f"Failed to read local sitemap.json: {e}")

    total_pages = 125
    try:
        r1 = smart_get(FIRST_PAGE_URL, timeout=REQUEST_TIMEOUT)
        if r1 and r1.status_code == 200:
            m = re.search(r"Page\s+\d+\s+of\s+(\d+)", r1.text, re.I)
            if m:
                total_pages = int(m.group(1))
    except Exception as e:
        logger.warning(f"Failed to auto-detect total pages: {e}")

    console.print(f"[*] Crawling [bold]{total_pages}[/bold] catalog pages from site with {workers} worker threads...")

    pages_data = {}
    pending = list(range(1, total_pages + 1))
    round_n = 1

    while pending and round_n <= 3:
        current_workers = workers if round_n == 1 else 4
        desc = "Crawling catalog pages" if round_n == 1 else f"Retrying missing pages (Round {round_n})"

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(bar_width=35),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task(desc, total=len(pending))
            with ThreadPoolExecutor(max_workers=current_workers) as executor:
                futures = {executor.submit(fetch_sitemap_page, p): p for p in pending}
                for fut in as_completed(futures):
                    p_num, items = fut.result()
                    if items:
                        pages_data[p_num] = items
                    progress.advance(task)

        pending = [p for p in range(1, total_pages + 1) if not pages_data.get(p)]
        round_n += 1

    all_series = []
    seen = set()
    for p in range(1, total_pages + 1):
        for item in pages_data.get(p, []):
            if item["url"] not in seen:
                seen.add(item["url"])
                all_series.append(item)

    with open(SITEMAP_FILE, "w", encoding="utf-8") as f:
        json.dump(all_series, f, ensure_ascii=False, indent=2)

    console.print(f"[bold green][OK] Stage 1 Complete: Successfully cached {len(all_series)} series into sitemap.json[/bold green]\n")
    logger.info(f"Step 1 Complete: {len(all_series)} series saved to sitemap.json")
    return all_series


# ==============================================================================
# Stage 2: Metadata, Banners & Episodes Parsers
# ==============================================================================
def parse_episodes_from_html(html_snippet: str) -> list[dict]:
    soup = BeautifulSoup(html_snippet, "html.parser")
    episodes = []

    for a in soup.find_all("a", class_="drama-ep-link"):
        href = a.get("href")
        if not href:
            continue

        h_tag = a.find(["h4", "h3", "h2"])
        title = h_tag.get_text(strip=True) if h_tag else ""

        badge = a.find(class_=re.compile(r"duration-badge|ep-num", re.I))
        num_str = badge.get_text(strip=True) if badge else ""
        if not num_str and title:
            m = re.search(r"\d+", title)
            num_str = m.group(0) if m else ""
        if not num_str and href:
            m = re.search(r"/(\d+)(?:-\d+)?/?$", href)
            num_str = m.group(1) if m else ""

        date_el = a.find(class_=re.compile(r"date", re.I))
        date_str = date_el.get_text(strip=True) if date_el else ""

        img = a.find("img")
        thumb_url = None
        if img:
            raw_thumb = img.get("data-src") or img.get("data-lazy-src") or img.get("src")
            thumb_url = clean_image_url(raw_thumb)

        ep_num = int(num_str) if (isinstance(num_str, str) and num_str.isdigit()) else num_str

        episodes.append({
            "episode_number": ep_num,
            "title": title or (f"Episode {ep_num}" if ep_num else "Episode"),
            "url": href.strip(),
            "date": date_str,
            "thumbnail": thumb_url
        })

    return episodes


def parse_series_metadata(soup: BeautifulSoup, default_url: str = "") -> dict:
    canonical_el = soup.find("link", rel=re.compile(r"canonical", re.I))
    og_url_el = soup.find("meta", property="og:url")

    series_url = ""
    if canonical_el and canonical_el.get("href"):
        series_url = canonical_el["href"].strip()
    elif og_url_el and og_url_el.get("content"):
        series_url = og_url_el["content"].strip()
    elif default_url and not default_url.rstrip("/").endswith("drama-online.tv"):
        series_url = default_url.strip()
    else:
        series_url = default_url or ""

    h1 = soup.find("h1")
    raw_title = h1.get_text(strip=True) if h1 else ""

    title_en = raw_title
    title_ar = ""
    if "/" in raw_title:
        parts = [p.strip() for p in raw_title.split("/", 1)]
        title_en = parts[0]
        title_ar = parts[1]

    other_names_el = soup.find(class_="drama-other-names")
    other_names = other_names_el.get_text(strip=True) if other_names_el else ""

    story_el = soup.find(class_=re.compile(r"drama-story|story-text|synopsis", re.I)) or soup.find("div", class_="story")
    story = story_el.get_text(strip=True) if story_el else ""
    if not story:
        hero = soup.find(class_="drama-hero")
        if hero:
            for p in hero.find_all("p"):
                txt = p.get_text(strip=True)
                if len(txt) > 30:
                    story = txt
                    break

    poster_img = soup.select_one(".drama-poster img, .movie-poster img")
    poster_url = None
    if poster_img:
        raw_poster = poster_img.get("data-src") or poster_img.get("data-lazy-src") or poster_img.get("src")
        poster_url = clean_image_url(raw_poster)

    overlay = soup.find(class_=re.compile(r"drama-bg-overlay|bg-overlay", re.I))
    banner_url = None
    if overlay and overlay.get("style"):
        m = re.search(r"url\(['\"]?([^'\")]+)['\"]?\)", overlay["style"])
        if m:
            banner_url = clean_image_url(m.group(1))

    banner_to_use = poster_url or banner_url

    metadata = {}
    hero_section = soup.find(class_="drama-hero") or soup
    for item in hero_section.find_all(class_=re.compile(r"meta-item|info-item|item", re.I)):
        text = item.get_text(separator=":", strip=True)
        if ":" in text:
            key, val = [x.strip() for x in text.split(":", 1)]
            val = re.sub(r"^:\s*", "", val)
            if key and val:
                metadata[key] = val

    drama_id = None
    season = "1"
    btn = soup.find(id="load-more-episodes")
    if btn:
        drama_id = btn.get("data-drama-id")
        season = btn.get("data-season", "1")

    if not drama_id:
        m = re.search(r"drama_id:\s*['\"]?(\d+)['\"]?", soup.text)
        if m:
            drama_id = m.group(1)

    return {
        "title": raw_title,
        "title_en": title_en,
        "title_ar": title_ar,
        "other_names": other_names,
        "url": series_url,
        "drama_id": drama_id,
        "season": season,
        "story": story,
        "country": metadata.get("الدولة") or metadata.get("Country"),
        "year": metadata.get("السنة") or metadata.get("Year"),
        "status": metadata.get("الحالة") or metadata.get("Status"),
        "rating": metadata.get("التقييم") or metadata.get("Rating"),
        "declared_episodes": metadata.get("الحلقات") or metadata.get("Episodes"),
        "poster": poster_url,
        "banner": banner_to_use,
        "metadata_extra": metadata,
    }


def fetch_more_episodes_ajax(drama_id: str, season: str = "1", start_page: int = 2, referer_url: str = BASE_URL, max_retries: int = MAX_RETRIES) -> list[dict]:
    all_extra_episodes = []
    current_page = start_page
    headers = {
        **DEFAULT_HEADERS,
        "Referer": referer_url,
        "X-Requested-With": "XMLHttpRequest",
    }

    while True:
        payload = {
            "action": "drama_load_more_episodes",
            "drama_id": str(drama_id),
            "season": str(season),
            "page": current_page
        }

        page_success = False
        remaining = False

        for attempt in range(1, max_retries + 1):
            try:
                if HAS_CURL_CFFI:
                    resp = requests.post(AJAX_URL, data=payload, headers=headers, impersonate="chrome", timeout=REQUEST_TIMEOUT)
                else:
                    resp = requests.post(AJAX_URL, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)

                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success"):
                        res_data = data.get("data", {})
                        html_chunk = res_data.get("html", "")
                        remaining = res_data.get("remaining", False)
                        if html_chunk:
                            new_eps = parse_episodes_from_html(html_chunk)
                            all_extra_episodes.extend(new_eps)
                        page_success = True
                        break
                elif resp.status_code == 429:
                    time.sleep(30)
                    continue
                time.sleep(attempt * 1.5)
            except Exception:
                time.sleep(attempt * 1.5)

        if not page_success or not remaining:
            break

        current_page += 1
        time.sleep(0.5)

    return all_extra_episodes


# ==============================================================================
# Stage 3: Video Embed Extraction & Decryption
# ==============================================================================
def parse_episode_servers_page(html_content: str):
    soup = BeautifulSoup(html_content, "html.parser")
    h1 = soup.find("h1")
    page_title = h1.get_text(strip=True) if h1 else ""

    ep_match = re.search(r"episodeId:\s*(\d+)", html_content)
    nonce_match = re.search(r"watchNonce:\s*['\"]([^'\"]+)['\"]", html_content)
    api_match = re.search(r"apiUrl:\s*['\"]([^'\"]+)['\"]", html_content)

    episode_id = ep_match.group(1) if ep_match else None
    nonce = nonce_match.group(1) if nonce_match else ""
    api_url = api_match.group(1) if api_match else "https://drama-online.tv/wp-json/drama/v1/watch-url"

    servers = []
    for btn in soup.find_all("button", class_=re.compile(r"server-btn")):
        srv_idx = btn.get("data-server-index")
        if srv_idx is None:
            continue

        title = btn.get("title") or btn.get_text(strip=True)
        if not title:
            classes = btn.get("class", [])
            filtered = [c for c in classes if c not in ("server-btn", "current", "loading", "error")]
            title = filtered[0].capitalize() if filtered else f"Server {srv_idx}"

        servers.append({"name": title.strip(), "index": int(srv_idx)})

    return page_title, episode_id, nonce, api_url, servers


def fetch_single_server_embed(api_url: str, episode_id: str, nonce: str, server_info: dict) -> dict:
    name = server_info["name"]
    srv_idx = server_info["index"]
    headers = {
        **DEFAULT_HEADERS,
        "Referer": BASE_URL + "/",
        "X-Drama-Watch-Nonce": nonce
    }
    result = {"name": name, "embed_url": None, "status": "error"}

    try:
        api_endpoint = f"{api_url}?ep={episode_id}&srv={srv_idx}"
        resp = smart_get(api_endpoint, headers=headers, timeout=12, max_retries=3)
        if not resp or resp.status_code != 200:
            return result

        data = resp.json()
        if not data.get("success") or not data.get("url"):
            return result

        watch_url = data["url"]
        resp_watch = smart_get(watch_url, headers=headers, timeout=12, max_retries=2)
        if resp_watch and resp_watch.status_code == 200:
            watch_html = resp_watch.text
            soup_watch = BeautifulSoup(watch_html, "html.parser")
            iframe = soup_watch.find("iframe")
            video_src = soup_watch.find("source")

            raw_embed = ""
            if iframe and iframe.get("src"):
                raw_embed = iframe["src"]
            elif video_src and video_src.get("src"):
                raw_embed = video_src["src"]
            else:
                m = re.search(r'<iframe[^>]+src=["\']([^"\']+)["\']', watch_html)
                if m:
                    raw_embed = m.group(1)

            final_embed = clean_embed_url(raw_embed) if raw_embed else watch_url
            result["embed_url"] = final_embed
            result["status"] = "success"
        else:
            result["embed_url"] = watch_url
            result["status"] = "success"

    except Exception:
        pass

    return result


def extract_servers_for_episode(ep_url: str, workers: int = DEFAULT_SERVER_WORKERS) -> tuple[str, dict]:
    resp = smart_get(ep_url, timeout=REQUEST_TIMEOUT)
    if not resp or resp.status_code != 200:
        return "", {}

    page_title, episode_id, nonce, api_url, server_buttons = parse_episode_servers_page(resp.text)
    if not episode_id or not server_buttons:
        return page_title, {}

    results = []
    if workers <= 1:
        for srv in server_buttons:
            results.append(fetch_single_server_embed(api_url, episode_id, nonce, srv))
            time.sleep(INTER_SERVER_DELAY + random.uniform(0.1, 0.4))
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(fetch_single_server_embed, api_url, episode_id, nonce, srv): srv
                for srv in server_buttons
            }
            for fut in as_completed(futures):
                results.append(fut.result())

    index_map = {srv["index"]: i for i, srv in enumerate(server_buttons)}
    results.sort(key=lambda x: index_map.get(x.get("index", 999), 999))

    servers_dict = {}
    for s in results:
        if s["status"] == "success" and s.get("embed_url"):
            name = s["name"]
            url = s["embed_url"]
            if name in servers_dict:
                count = 2
                while f"{name} {count}" in servers_dict:
                    count += 1
                name = f"{name} {count}"
            servers_dict[name] = url

    return page_title, servers_dict


# ==============================================================================
# Unified End-to-End Per-Series Processor
# ==============================================================================
def is_series_fully_completed(series_url: str) -> tuple[bool, Path, dict]:
    """Check if a series is already 100% processed (info.json + banner + servers)."""
    if not SERIES_DOWNLOAD_DIR.exists():
        return False, None, None

    for folder in SERIES_DOWNLOAD_DIR.iterdir():
        if folder.is_dir():
            info_f = folder / "info.json"
            banner_f = folder / "banner.jpg"
            if info_f.exists() and banner_f.exists():
                try:
                    with open(info_f, "r", encoding="utf-8-sig") as f:
                        data = json.load(f)
                    if data.get("url") == series_url:
                        episodes = data.get("episodes", [])
                        if episodes and all(ep.get("servers") and len(ep["servers"]) > 0 for ep in episodes):
                            return True, folder, data
                        return False, folder, data
                except Exception:
                    pass
    return False, None, None


def process_single_series_end_to_end(
    series_item: dict,
    index: int,
    total: int,
    progress: Progress,
    task_overall: int,
    task_current: int,
    skip_servers: bool = False,
    server_workers: int = DEFAULT_SERVER_WORKERS,
) -> tuple[bool, int, int]:
    """Process an entire series from metadata to poster to full embed servers."""
    series_title = series_item.get("title", "Unknown Series")
    series_url = series_item.get("url", "")

    # 1. Check if completely finished already
    is_done, existing_folder, cached_data = is_series_fully_completed(series_url)
    if is_done and not skip_servers:
        eps_count = len(cached_data.get("episodes", []))
        srv_count = sum(len(ep.get("servers", {})) for ep in cached_data.get("episodes", []))
        progress.console.print(
            f"[dim][OK] [{index}/{total}] Already Cached: [bold cyan]{series_title[:38]}[/bold cyan] ({eps_count} eps | {srv_count} servers)[/dim]"
        )
        progress.advance(task_overall)
        return False, eps_count, srv_count

    # 2. Extract or load metadata & episodes
    series_folder = existing_folder
    series_data = cached_data

    if not series_data:
        progress.update(task_current, description=f"[bold cyan]Fetching Metadata: {series_title[:28]}...", total=10, completed=1)

        resp = smart_get(series_url, timeout=REQUEST_TIMEOUT, max_retries=MAX_RETRIES)
        if not resp or resp.status_code != 200:
            progress.console.print(f"[bold red][FAIL] [{index}/{total}] Failed to load page: {series_title}[/bold red]")
            logger.error(f"Failed to fetch series HTML: {series_url}")
            progress.advance(task_overall)
            return False, 0, 0

        html_content = resp.text
        soup = BeautifulSoup(html_content, "html.parser")
        meta = parse_series_metadata(soup, default_url=series_url)
        if meta.get("url"):
            series_url = meta["url"]

        episodes = parse_episodes_from_html(html_content)

        # AJAX load more pagination
        btn = soup.find(id="load-more-episodes")
        drama_id = meta.get("drama_id")
        season = meta.get("season", "1")

        if btn and drama_id:
            start_page = int(btn.get("data-page", 2))
            progress.update(task_current, description=f"[bold magenta]AJAX Pagination: {series_title[:25]}...")
            extra_episodes = fetch_more_episodes_ajax(
                drama_id=drama_id,
                season=season,
                start_page=start_page,
                referer_url=series_url
            )
            episodes.extend(extra_episodes)

        seen_urls = set()
        unique_episodes = []
        for ep in episodes:
            if ep["url"] not in seen_urls:
                seen_urls.add(ep["url"])
                unique_episodes.append(ep)

        def sort_key(item):
            num = item.get("episode_number")
            if isinstance(num, int):
                return (0, num)
            if isinstance(num, str) and num.isdigit():
                return (0, int(num))
            return (1, 0)

        unique_episodes.sort(key=sort_key)

        folder_name = sanitize_filename(meta["title_en"] or meta["title"] or series_title)
        series_folder = SERIES_DOWNLOAD_DIR / folder_name
        series_folder.mkdir(parents=True, exist_ok=True)

        # Download banner
        banner_target_url = meta.get("poster") or meta.get("banner")
        banner_file_name = download_banner(banner_target_url, series_folder)

        series_data = {
            "title": meta["title"] or series_title,
            "title_en": meta["title_en"],
            "title_ar": meta["title_ar"],
            "other_names": meta["other_names"],
            "url": meta["url"] or series_url,
            "drama_id": meta["drama_id"],
            "country": meta["country"],
            "year": meta["year"],
            "status": meta["status"],
            "rating": meta["rating"],
            "declared_episodes": meta["declared_episodes"],
            "total_episodes_fetched": len(unique_episodes),
            "story": meta["story"],
            "poster": meta["poster"],
            "banner": meta.get("banner") or meta.get("poster"),
            "banner_file": banner_file_name or "banner.jpg",
            "episodes": unique_episodes
        }

        # Commit initial metadata
        with open(series_folder / "info.json", "w", encoding="utf-8") as f:
            json.dump(series_data, f, ensure_ascii=False, indent=2)

    episodes = series_data.get("episodes", [])
    total_eps = len(episodes)
    resolved_servers_count = sum(len(ep.get("servers", {})) for ep in episodes)

    if skip_servers:
        progress.console.print(
            f"[green][OK] [{index}/{total}] Metadata Ready: [bold]{series_title[:35]}[/bold] ({total_eps} episodes | Banner OK)[/green]"
        )
        progress.advance(task_overall)
        return True, total_eps, resolved_servers_count

    # 3. Resolve Video Embed Servers
    progress.update(task_current, description=f"[bold green]{series_title[:28]}[/bold green]", total=total_eps, completed=0)

    episodes_updated = 0
    new_servers_added = 0

    for ep_idx, ep in enumerate(episodes, 1):
        existing_srv = ep.get("servers")
        if existing_srv and len(existing_srv) > 0:
            progress.advance(task_current)
            continue

        ep_url = ep.get("url")
        if not ep_url:
            progress.advance(task_current)
            continue

        progress.update(
            task_current,
            description=f"[bold green]{series_title[:24]}[/bold green] (Ep {ep_idx}/{total_eps})"
        )

        title, servers = extract_servers_for_episode(ep_url, workers=server_workers)
        if title:
            ep["title"] = title
        ep["servers"] = servers
        episodes_updated += 1
        new_servers_added += len(servers)

        # Atomic per-episode save
        with open(series_folder / "info.json", "w", encoding="utf-8") as f:
            json.dump(series_data, f, ensure_ascii=False, indent=2)

        progress.advance(task_current)
        time.sleep(INTER_EPISODE_DELAY + random.uniform(0.2, 0.5))

    total_servers = sum(len(ep.get("servers", {})) for ep in episodes)

    progress.console.print(
        f"[bold green][OK] [{index}/{total}] Completed: [bold white]{series_title[:32]}[/bold white] | [cyan]{total_eps} Episodes[/cyan] | [yellow]{total_servers} Embed Servers[/yellow] | [magenta]banner.jpg[/magenta][/bold green]"
    )
    logger.info(f"Completed {series_title}: {total_eps} eps, {total_servers} servers")

    progress.advance(task_overall)
    return True, total_eps, total_servers


# ==============================================================================
# Live Terminal Orchestrator & Monitoring Dashboard
# ==============================================================================
def run_live_pipeline(series_list: list[dict], skip_servers: bool = False, server_workers: int = DEFAULT_SERVER_WORKERS):
    SERIES_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    total_series = len(series_list)
    start_time = time.time()

    # Pre-scan existing downloads for dashboard metrics
    initial_completed_series = 0
    initial_total_episodes = 0
    initial_total_servers = 0

    for f in SERIES_DOWNLOAD_DIR.iterdir():
        if f.is_dir():
            info_f = f / "info.json"
            if info_f.exists():
                try:
                    with open(info_f, "r", encoding="utf-8-sig") as jf:
                        d = json.load(jf)
                    eps = d.get("episodes", [])
                    if eps:
                        initial_completed_series += 1
                        initial_total_episodes += len(eps)
                        for ep in eps:
                            initial_total_servers += len(ep.get("servers", {}))
                except Exception:
                    pass

    # Dashboard Overview Header
    dash_table = Table(box=BOX_STYLE, border_style="cyan", show_header=False)
    dash_table.add_column("Key", style="bold cyan", width=26)
    dash_table.add_column("Val", style="bold yellow", width=42)

    dash_table.add_row("Total Catalog Target", f"{total_series:,} Series")
    dash_table.add_row("Existing Cached Series", f"{initial_completed_series:,} Series")
    dash_table.add_row("Existing Total Episodes", f"{initial_total_episodes:,} Episodes")
    dash_table.add_row("Existing Embed Servers", f"{initial_total_servers:,} Video Links")
    dash_table.add_row("Concurrency Profile", f"Server Workers: {server_workers} | Polite Delay: {INTER_EPISODE_DELAY}s")
    dash_table.add_row("Workspace Directory", str(SERIES_DOWNLOAD_DIR.name))

    console.print(Panel(
        dash_table,
        title="[bold green]Pipeline Dashboard - Initial Status[/bold green]",
        border_style="green",
        box=BOX_STYLE
    ))
    console.print()

    # Multi-Progress Live Display
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=38),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        task_overall = progress.add_task(
            "[bold cyan]Overall Catalog Progress[/bold cyan]",
            total=total_series
        )
        task_current = progress.add_task(
            "[bold magenta]Active Drama Episodes[/bold magenta]",
            total=10,
            completed=0
        )

        try:
            for idx, item in enumerate(series_list, 1):
                was_new, eps_c, srv_c = process_single_series_end_to_end(
                    series_item=item,
                    index=idx,
                    total=total_series,
                    progress=progress,
                    task_overall=task_overall,
                    task_current=task_current,
                    skip_servers=skip_servers,
                    server_workers=server_workers
                )
                if was_new:
                    time.sleep(INTER_SERIES_DELAY + random.uniform(0.3, 0.8))
        except KeyboardInterrupt:
            progress.console.print("\n[bold yellow]>> Execution paused by user (Ctrl+C).[/bold yellow]")
            progress.console.print("[green]All fetched series, episodes, and servers are safely preserved on disk.[/green]")
            logger.info("Pipeline stopped by user (KeyboardInterrupt)")

    # Final Summary Table
    display_summary(start_time)


# ==============================================================================
# Final Reporting & Execution Summary
# ==============================================================================
def display_summary(start_time: float):
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)
    hours, minutes = divmod(minutes, 60)

    time_str = f"{hours}h {minutes}m {seconds}s" if hours > 0 else f"{minutes} min {seconds} sec"

    folders = [f for f in SERIES_DOWNLOAD_DIR.iterdir() if f.is_dir() and (f / "info.json").exists()]
    total_episodes = 0
    total_servers = 0

    for f in folders:
        try:
            with open(f / "info.json", "r", encoding="utf-8-sig") as jf:
                d = json.load(jf)
            eps = d.get("episodes", [])
            total_episodes += len(eps)
            for ep in eps:
                total_servers += len(ep.get("servers", {}))
        except Exception:
            pass

    table = Table(title="[bold green]Pipeline Execution Final Metrics[/bold green]", border_style="bright_blue", box=BOX_STYLE)
    table.add_column("Metric", style="bold cyan", width=34)
    table.add_column("Value", style="bold yellow", width=34)

    table.add_row("Total Processed Series", f"{len(folders):,}")
    table.add_row("Total Harvested Episodes", f"{total_episodes:,}")
    table.add_row("Total Resolved Embed Servers", f"{total_servers:,}")
    table.add_row("Storage Workspace", str(SERIES_DOWNLOAD_DIR))
    table.add_row("Audit Log File", str(LOG_FILE))
    table.add_row("Configuration Profile", str(CONFIG_FILE.name))
    table.add_row("Total Elapsed Runtime", time_str)

    console.print()
    console.print(table)
    console.print(Panel(
        f"[bold green]All completed data is organized, verified, and saved at:[/bold green]\n[cyan]{SERIES_DOWNLOAD_DIR}[/cyan]",
        border_style="green",
        box=BOX_STYLE
    ))


# ==============================================================================
# Main Entry Point
# ==============================================================================
def main():
    default_mode = OPTIONS.get("default_mode", "all")
    test_default_count = int(OPTIONS.get("test_series_count", 1))
    skip_servers_default = bool(OPTIONS.get("skip_servers", False))
    force_sitemap_default = bool(OPTIONS.get("force_sitemap_refresh", False))

    parser = argparse.ArgumentParser(description="Drama Online Master Automation Pipeline")
    parser.add_argument("--all", "-a", action="store_true", help="Explicitly run full pipeline on all series in catalog")
    parser.add_argument("--test", "-t", type=int, nargs="?", const=test_default_count, help="Run test mode on N series only (e.g. --test 3)")
    parser.add_argument("--force-sitemap", action="store_true", default=force_sitemap_default, help="Force fresh web scraping of sitemap.json")
    parser.add_argument("--skip-servers", action="store_true", default=skip_servers_default, help="Extract metadata, posters, and episodes while skipping servers")
    parser.add_argument("--server-workers", type=int, default=DEFAULT_SERVER_WORKERS, help=f"Concurrent server resolution workers (default: {DEFAULT_SERVER_WORKERS})")
    args = parser.parse_args()

    console.print(Panel.fit(
        "[bold green]Drama Online Master Automation Pipeline[/bold green]\n"
        "[cyan]End-to-End Media Ingestion: Catalog -> Metadata -> Banner -> Episodes -> Decrypted Embeds[/cyan]\n"
        f"[dim]Loaded configuration profile: {CONFIG_FILE.name}[/dim]",
        border_style="bold green",
        box=BOX_STYLE
    ))

    # Stage 1: Load or Crawl Sitemap
    all_series = step_1_sitemap(force=args.force_sitemap, workers=DEFAULT_SITEMAP_WORKERS)

    # Determine execution scope
    if args.test:
        target_series = all_series[:args.test]
        console.print(f"[bold yellow]>> Test mode active: Processing first {len(target_series)} series only.[/bold yellow]\n")
    elif default_mode == "test" and not args.all:
        target_series = all_series[:test_default_count]
        console.print(f"[bold yellow]>> Config test mode: Processing first {len(target_series)} series.[/bold yellow]\n")
    else:
        target_series = all_series
        console.print(f"[bold cyan]>> Production Mode Active: Processing all {len(target_series):,} series from catalog.[/bold cyan]\n")

    # Run unified live pipeline
    run_live_pipeline(
        series_list=target_series,
        skip_servers=args.skip_servers,
        server_workers=args.server_workers
    )


if __name__ == "__main__":
    main()
