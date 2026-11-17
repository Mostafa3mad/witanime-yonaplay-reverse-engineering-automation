"""
================================================================================
  Drama Online Master Pipeline - نظام التحميل الشامل لموقع دراما أونلاين
  المسار الكامل: Sitemap -> تفاصيل المسلسل -> البانر -> الحلقات -> السيرفرات
================================================================================
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

try:
    from curl_cffi import requests
except ImportError:
    import requests

from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
)

# ==============================================================================
# المسارات والإعدادات الأساسية
# ==============================================================================
CURRENT_DIR = Path(__file__).resolve().parent
SERIES_DOWNLOAD_DIR = CURRENT_DIR / "series_download"
SITEMAP_FILE = CURRENT_DIR / "sitemap.json"
LOGS_DIR = CURRENT_DIR / "logs"
LOG_FILE = LOGS_DIR / "pipeline.log"

BASE_URL = "https://drama-online.tv"
FIRST_PAGE_URL = "https://drama-online.tv/drama-list/"
PAGE_URL_TEMPLATE = "https://drama-online.tv/drama-list/page/{page}/"
AJAX_URL = "https://drama-online.tv/wp-admin/admin-ajax.php"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ar,en;q=0.9",
}

DEFAULT_SERIES_WORKERS = 3
DEFAULT_SERVER_WORKERS = 6
MAX_RETRIES = 4

console = Console()

# تهيئة نظام السجلات
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8"
)
logger = logging.getLogger("drama_pipeline")


# ==============================================================================
# دوال مساعدة عامة (Utilities)
# ==============================================================================
def sanitize_filename(name: str) -> str:
    """تنظيف أسماء المجلدات من الحروف الممنوعة في أنظمة الملفات."""
    clean = re.sub(r'[\\/*?:"<>|]', "", name).strip().rstrip(".")
    clean = re.sub(r"\s+", " ", clean)
    return clean or "Unnamed_Series"


def clean_image_url(url: str) -> str:
    """تنظيف وتوليد الرابط الكامل والمباشر للصورة."""
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
    """تنظيف وتجهيز روابط الفيديو والتضمين لتشغيل مباشر."""
    if not url:
        return ""
    url = url.strip()

    # إزالة أي علامات هاش زائدة بعد البروتوكول
    url = re.sub(r"^https?://#+", "https://", url)

    # تحويل روابط Mega لصيغة embed
    if "mega.nz" in url or "mega.io" in url:
        if "/file/" in url:
            url = url.replace("/file/", "/embed/")
        elif "/embed/" not in url:
            url = url.replace("mega.nz/", "mega.nz/embed/").replace("mega.io/", "mega.io/embed/")

    # تحويل روابط Google Drive لصيغة preview
    if "drive.google.com" in url and "/preview" not in url:
        url = re.sub(r"/view.*$", "/preview", url)
        url = re.sub(r"/edit.*$", "/preview", url)

    if url.startswith("http://"):
        url = url.replace("http://", "https://", 1)

    return url


def download_banner(image_url: str, output_folder: Path) -> str:
    """تحميل وحفظ صورة البوستر كـ banner.jpg داخل مجلد المسلسل."""
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

        # إذا كانت الصورة محملة محلياً مسبقاً
        if dest_path.exists() and dest_path.stat().st_size > 1000:
            return banner_filename

        if os.path.exists(image_url) and os.path.isfile(image_url):
            import shutil
            shutil.copy2(image_url, dest_path)
            return banner_filename

        resp = requests.get(image_url, headers=DEFAULT_HEADERS, timeout=20)
        if resp.status_code == 200 and resp.content:
            with open(dest_path, "wb") as f:
                f.write(resp.content)
            logger.info(f"Downloaded banner for {output_folder.name}: {banner_filename}")
            return banner_filename
    except Exception as e:
        logger.warning(f"Failed to download banner for {output_folder.name}: {e}")

    return ""


# ==============================================================================
# المرحلة 1: خريطة الموقع (Sitemap Extraction)
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
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=DEFAULT_HEADERS, impersonate="chrome", timeout=20)
            if resp.status_code == 200:
                return page, parse_sitemap_cards(resp.text)
            elif resp.status_code == 404:
                return page, []
            time.sleep(attempt * 1.5)
        except Exception:
            time.sleep(attempt * 1.5)
    return page, []


def step_1_sitemap(force: bool = False, workers: int = 10) -> list[dict]:
    console.print(Panel("[bold cyan]المرحلة 1: فحص واستخراج خريطة مسلسلات الموقع (Sitemap)[/bold cyan]", border_style="cyan"))
    logger.info("--- Starting Step 1: Sitemap ---")

    if SITEMAP_FILE.exists() and not force:
        with open(SITEMAP_FILE, "r", encoding="utf-8") as f:
            all_series = json.load(f)
        console.print(f"[green]✓ تم العثور على sitemap.json جاهز يحتوي على [bold]{len(all_series)}[/bold] مسلسل.[/green]\n")
        logger.info(f"Loaded {len(all_series)} series from existing sitemap.json")
        return all_series

    # تحديد عدد الصفحات من الصفحة الأولى
    total_pages = 125
    try:
        r1 = requests.get(FIRST_PAGE_URL, headers=DEFAULT_HEADERS, impersonate="chrome", timeout=20)
        if r1.status_code == 200:
            m = re.search(r"Page\s+\d+\s+of\s+(\d+)", r1.text, re.I)
            if m:
                total_pages = int(m.group(1))
    except Exception as e:
        logger.warning(f"Failed to auto-detect total pages: {e}")

    console.print(f"[*] جاري سحب [bold]{total_pages}[/bold] صفحة من قائمة الدراما...")

    pages_data = {}
    pending = list(range(1, total_pages + 1))
    round_n = 1

    while pending and round_n <= 3:
        current_workers = workers if round_n == 1 else 4
        desc = "سحب صفحات الدراما" if round_n == 1 else f"إعادة محاولة الصفحات المفقودة (جولة {round_n})"

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

    console.print(f"[bold green]✓ اكتملت المرحلة 1: تم حفظ {len(all_series)} مسلسل في sitemap.json[/bold green]\n")
    logger.info(f"Step 1 Complete: {len(all_series)} series saved to sitemap.json")
    return all_series


# ==============================================================================
# المرحلة 2: تفاصيل المسلسل، البانر، والحلقات (Series, Banner & Episodes)
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
            "title": title or (f"الحلقة {ep_num}" if ep_num else "حلقة"),
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
        "metadata_extra": metadata
    }


def fetch_more_episodes_ajax(drama_id: str, season: str = "1", start_page: int = 2, referer_url: str = BASE_URL, max_retries: int = 4) -> list[dict]:
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
                resp = requests.post(AJAX_URL, data=payload, headers=headers, impersonate="chrome", timeout=25)
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
                time.sleep(attempt * 2.0)
            except Exception:
                time.sleep(attempt * 2.0)

        if not page_success or not remaining:
            break

        current_page += 1
        time.sleep(0.5)

    return all_extra_episodes


def process_single_series_episodes(series_url: str, force: bool = False) -> Path:
    """استخراج تفاصيل مسلسل وتحميل البانر وتجهيز info.json مع دعم الاستئناف."""
    # البحث إذا كان المسلسل موجود مسبقاً
    for existing_folder in SERIES_DOWNLOAD_DIR.iterdir():
        if existing_folder.is_dir():
            info_f = existing_folder / "info.json"
            if info_f.exists():
                try:
                    with open(info_f, "r", encoding="utf-8") as jf:
                        d = json.load(jf)
                    if d.get("url") == series_url and not force:
                        if d.get("episodes") and len(d["episodes"]) > 0:
                            return existing_folder
                except Exception:
                    pass

    html_content = ""
    for attempt in range(1, 4):
        try:
            resp = requests.get(series_url, headers=DEFAULT_HEADERS, impersonate="chrome", timeout=25)
            if resp.status_code == 200:
                html_content = resp.text
                break
            time.sleep(attempt * 2.0)
        except Exception:
            time.sleep(attempt * 2.0)

    if not html_content:
        logger.error(f"Failed to fetch series page: {series_url}")
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    meta = parse_series_metadata(soup, default_url=series_url)
    if meta.get("url"):
        series_url = meta["url"]

    episodes = parse_episodes_from_html(html_content)

    btn = soup.find(id="load-more-episodes")
    drama_id = meta.get("drama_id")
    season = meta.get("season", "1")

    if btn and drama_id:
        start_page = int(btn.get("data-page", 2))
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

    folder_name = sanitize_filename(meta["title_en"] or meta["title"] or "Series")
    series_folder = SERIES_DOWNLOAD_DIR / folder_name
    series_folder.mkdir(parents=True, exist_ok=True)

    banner_target_url = meta.get("poster") or meta.get("banner")
    banner_file_name = download_banner(banner_target_url, series_folder)

    series_data = {
        "title": meta["title"],
        "title_en": meta["title_en"],
        "title_ar": meta["title_ar"],
        "other_names": meta["other_names"],
        "url": meta["url"],
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

    info_file_path = series_folder / "info.json"
    with open(info_file_path, "w", encoding="utf-8") as f:
        json.dump(series_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved series info: {meta['title']} ({len(unique_episodes)} episodes) in {series_folder.name}")
    return series_folder


def step_2_series_and_episodes(series_list: list[dict], workers: int = DEFAULT_SERIES_WORKERS) -> list[Path]:
    console.print(Panel("[bold magenta]المرحلة 2: تجهيز مجلدات المسلسلات، تحميل البانر، واستخراج الحلقات[/bold magenta]", border_style="magenta"))
    logger.info("--- Starting Step 2: Series & Episodes ---")

    SERIES_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # فحص المسلسلات المكتملة مسبقاً لدعم الاستئناف
    pending_items = []
    already_done = 0

    for item in series_list:
        clean_name = sanitize_filename(item["title"])
        f_path = SERIES_DOWNLOAD_DIR / clean_name
        info_f = f_path / "info.json"
        banner_f = f_path / "banner.jpg"

        if info_f.exists() and banner_f.exists():
            try:
                with open(info_f, "r", encoding="utf-8") as jf:
                    d = json.load(jf)
                if d.get("episodes") and len(d["episodes"]) > 0:
                    already_done += 1
                    continue
            except Exception:
                pass

        pending_items.append(item)

    console.print(f"إجمالي المسلسلات: [bold]{len(series_list)}[/bold] | مكتمل مسبقاً: [bold green]{already_done}[/bold green] | متبقي للمعالجة: [bold yellow]{len(pending_items)}[/bold yellow]")

    if not pending_items:
        console.print("[green]✓ كافة المسلسلات مكتملة الحلقات والبانرات بالفعل. تخطي المرحلة 2.[/green]\n")
        return [f for f in SERIES_DOWNLOAD_DIR.iterdir() if f.is_dir() and (f / "info.json").exists()]

    completed_folders = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold magenta]{task.description}"),
        BarColumn(bar_width=35),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        TextColumn("[cyan]{task.fields[current]}"),
        console=console
    ) as progress:
        task = progress.add_task("تجهيز بيانات المسلسلات...", total=len(pending_items), current="")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_single_series_episodes, it["url"]): it
                for it in pending_items
            }
            for fut in as_completed(futures):
                folder = fut.result()
                if folder:
                    completed_folders.append(folder)
                    progress.update(task, current=f"{folder.name[:25]}")
                progress.advance(task)

    console.print(f"[bold green]✓ اكتملت المرحلة 2: تم تجهيز {len(completed_folders)} مجلد مسلسلات جديد.[/bold green]\n")
    logger.info(f"Step 2 Complete: {len(completed_folders)} new folders processed")

    return [f for f in SERIES_DOWNLOAD_DIR.iterdir() if f.is_dir() and (f / "info.json").exists()]


# ==============================================================================
# المرحلة 3: استخراج سيرفرات المشاهدة (Video Embed Servers Extraction)
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
        resp = requests.get(api_endpoint, headers=headers, timeout=12)
        if resp.status_code != 200:
            return result

        data = resp.json()
        if not data.get("success") or not data.get("url"):
            return result

        watch_url = data["url"]
        resp_watch = requests.get(watch_url, headers=headers, timeout=12)
        if resp_watch.status_code == 200:
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
    try:
        resp = requests.get(ep_url, headers=DEFAULT_HEADERS, impersonate="chrome", timeout=20)
        if resp.status_code != 200:
            return "", {}
        html = resp.text
    except Exception:
        return "", {}

    page_title, episode_id, nonce, api_url, server_buttons = parse_episode_servers_page(html)
    if not episode_id or not server_buttons:
        return page_title, {}

    results = []
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


def process_series_servers(series_folder: Path, server_workers: int = DEFAULT_SERVER_WORKERS) -> tuple[Path, int]:
    info_file = series_folder / "info.json"
    if not info_file.exists():
        return series_folder, 0

    try:
        with open(info_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        episodes = data.get("episodes", [])
        if not episodes:
            return series_folder, 0

        updated_count = 0
        for ep in episodes:
            if ep.get("servers") and len(ep["servers"]) > 0:
                continue

            ep_url = ep.get("url")
            if not ep_url:
                continue

            title, servers = extract_servers_for_episode(ep_url, workers=server_workers)
            if title:
                ep["title"] = title
            ep["servers"] = servers
            updated_count += 1

            # حفظ بعد كل حلقة مباشرة
            with open(info_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            time.sleep(0.8)  # مهلة لتفادي حظر Rate Limit

        return series_folder, updated_count
    except Exception as e:
        logger.error(f"Error processing servers in {series_folder.name}: {e}")
        return series_folder, 0


def step_3_servers(folders: list[Path], workers: int = 2, server_workers: int = DEFAULT_SERVER_WORKERS):
    console.print(Panel("[bold green]المرحلة 3: استخراج سيرفرات المشاهدة لجميع الحلقات[/bold green]", border_style="green"))
    logger.info("--- Starting Step 3: Embed Servers ---")

    pending_folders = []
    for f in folders:
        info_f = f / "info.json"
        if not info_f.exists():
            continue
        try:
            with open(info_f, "r", encoding="utf-8") as jf:
                d = json.load(jf)
            eps = d.get("episodes", [])
            if any(not ep.get("servers") or len(ep.get("servers", {})) == 0 for ep in eps):
                pending_folders.append(f)
        except Exception:
            pending_folders.append(f)

    console.print(f"إجمالي المجلدات: [bold]{len(folders)}[/bold] | مكتملة السيرفرات: [bold green]{len(folders) - len(pending_folders)}[/bold green] | متبقية: [bold yellow]{len(pending_folders)}[/bold yellow]")

    if not pending_folders:
        console.print("[green]✓ جميع الحلقات تحتوي على سيرفرات المشاهدة بالفعل. اكتملت المرحلة 3.[/green]\n")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold green]{task.description}"),
        BarColumn(bar_width=35),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        TextColumn("[cyan]{task.fields[current]}"),
        console=console
    ) as progress:
        task = progress.add_task("سحب سيرفرات الحلقات...", total=len(pending_folders), current="")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_series_servers, folder, server_workers): folder
                for folder in pending_folders
            }
            for fut in as_completed(futures):
                folder, updated = fut.result()
                progress.update(task, current=f"{folder.name[:20]} (+{updated} eps)")
                progress.advance(task)

    console.print("[bold green]✓ اكتملت المرحلة 3: تم استخراج وتحديث جميع سيرفرات الحلقات بنجاح.[/bold green]\n")
    logger.info("Step 3 Complete: All servers extracted")


# ==============================================================================
# ملخص وإحصائيات العمل النهائي (Final Summary)
# ==============================================================================
def display_summary(start_time: float):
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)

    folders = [f for f in SERIES_DOWNLOAD_DIR.iterdir() if f.is_dir() and (f / "info.json").exists()]
    total_episodes = 0
    total_servers = 0

    for f in folders:
        try:
            with open(f / "info.json", "r", encoding="utf-8") as jf:
                d = json.load(jf)
            eps = d.get("episodes", [])
            total_episodes += len(eps)
            for ep in eps:
                total_servers += len(ep.get("servers", {}))
        except Exception:
            pass

    table = Table(title="[bold green]📊 ملخص تقرير التحميل الشامل (Pipeline Summary)[/bold green]", border_style="bright_blue")
    table.add_column("المعيار", style="bold cyan", width=30)
    table.add_column("القيمة", style="bold yellow", width=30)

    table.add_row("إجمالي المسلسلات المجهزة", str(len(folders)))
    table.add_row("إجمالي الحلقات المستخرجة", str(total_episodes))
    table.add_row("إجمالي روابط السيرفرات", str(total_servers))
    table.add_row("المجلد الرئيسي للمسلسلات", str(SERIES_DOWNLOAD_DIR.name))
    table.add_row("ملف السجلات (Logs)", str(LOG_FILE.name))
    table.add_row("إجمالي وقت التشغيل", f"{minutes} دقيقة و {seconds} ثانية")

    console.print(table)
    console.print(Panel(f"[bold green]✨ اكتملت جميع المراحل بنجاح! المسارات محفوظة وجاهزة للاستخدام في:[/bold green]\n[cyan]{SERIES_DOWNLOAD_DIR}[/cyan]", border_style="green"))


# ==============================================================================
# نقطة الدخول الرئيسية (CLI Execution)
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Drama Online Master Pipeline")
    parser.add_argument("--all", "-a", action="store_true", help="تشغيل خط الإنتاج بالكامل لكافة مسلسلات sitemap")
    parser.add_argument("--test", "-t", type=int, nargs="?", const=1, help="تشغيل اختبار على عدد محدد من المسلسلات (افتراضياً 1)")
    parser.add_argument("--force-sitemap", action="store_true", help="إعادة سحب sitemap.json من الصفر حتى وإن كان موجوداً")
    parser.add_argument("--skip-servers", action="store_true", help="تخطي مرحلة استخراج السيرفرات والاكتفاء بالبيانات والبوستر والحلقات")
    parser.add_argument("--series-workers", type=int, default=DEFAULT_SERIES_WORKERS, help="عدد خيوط المعالجة المتزامنة للمسلسلات")
    parser.add_argument("--server-workers", type=int, default=DEFAULT_SERVER_WORKERS, help="عدد خيوط سحب سيرفرات الحلقة الواحدة")
    args = parser.parse_args()

    start_time = time.time()

    console.print(Panel.fit(
        "[bold green]🚀 Drama Online Full Pipeline Automation[/bold green]\n"
        "[cyan]نظام الأتمتة الشامل: خريطة الموقع ➔ بيانات المسلسل ➔ البانر ➔ الحلقات ➔ السيرفرات[/cyan]",
        border_style="bold green"
    ))

    # المرحلة 1: خريطة الموقع
    all_series = step_1_sitemap(force=args.force_sitemap)

    # تحديد نطاق العمل (اختبار أو كامل)
    if args.test:
        target_series = all_series[:args.test]
        console.print(f"[bold yellow]⚠️ نمط الاختبار مفعل: سيتم تطبيق العمليات على أول {len(target_series)} مسلسل فقط.[/bold yellow]\n")
    elif args.all:
        target_series = all_series
    else:
        # التشغيل الافتراضي: اختبار أول مسلسل إذا لم يحدد وسيط لتجنب بدء تحميل 2600 مسلسل بالخطأ
        console.print("[bold yellow]ℹ️ لم يتم تحديد --all. سيتم إجراء اختبار على أول مسلسل للتأكد من سلامة المراحل.[/bold yellow]")
        console.print("[yellow]لتشغيل كافة المسلسلات بالكامل: استخدم الأمر مع وسيط: --all[/yellow]\n")
        target_series = all_series[:1]

    # المرحلة 2: المسلسلات والبانرات والحلقات
    ready_folders = step_2_series_and_episodes(target_series, workers=args.series_workers)

    # المرحلة 3: السيرفرات (إذا لم يُطلب تخطيها)
    if not args.skip_servers:
        step_3_servers(ready_folders, workers=2, server_workers=args.server_workers)
    else:
        console.print("[yellow]⚠️ تم تخطي مرحلة استخراج السيرفرات بناءً على طلبك (--skip-servers).[/yellow]\n")

    # الملخص النهائي
    display_summary(start_time)


if __name__ == "__main__":
    main()
