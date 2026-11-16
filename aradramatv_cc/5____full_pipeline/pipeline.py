import argparse
import json
import logging
import re
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from curl_cffi import requests
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

BASE_DIR = Path(__file__).resolve().parent.parent
LINKS_DIR = BASE_DIR / "aradramatv_links"
SITEMAP_FILE = BASE_DIR / "sitemap.json"
LOGS_DIR = BASE_DIR / "logs"
LOG_FILE = LOGS_DIR / "pipeline.log"

TOTAL_SITEMAP_PAGES = 82
DEFAULT_WORKERS = 15
MAX_RETRIES = 3

console = Console()

LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8"
)
logger = logging.getLogger("pipeline")


def sanitize_filename(name: str) -> str:
    clean = re.sub(r'[\\/*?:"<>|]', "", name).strip().rstrip(".")
    return clean or "Unnamed_Series"


def safe_get(url: str, retries: int = MAX_RETRIES, timeout: int = 25):
    for attempt in range(1, retries + 1):
        try:
            res = requests.get(url, impersonate="chrome", timeout=timeout)
            if res.status_code == 200:
                return res
            time.sleep(attempt * 1.5)
        except Exception as e:
            if attempt == retries:
                logger.warning(f"Failed to fetch URL: {url} | Error: {e}")
            time.sleep(attempt * 1.5)
    return None


def fetch_sitemap_page(page: int) -> tuple[int, list[dict]]:
    url = f"https://aradramatv.cc/category/serie/page/{page}/"
    res = safe_get(url)
    if not res:
        return page, []

    soup = BeautifulSoup(res.content, "html.parser")
    items = []
    for article in soup.find_all("article"):
        h3 = article.find("h3")
        title = h3.get_text(strip=True) if h3 else None
        link_el = article.select_one("a.first_A") or (h3.find_parent("a") if h3 else None) or article.find("a", href=True)
        url_href = link_el.get("href") if link_el else None

        if not title and link_el and link_el.get("title"):
            title = link_el["title"].strip()

        if url_href and title:
            items.append({
                "title": title,
                "url": url_href.strip()
            })
    return page, items


def run_step_1_sitemap(force: bool = False, workers: int = DEFAULT_WORKERS):
    console.print(Panel("[bold cyan]STEP 1: SITEMAP & FOLDER INITIALIZATION[/bold cyan]", border_style="cyan"))
    logger.info("--- Starting Step 1: Sitemap ---")

    all_series = []
    if SITEMAP_FILE.exists() and not force:
        console.print(f"[green]✓ Found existing sitemap at: {SITEMAP_FILE}[/green]")
        with open(SITEMAP_FILE, "r", encoding="utf-8") as f:
            all_series = json.load(f)
        console.print(f"[bold]Loaded {len(all_series)} series from cache.[/bold]")
    else:
        pages_data = {}
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=35),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Scraping sitemap pages...", total=TOTAL_SITEMAP_PAGES)
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(fetch_sitemap_page, p): p for p in range(1, TOTAL_SITEMAP_PAGES + 1)}
                for future in as_completed(futures):
                    p_num, items = future.result()
                    pages_data[p_num] = items
                    progress.advance(task)

        seen = set()
        for p in range(1, TOTAL_SITEMAP_PAGES + 1):
            for item in pages_data.get(p, []):
                if item["url"] not in seen:
                    seen.add(item["url"])
                    all_series.append(item)

        with open(SITEMAP_FILE, "w", encoding="utf-8") as f:
            json.dump(all_series, f, ensure_ascii=False, indent=2)
        console.print(f"[green]✓ Saved {len(all_series)} series to {SITEMAP_FILE}[/green]")
        logger.info(f"Saved {len(all_series)} series to {SITEMAP_FILE}")

    LINKS_DIR.mkdir(parents=True, exist_ok=True)
    used_folders = set()
    created_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=35),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        task = progress.add_task("Verifying series folders...", total=len(all_series))
        for item in all_series:
            clean_name = sanitize_filename(item["title"])
            folder_name = clean_name
            suffix = 2
            while folder_name in used_folders:
                folder_name = f"{clean_name} ({suffix})"
                suffix += 1
            used_folders.add(folder_name)

            folder_path = LINKS_DIR / folder_name
            folder_path.mkdir(parents=True, exist_ok=True)
            info_file = folder_path / "info.json"

            if not info_file.exists():
                with open(info_file, "w", encoding="utf-8") as f:
                    json.dump({"title": item["title"], "url": item["url"]}, f, ensure_ascii=False, indent=2)
                created_count += 1
            progress.advance(task)

    console.print(f"[bold green]✓ Step 1 Complete: {len(all_series)} series folders ready ({created_count} newly initialized).[/bold green]\n")
    logger.info(f"Step 1 Complete: {len(all_series)} total folders, {created_count} newly created")


def extract_series_data(content_or_url: str) -> dict:
    if content_or_url.startswith("http://") or content_or_url.startswith("https://"):
        res = safe_get(content_or_url)
        if not res:
            return {}
        soup = BeautifulSoup(res.content, "html.parser")
        url = content_or_url
    else:
        soup = BeautifulSoup(content_or_url, "html.parser")
        url = ""

    data = {
        "title": "",
        "url": url,
        "poster": "",
        "episodes_url": "",
        "english_title": "",
        "arabic_title": "",
        "other_names": "",
        "genres": [],
        "episodes_count": 0,
        "country": "",
        "network": "",
        "broadcast_period": "",
        "story": "",
        "cast": []
    }

    title_el = soup.select_one(".b_title h3") or soup.find(itemprop="name")
    if title_el:
        data["title"] = title_el.get_text(strip=True)

    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content"):
        data["poster"] = og_img["content"]
    else:
        img_el = soup.select_one(".attachment-full")
        data["poster"] = img_el.get("src", "") if img_el else ""

    episodes_url = ""
    for a in soup.find_all("a", href=True):
        txt = a.get_text(strip=True)
        href = a["href"]
        if "حلقات الدراما" in txt or "مشاهدة حلقات" in txt:
            episodes_url = href
            break
        if ("cat=" in href or "/category/episodes/" in href) and "new" not in href and not href.startswith("#"):
            if not episodes_url:
                episodes_url = href

    data["episodes_url"] = episodes_url

    s_desc = soup.find(class_="s-desc")
    if s_desc:
        info_h3 = s_desc.find(lambda el: el.name in ["h2", "h3"] and "معلومات" in el.get_text())
        if info_h3:
            p_info = info_h3.find_next_sibling("p")
            if p_info:
                for span in p_info.find_all("span"):
                    key = span.get_text(strip=True).rstrip(":").strip()
                    next_node = span.next_sibling
                    val = ""
                    while next_node and getattr(next_node, "name", None) != "span":
                        if getattr(next_node, "name", None) == "br":
                            break
                        val += next_node.get_text() if hasattr(next_node, "get_text") else str(next_node)
                        next_node = next_node.next_sibling
                    val = val.strip().lstrip(":").strip()

                    if "المسلسل" in key:
                        data["english_title"] = val
                    elif "العربي" in key:
                        data["arabic_title"] = val
                    elif "أيضا" in key or "ايضا" in key:
                        data["other_names"] = val
                    elif "النوع" in key:
                        data["genres"] = [g.strip() for g in val.split(",") if g.strip()]
                    elif "الحلقات" in key:
                        m = re.search(r"\d+", val)
                        data["episodes_count"] = int(m.group()) if m else 0
                    elif "البلد" in key:
                        data["country"] = val
                    elif "الشبكة" in key or ("العرض" in key and "شبكة" in key):
                        data["network"] = val
                    elif "البث" in key:
                        data["broadcast_period"] = val

        story_h3 = s_desc.find(lambda el: el.name in ["h2", "h3"] and "القصة" in el.get_text())
        if story_h3:
            p_story = story_h3.find_next_sibling("p")
            data["story"] = p_story.get_text(strip=True) if p_story else ""

        cast_h3 = s_desc.find(lambda el: el.name in ["h2", "h3"] and any(k in el.get_text() for k in ["أبطال", "طاقم", "الابطال"]))
        if cast_h3:
            p_cast = cast_h3.find_next_sibling("p")
            if p_cast:
                cast_list = []
                for span in p_cast.find_all("span"):
                    role = span.get_text(strip=True)
                    prev = span.previous_sibling
                    actor = str(prev).replace(": في دور", "").replace("في دور", "").strip()
                    cast_list.append({"actor": actor, "role": role})
                data["cast"] = cast_list

    return data


def process_series_details(folder_path: Path) -> tuple[Path, bool, str]:
    info_file = folder_path / "info.json"
    try:
        with open(info_file, "r", encoding="utf-8") as f:
            existing = json.load(f)

        if existing.get("story") and existing.get("episodes_url"):
            return folder_path, True, "Cached"

        url = existing.get("url")
        if not url:
            return folder_path, False, "No URL"

        full_data = extract_series_data(url)
        if not full_data:
            return folder_path, False, "Failed to scrape"

        if not full_data.get("title") and existing.get("title"):
            full_data["title"] = existing["title"]
        if not full_data.get("url"):
            full_data["url"] = url
        if "episodes" in existing:
            full_data["episodes"] = existing["episodes"]

        with open(info_file, "w", encoding="utf-8") as f:
            json.dump(full_data, f, ensure_ascii=False, indent=2)

        logger.info(f"Updated series details for: {folder_path.name}")
        return folder_path, True, "Updated"
    except Exception as e:
        logger.error(f"Error processing series {folder_path.name}: {e}")
        return folder_path, False, str(e)


def run_step_2_series_data(workers: int = 20):
    console.print(Panel("[bold yellow]STEP 2: EXTRACT SERIES DATA & EPISODES URL[/bold yellow]", border_style="yellow"))
    logger.info("--- Starting Step 2: Series Data ---")

    folders = [f for f in LINKS_DIR.iterdir() if f.is_dir() and (f / "info.json").exists()]
    pending = []

    for f in folders:
        try:
            with open(f / "info.json", "r", encoding="utf-8") as jf:
                d = json.load(jf)
            if not d.get("story") or not d.get("episodes_url"):
                pending.append(f)
        except Exception:
            pending.append(f)

    console.print(f"Total Series: [bold]{len(folders)}[/bold] | Already Completed: [bold green]{len(folders) - len(pending)}[/bold green] | Pending: [bold yellow]{len(pending)}[/bold yellow]")

    if not pending:
        console.print("[green]✓ All series data already up to date. Skipping step 2.[/green]\n")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold yellow]{task.description}"),
        BarColumn(bar_width=35),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        TextColumn("[cyan]{task.fields[current]}"),
        console=console
    ) as progress:
        task = progress.add_task("Extracting series data...", total=len(pending), current="")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_series_details, folder): folder for folder in pending}
            for future in as_completed(futures):
                folder, ok, msg = future.result()
                progress.update(task, current=folder.name[:25])
                progress.advance(task)

    console.print("[bold green]✓ Step 2 Complete: Series details updated.[/bold green]\n")
    logger.info("Step 2 Complete")


def fetch_all_episodes(episodes_url: str) -> list[dict]:
    if not episodes_url:
        return []

    res = safe_get(episodes_url)
    if not res:
        return []

    canonical_url = res.url.rstrip("/") + "/"
    soup = BeautifulSoup(res.content, "html.parser")

    total_pages = 1
    pag = soup.find(class_="wp-pagenavi") or soup.find(class_="pagination")
    if pag:
        pages_span = pag.find(class_="pages")
        if pages_span:
            m = re.search(r"من\s*(\d+)", pages_span.get_text())
            if m:
                total_pages = int(m.group(1))
        if total_pages == 1:
            for a in pag.find_all("a", href=True):
                m = re.search(r"/page/(\d+)", a["href"])
                if m:
                    total_pages = max(total_pages, int(m.group(1)))

    all_episodes = []

    def parse_page(page_soup):
        page_eps = []
        for article in page_soup.find_all("article"):
            h3 = article.find("h3")
            title = h3.get_text(strip=True) if h3 else ""
            link_el = article.find("a", class_="first_A") or (h3.find_parent("a") if h3 else None) or article.find("a", href=True)
            url = link_el.get("href", "").strip() if link_el else ""
            if url and title:
                page_eps.append({
                    "title": title,
                    "url": url,
                    "player": "",
                    "servers": {}
                })
        return page_eps

    all_episodes.extend(parse_page(soup))

    for p in range(2, total_pages + 1):
        page_url = f"{canonical_url}&paged={p}" if "?" in canonical_url else f"{canonical_url}page/{p}/"
        r = safe_get(page_url)
        if r:
            s = BeautifulSoup(r.content, "html.parser")
            all_episodes.extend(parse_page(s))

    all_episodes.reverse()

    for idx, ep in enumerate(all_episodes, start=1):
        m = re.search(r"الحلقة\s*(\d+)", ep["title"])
        ep["episode"] = int(m.group(1)) if m else idx

    return all_episodes


def process_series_episodes(folder_path: Path) -> tuple[Path, int]:
    info_file = folder_path / "info.json"
    try:
        with open(info_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        if data.get("episodes") and len(data["episodes"]) > 0:
            return folder_path, len(data["episodes"])

        episodes_url = data.get("episodes_url")
        if not episodes_url:
            return folder_path, 0

        episodes = fetch_all_episodes(episodes_url)
        if episodes:
            data["episodes"] = episodes
            with open(info_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Extracted {len(episodes)} episodes for: {folder_path.name}")
            return folder_path, len(episodes)
        return folder_path, 0
    except Exception as e:
        logger.error(f"Error fetching episodes for {folder_path.name}: {e}")
        return folder_path, 0


def run_step_3_episodes(workers: int = 15):
    console.print(Panel("[bold magenta]STEP 3: EXTRACT ALL EPISODE LINKS[/bold magenta]", border_style="magenta"))
    logger.info("--- Starting Step 3: Episodes Links ---")

    folders = [f for f in LINKS_DIR.iterdir() if f.is_dir() and (f / "info.json").exists()]
    pending = []

    for f in folders:
        try:
            with open(f / "info.json", "r", encoding="utf-8") as jf:
                d = json.load(jf)
            if not d.get("episodes") or len(d["episodes"]) == 0:
                if d.get("episodes_url"):
                    pending.append(f)
        except Exception:
            pass

    console.print(f"Total Series: [bold]{len(folders)}[/bold] | Already have episodes: [bold green]{len(folders) - len(pending)}[/bold green] | Pending: [bold magenta]{len(pending)}[/bold magenta]")

    if not pending:
        console.print("[green]✓ All series already have episode links. Skipping step 3.[/green]\n")
        return

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
        task = progress.add_task("Fetching all episodes...", total=len(pending), current="")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_series_episodes, folder): folder for folder in pending}
            for future in as_completed(futures):
                folder, count = future.result()
                progress.update(task, current=f"{folder.name[:20]} ({count} eps)")
                progress.advance(task)

    console.print("[bold green]✓ Step 3 Complete: All episodes extracted.[/bold green]\n")
    logger.info("Step 3 Complete")


def extract_episode_servers_unfiltered(ep_url: str) -> tuple[str, dict]:
    res = safe_get(ep_url, timeout=15)
    if not res:
        return "", {}

    soup = BeautifulSoup(res.content, "html.parser")
    servers = {}
    active_player = ""

    for li in soup.select("li.server[data-url]"):
        name = li.get_text(strip=True)
        s_url = li.get("data-url", "").strip()
        if name and s_url:
            servers[name] = s_url
            if "active" in li.get("class", []):
                active_player = s_url

    if not servers:
        for ifr in soup.find_all("iframe"):
            src = ifr.get("src") or ifr.get("data-src") or ""
            if src.startswith("http"):
                domain = urllib.parse.urlparse(src).netloc.replace("www.", "").split(".")[0].capitalize()
                servers[domain] = src

    iframe = soup.select_one(".iframe iframe[src]")
    if iframe and iframe.get("src"):
        active_player = iframe["src"].strip()

    if not active_player and servers:
        active_player = next(iter(servers.values()))

    return active_player, servers


def process_series_servers(folder_path: Path) -> tuple[Path, int]:
    info_file = folder_path / "info.json"
    try:
        with open(info_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        episodes = data.get("episodes", [])
        if not episodes:
            return folder_path, 0

        updated_count = 0
        for ep in episodes:
            if ep.get("servers") and len(ep["servers"]) > 0 and ep.get("player"):
                continue

            ep_url = ep.get("url")
            if not ep_url:
                continue

            player, servers = extract_episode_servers_unfiltered(ep_url)
            ep["player"] = player
            ep["servers"] = servers
            updated_count += 1

        if updated_count > 0:
            with open(info_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Updated servers for {updated_count} episodes in: {folder_path.name}")

        return folder_path, updated_count
    except Exception as e:
        logger.error(f"Error processing servers in {folder_path.name}: {e}")
        return folder_path, 0


def run_step_4_servers(workers: int = 15):
    console.print(Panel("[bold green]STEP 4: EXTRACT ALL EMBED & WATCH SERVERS (UNFILTERED)[/bold green]", border_style="green"))
    logger.info("--- Starting Step 4: Video Servers ---")

    folders = [f for f in LINKS_DIR.iterdir() if f.is_dir() and (f / "info.json").exists()]
    pending = []

    for f in folders:
        try:
            with open(f / "info.json", "r", encoding="utf-8") as jf:
                d = json.load(jf)
            eps = d.get("episodes", [])
            if eps and any(not ep.get("servers") or len(ep.get("servers", {})) == 0 for ep in eps):
                pending.append(f)
        except Exception:
            pass

    console.print(f"Total Series: [bold]{len(folders)}[/bold] | Already have servers: [bold green]{len(folders) - len(pending)}[/bold green] | Pending: [bold green]{len(pending)}[/bold green]")

    if not pending:
        console.print("[green]✓ All episodes already have watch servers. Skipping step 4.[/green]\n")
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
        task = progress.add_task("Extracting episode servers...", total=len(pending), current="")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_series_servers, folder): folder for folder in pending}
            for future in as_completed(futures):
                folder, cnt = future.result()
                progress.update(task, current=f"{folder.name[:20]}")
                progress.advance(task)

    console.print("[bold green]✓ Step 4 Complete: All servers extracted successfully.[/bold green]\n")
    logger.info("Step 4 Complete")


def print_summary_table():
    folders = [f for f in LINKS_DIR.iterdir() if f.is_dir() and (f / "info.json").exists()]
    total_series = len(folders)
    with_story = 0
    with_episodes_url = 0
    with_episodes = 0
    total_episodes = 0
    total_episodes_with_servers = 0

    for f in folders:
        try:
            with open(f / "info.json", "r", encoding="utf-8") as jf:
                d = json.load(jf)
            if d.get("story"):
                with_story += 1
            if d.get("episodes_url"):
                with_episodes_url += 1
            eps = d.get("episodes", [])
            if eps:
                with_episodes += 1
                total_episodes += len(eps)
                total_episodes_with_servers += sum(1 for ep in eps if ep.get("servers") and len(ep["servers"]) > 0)
        except Exception:
            pass

    table = Table(title="[bold green]ARADRAMATV AUTOMATION SUMMARY[/bold green]", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right", style="bold")
    table.add_column("Status", justify="center")

    table.add_row("Total Series Folders", str(total_series), "[green]Active[/green]")
    table.add_row("Series with Full Details (Step 2)", str(with_story), f"[cyan]{with_story}/{total_series}[/cyan]")
    table.add_row("Series with Episodes URL (Step 2)", str(with_episodes_url), f"[cyan]{with_episodes_url}/{total_series}[/cyan]")
    table.add_row("Series with Episodes Extracted (Step 3)", str(with_episodes), f"[cyan]{with_episodes}/{total_series}[/cyan]")
    table.add_row("Total Episodes Scraped", str(total_episodes), "[green]Collected[/green]")
    table.add_row("Episodes with Servers (Step 4)", str(total_episodes_with_servers), f"[green]{total_episodes_with_servers}/{total_episodes}[/green]")

    console.print(table)


def main():
    parser = argparse.ArgumentParser(description="AradramaTv Full Automation Pipeline")
    parser.add_argument("--step", choices=["1", "2", "3", "4", "all"], default="all", help="Pipeline step to execute (default: all)")
    parser.add_argument("--force", action="store_true", help="Force re-scraping sitemap")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of concurrent worker threads")
    args = parser.parse_args()

    banner = """
    ╔════════════════════════════════════════════════════════════════════╗
    ║            ARADRAMATV COMPLETE SCRAPING PIPELINE                  ║
    ║   End-to-End Crawler: Sitemap ➔ Series ➔ Episodes ➔ Servers        ║
    ╚════════════════════════════════════════════════════════════════════╝
    """
    console.print(Panel(banner, style="bold blue", subtitle=f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"))

    start_time = time.time()
    try:
        if args.step in ["1", "all"]:
            run_step_1_sitemap(force=args.force, workers=args.workers)

        if args.step in ["2", "all"]:
            run_step_2_series_data(workers=args.workers)

        if args.step in ["3", "all"]:
            run_step_3_episodes(workers=args.workers)

        if args.step in ["4", "all"]:
            run_step_4_servers(workers=args.workers)

        print_summary_table()
        elapsed = round(time.time() - start_time, 2)
        console.print(f"[bold green]✨ Pipeline finished in {elapsed} seconds! Log saved to {LOG_FILE}[/bold green]")
        logger.info(f"Pipeline finished in {elapsed} seconds")

    except KeyboardInterrupt:
        console.print("\n[bold red]✖ Process interrupted by user. Safe exit (all completed items are preserved).[/bold red]")
        logger.warning("Pipeline interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()

