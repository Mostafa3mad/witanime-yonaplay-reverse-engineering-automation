import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from bs4 import BeautifulSoup
from curl_cffi import requests
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent.parent
LINKS_DIR = BASE_DIR / "aradramatv_links"
MAX_WORKERS = 50
MAX_RETRIES = 3


def safe_get(url: str, retries: int = MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            res = requests.get(url, impersonate="chrome", timeout=25)
            if res.status_code == 200:
                return res
            time.sleep(attempt * 1.5)
        except Exception:
            time.sleep(attempt * 1.5)
    return None


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
        if "?" in canonical_url:
            page_url = f"{canonical_url}&paged={p}"
        else:
            page_url = f"{canonical_url}page/{p}/"
        r = safe_get(page_url)
        if r:
            s = BeautifulSoup(r.content, "html.parser")
            all_episodes.extend(parse_page(s))

    all_episodes.reverse()

    for idx, ep in enumerate(all_episodes, start=1):
        m = re.search(r"الحلقة\s*(\d+)", ep["title"])
        ep["episode"] = int(m.group(1)) if m else idx

    return all_episodes


def process_series_folder(folder_path: Path) -> bool:
    info_file = folder_path / "info.json"
    if not info_file.exists():
        return False

    try:
        with open(info_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        if data.get("episodes") and len(data["episodes"]) > 0:
            return True

        episodes_url = data.get("episodes_url")
        if not episodes_url:
            return False

        episodes = fetch_all_episodes(episodes_url)
        if not episodes:
            return False

        data["episodes"] = episodes

        with open(info_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return True
    except Exception:
        return False


def main():
    if not LINKS_DIR.exists():
        print(f"Directory not found: {LINKS_DIR}")
        return

    folders = [f for f in LINKS_DIR.iterdir() if f.is_dir() and (f / "info.json").exists()]
    completed = []
    pending = []
    missing_ep_url = []

    for f in folders:
        try:
            with open(f / "info.json", "r", encoding="utf-8") as jf:
                d = json.load(jf)
            if d.get("episodes") and len(d["episodes"]) > 0:
                completed.append(f)
            elif d.get("episodes_url"):
                pending.append(f)
            else:
                missing_ep_url.append(f)
        except Exception:
            pending.append(f)

    print(f"Total series folders          : {len(folders)}")
    print(f"Already have episodes         : {len(completed)}")
    print(f"Ready to extract episodes     : {len(pending)}")
    print(f"Missing episodes_url (step 2) : {len(missing_ep_url)}")

    if not pending:
        print("No pending series with episodes_url to process.")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_series_folder, folder): folder for folder in pending}
        for _ in tqdm(as_completed(futures), total=len(pending), desc="Extracting episodes"):
            pass

    print("[✓] Finished extracting episodes.")


if __name__ == "__main__":
    main()

