import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from bs4 import BeautifulSoup
from curl_cffi import requests
from tqdm import tqdm

BASE_URL = "https://aradramatv.cc/category/serie/page/{page}/"
OUTPUT_FILE = Path(__file__).resolve().parent.parent / "sitemap.json"
LINKS_DIR = Path(__file__).resolve().parent.parent / "aradramatv_links"
TOTAL_PAGES = 82
MAX_WORKERS = 30
MAX_RETRIES = 3


def sanitize_filename(name: str) -> str:
    clean = re.sub(r'[\\/*?:"<>|]', "", name).strip().rstrip(".")
    return clean or "Unnamed_Series"


def save_series_folders(series_list: list[dict]):
    LINKS_DIR.mkdir(parents=True, exist_ok=True)
    used_folders = set()

    for item in series_list:
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
        with open(info_file, "w", encoding="utf-8") as f:
            json.dump({
                "title": item["title"],
                "url": item["url"]
            }, f, ensure_ascii=False, indent=2)


def fetch_page(page: int) -> tuple[int, list[dict]]:
    url = BASE_URL.format(page=page)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                impersonate="chrome",
                timeout=25,
            )
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                articles = soup.find_all("article")
                page_items = []
                for article in articles:
                    h3 = article.find("h3")
                    title = h3.get_text(strip=True) if h3 else None

                    link_el = article.select_one("a.first_A")
                    if not link_el and h3:
                        link_el = h3.find_parent("a")
                    if not link_el:
                        link_el = article.find("a", href=True)

                    url_href = link_el.get("href") if link_el else None

                    if not title and link_el and link_el.get("title"):
                        title = link_el["title"].strip()

                    if url_href and title:
                        page_items.append({
                            "title": title,
                            "url": url_href.strip(),
                        })
                return page, page_items
            elif response.status_code == 404:
                return page, []
            else:
                time.sleep(attempt * 1.5)
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"\n[!] Error on page {page}: {e}")
                return page, []
            time.sleep(attempt * 1.5)
    return page, []


def main():
    print(f"[*] Extracting series links from page 1 to {TOTAL_PAGES}...")

    pages_data = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_page, p): p for p in range(1, TOTAL_PAGES + 1)}
        for future in tqdm(as_completed(futures), total=TOTAL_PAGES, desc="Fetching pages"):
            page_num, items = future.result()
            pages_data[page_num] = items

    all_series = []
    seen_urls = set()

    for page in range(1, TOTAL_PAGES + 1):
        items = pages_data.get(page, [])
        for item in items:
            url = item["url"]
            if url not in seen_urls:
                seen_urls.add(url)
                all_series.append(item)

    print(f"\n[+] Total series extracted: {len(all_series)}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_series, f, ensure_ascii=False, indent=2)

    print(f"[✓] Saved sitemap to: {OUTPUT_FILE}")

    save_series_folders(all_series)
    print(f"[✓] Created series folders and info.json in: {LINKS_DIR}")


if __name__ == "__main__":
    main()
