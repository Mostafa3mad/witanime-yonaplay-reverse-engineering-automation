import json
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from bs4 import BeautifulSoup
from curl_cffi import requests
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent.parent
LINKS_DIR = BASE_DIR / "aradramatv_links"
SERIES_WORKERS = 5
SERVER_WORKERS = 30
CHECK_TIMEOUT = 8

DEAD_KEYWORDS = [
    "file was deleted", "file not found", "video not found", "has been removed",
    "video has been deleted", "deleted for copyright", "video unavailable",
    "video is unavailable", "no longer available", "the file has expired",
    "the file was removed", "file has been removed", "file does not exist",
    "page not found", "404 not found", "video_not_found", "not found or deleted",
    "content has been deleted", "видео не найдено", "видеозапись удалена",
    "файл удален", "файл не найден", "video removed", "this video has been removed"
]


def safe_get(url: str, retries: int = 3, timeout: int = 20):
    for attempt in range(1, retries + 1):
        try:
            res = requests.get(url, impersonate="chrome", timeout=timeout)
            if res.status_code == 200:
                return res
            time.sleep(attempt * 1.5)
        except Exception:
            time.sleep(attempt * 1.5)
    return None


def check_server(name: str, url: str) -> tuple[str, str, bool]:
    try:
        r = requests.get(
            url,
            impersonate="chrome",
            timeout=CHECK_TIMEOUT,
            headers={"Referer": "https://aradramatv.cc/"}
        )
        if r.status_code not in (200, 206):
            return name, url, False

        text = r.text.lower()
        if len(text) < 150:
            return name, url, False

        for kw in DEAD_KEYWORDS:
            if kw in text:
                return name, url, False

        return name, url, True
    except Exception:
        return name, url, False


def extract_episode_servers(ep_url: str) -> tuple[str, dict]:
    res = safe_get(ep_url)
    if not res:
        return "", {}

    soup = BeautifulSoup(res.content, "html.parser")
    candidates = {}
    active_url = ""

    for li in soup.select("li.server[data-url]"):
        name = li.get_text(strip=True)
        s_url = li.get("data-url", "").strip()
        if name and s_url:
            candidates[name] = s_url
            if "active" in li.get("class", []):
                active_url = s_url

    if not candidates:
        for ifr in soup.find_all("iframe"):
            src = ifr.get("src") or ifr.get("data-src") or ""
            if src.startswith("http"):
                domain = urllib.parse.urlparse(src).netloc.replace("www.", "").split(".")[0].capitalize()
                candidates[domain] = src

    iframe = soup.select_one(".iframe iframe[src]")
    if iframe and iframe.get("src"):
        active_url = iframe["src"].strip()

    valid_servers = {}
    with ThreadPoolExecutor(max_workers=SERVER_WORKERS) as executor:
        results = list(executor.map(lambda item: check_server(*item), candidates.items()))

    for name, s_url, is_ok in results:
        if is_ok:
            valid_servers[name] = s_url

    if active_url and active_url in valid_servers.values():
        player = active_url
    elif valid_servers:
        player = next(iter(valid_servers.values()))
    else:
        player = ""

    return player, valid_servers


def process_series_folder(folder_path: Path) -> bool:
    info_file = folder_path / "info.json"
    if not info_file.exists():
        return False

    try:
        with open(info_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        episodes = data.get("episodes")
        if not episodes:
            return False

        updated = False
        for ep in episodes:
            if ep.get("servers") and len(ep["servers"]) > 0 and ep.get("player"):
                continue

            ep_url = ep.get("url")
            if not ep_url:
                continue

            player, servers = extract_episode_servers(ep_url)
            ep["player"] = player
            ep["servers"] = servers
            updated = True

        if updated:
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
    pending = []

    for f in folders:
        try:
            with open(f / "info.json", "r", encoding="utf-8") as jf:
                d = json.load(jf)
            eps = d.get("episodes", [])
            if not eps:
                continue
            needs_servers = any(not ep.get("servers") or len(ep.get("servers", {})) == 0 for ep in eps)
            if needs_servers:
                pending.append(f)
        except Exception:
            pending.append(f)

    print(f"Total series folders          : {len(folders)}")
    print(f"Pending for server extraction : {len(pending)}")

    if not pending:
        print("All series already have valid servers.")
        return

    with ThreadPoolExecutor(max_workers=SERIES_WORKERS) as executor:
        futures = {executor.submit(process_series_folder, folder): folder for folder in pending}
        for _ in tqdm(as_completed(futures), total=len(pending), desc="Extracting servers"):
            pass

    print("[✓] Finished extracting servers for all episodes.")


if __name__ == "__main__":
    main()
