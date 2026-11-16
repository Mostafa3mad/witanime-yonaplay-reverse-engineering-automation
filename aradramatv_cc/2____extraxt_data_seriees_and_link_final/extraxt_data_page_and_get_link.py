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


def process_series_folder(folder_path: Path) -> bool:
    info_file = folder_path / "info.json"
    if not info_file.exists():
        return False

    try:
        with open(info_file, "r", encoding="utf-8") as f:
            existing = json.load(f)

        if existing.get("story") and existing.get("episodes_url"):
            return True

        url = existing.get("url")
        if not url:
            return False

        full_data = extract_series_data(url)
        if not full_data:
            return False

        if not full_data.get("title") and existing.get("title"):
            full_data["title"] = existing["title"]
        if not full_data.get("url"):
            full_data["url"] = url

        with open(info_file, "w", encoding="utf-8") as f:
            json.dump(full_data, f, ensure_ascii=False, indent=2)

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
            if not d.get("story") or not d.get("episodes_url"):
                pending.append(f)
        except Exception:
            pending.append(f)

    print(f"Total series folders : {len(folders)}")
    print(f"Already completed    : {len(folders) - len(pending)}")
    print(f"Pending to process   : {len(pending)}")

    if not pending:
        print("All series are already up to date.")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_series_folder, folder): folder for folder in pending}
        for _ in tqdm(as_completed(futures), total=len(pending), desc="Processing series"):
            pass

    print("[✓] Finished processing all series.")


if __name__ == "__main__":
    main()
