import os
import json
import shutil
import requests
import base64
import hashlib
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# CONFIG
# ============================================================

SOURCE_DIR = r"E:\Series Automation\witanime_you\sss_new_scrap"
DEST_DIR = r"E:\Series Automation\witanime_you\Data"

BASE_URL = "https://mid.yonaplay.net"

MAX_THREADS = 20

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)

print_lock = Lock()


# ============================================================
# SAFE PRINT
# ============================================================
def decryptAES(b64_data, key):
    # Base64 -> bytes
    raw = base64.b64decode(b64_data)

    # نفس تقسيم JavaScript
    iv = raw[:12]
    tag = raw[12:28]
    ciphertext = raw[28:]

    # SHA-256 للـ key
    aes_key = hashlib.sha256(
        key.encode("utf-8")
    ).digest()

    # WebCrypto AES-GCM
    encrypted = ciphertext + tag

    plaintext = AESGCM(aes_key).decrypt(
        iv,
        encrypted,
        None
    )

    return plaintext.decode("utf-8")
def safe_print(*args, **kwargs):

    with print_lock:
        print(*args, **kwargs)


# ============================================================
# CREATE SESSION
# ============================================================

def create_session():

    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,

        backoff_factor=2,

        status_forcelist=[
            404,
            429,
            500,
            502,
            503,
            504,
        ],

        allowed_methods=[
            "GET",
            "POST",
        ],

        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry
    )

    session.mount(
        "http://",
        adapter
    )

    session.mount(
        "https://",
        adapter
    )

    return session


# ============================================================
# OPEN EMBED
# ============================================================

def get_embed(session, embed_url):

    headers = {
        "accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,*/*;q=0.8"
        ),
        "accept-language": (
            "en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7"
        ),
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "referer": "https://witanime.you/",
        "user-agent": USER_AGENT,
    }

    response = session.get(
        embed_url,
        headers=headers,
        timeout=30,
    )

    response.raise_for_status()

    return response


# ============================================================
# INIT SESSION
# ============================================================

def init_session(session, embed_url):

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": BASE_URL,
        "referer": embed_url,
        "user-agent": USER_AGENT,
        "x-requested-with": "XMLHttpRequest",
    }

    response = session.post(
        f"{BASE_URL}/api/init-session.php",
        headers=headers,
        json={},
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):

        raise RuntimeError(
            data.get(
                "error",
                "init-session failed"
            )
        )

    return data


# ============================================================
# GET SOURCES
# ============================================================

def get_sources(session, embed_url, code):

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": BASE_URL,
        "referer": embed_url,
        "user-agent": USER_AGENT,
        "x-requested-with": "XMLHttpRequest",
    }

    response = session.post(
        f"{BASE_URL}/api/sources.php",
        headers=headers,
        json={
            "code": code
        },
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):

        raise RuntimeError(
            data.get(
                "error",
                "sources.php failed"
            )
        )

    return data


# ============================================================
# API.PHP
# ============================================================

def get_api_response(
    session,
    embed_url,
    code,
    page_key,
    server_token
):

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": BASE_URL,
        "referer": embed_url,
        "user-agent": USER_AGENT,
        "x-requested-with": "XMLHttpRequest",
    }

    payload = {
        "code": code,
        "token": server_token,
        "key": page_key,
    }

    response = session.post(
        f"{BASE_URL}/api/api.php",
        headers=headers,
        json=payload,
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):

        raise RuntimeError(
            data.get(
                "error",
                "api.php failed"
            )
        )
    print(data)
    return data


# ============================================================
# EXTRACT ALL SERVERS
# ============================================================

def get_all_servers(sources):

    servers = []

    qualities = sources.get(
        "qualities",
        {}
    )

    for quality, quality_data in qualities.items():

        quality_label = quality_data.get(
            "label",
            quality
        )

        for server in quality_data.get(
            "servers",
            []
        ):

            token = server.get("token")

            if not token:
                continue

            servers.append({
                "name": server.get(
                    "name",
                    "unknown"
                ),
                "token": token,
            })
    print(servers)
    return servers


# ============================================================
# PROCESS ONE YONAPLAY URL
# ============================================================

def process_yonaplay_url(yonaplay_url):

    session = create_session()

    # --------------------------------------------------------
    # 1. Open embed
    # --------------------------------------------------------

    get_embed(
        session,
        yonaplay_url
    )

    # --------------------------------------------------------
    # 2. Init session
    # --------------------------------------------------------

    init_data = init_session(
        session,
        yonaplay_url
    )

    code = init_data["c"]
    page_key = init_data["k"]

    # --------------------------------------------------------
    # 3. Get sources
    # --------------------------------------------------------

    sources = get_sources(
        session,
        yonaplay_url,
        code
    )

    # --------------------------------------------------------
    # 4. Extract servers
    # --------------------------------------------------------

    servers = get_all_servers(
        sources
    )

    if not servers:

        raise RuntimeError(
            "No servers found"
        )

    # --------------------------------------------------------
    # 5. Get API response for every server
    # --------------------------------------------------------

    result_servers = {}

    for server in servers:

        data = get_api_response(
            session,
            yonaplay_url,
            code,
            page_key,
            server["token"]
        )

        d_value = data.get("d")

        d_value = decryptAES(data.get("d"), page_key)


        if not d_value:

            continue

        server_name = server["name"]


        if server_name in result_servers:

            server_name = (
                f"{server_name}_"
            )

        result_servers[server_name] = d_value

    if not result_servers:

        raise RuntimeError(
            "No API results returned"
        )

    return {
        "code": code,
        "servers": result_servers,
    }


# ============================================================
# PROCESS ONE EPISODE
# ============================================================
def process_episode(args):

    episode_index, episode = args

    servers = episode.get(
        "servers"
    )

    if not isinstance(
        servers,
        dict
    ):

        return (
            episode_index,
            False,
            None
        )

    # --------------------------------------------------------
    # Get Yonaplay URL
    # --------------------------------------------------------

    yonaplay_url = servers.get(
        "yonaplay"
    )

    if not yonaplay_url:

        return (
            episode_index,
            False,
            None
        )

    episode_number = episode.get(
        "episode",
        "?"
    )

    # ========================================================
    # RETRY
    # ========================================================

    max_retries = 5

    for attempt in range(
        1,
        max_retries + 1
    ):

        try:

            safe_print(
                f"      [TRY] Episode "
                f"{episode_number} | "
                f"Attempt {attempt}/{max_retries}"
            )

            # ------------------------------------------------
            # Process Yonaplay
            # ------------------------------------------------
            # مهم:
            # كل Retry هنا يستدعي الدالة من البداية
            #
            # process_yonaplay_url()
            #       ↓
            # create_session()
            #       ↓
            # get_embed()
            #       ↓
            # init_session()
            #       ↓
            # get_sources()
            #       ↓
            # api.php
            #       ↓
            # decrypt
            # ------------------------------------------------

            result = process_yonaplay_url(
                yonaplay_url
            )

            # ------------------------------------------------
            # IMPORTANT:
            # Remove Yonaplay completely
            # ------------------------------------------------

            servers.pop(
                "yonaplay",
                None
            )

            # ------------------------------------------------
            # Add ONLY the resulting servers
            # ------------------------------------------------

            for server_name, server_data in (
                result["servers"].items()
            ):

                servers[server_name] = server_data

            safe_print(
                f"      [OK] Episode "
                f"{episode_number} | "
                f"Code: {result['code']} | "
                f"Servers: "
                f"{', '.join(result['servers'].keys())}"
            )

            return (
                episode_index,
                True,
                {
                    "episode": episode_number,
                    "code": result["code"],
                    "servers": list(
                        result["servers"].keys()
                    )
                }
            )

        except Exception as e:

            safe_print(
                f"      [ERROR] Episode "
                f"{episode_number} | "
                f"Attempt {attempt}/{max_retries} | "
                f"{e}"
            )



            if attempt < max_retries:

                safe_print(
                    f"      [RETRY] Episode "
                    f"{episode_number} | "
                    f"Restarting from beginning..."
                )

                continue

            # ------------------------------------------------
            # كل المحاولات فشلت
            # ------------------------------------------------

            safe_print(
                f"      [FAILED] Episode "
                f"{episode_number} | "
                f"All {max_retries} attempts failed"
            )

            return (
                episode_index,
                False,
                {
                    "episode": episode_number,
                    "error": str(e)
                }
            )

# ============================================================
# PROCESS INFO.JSON
# ============================================================

def process_info_json(
    source_info,
    destination_info
):

    # --------------------------------------------------------
    # Read JSON
    # --------------------------------------------------------

    with open(
        source_info,
        "r",
        encoding="utf-8"
    ) as f:

        data = json.load(f)

    episodes = data.get(
        "episodes",
        []
    )

    if not episodes:

        return (
            False,
            0,
            0
        )

    replaced = 0
    errors = 0

    # --------------------------------------------------------
    # Process episodes concurrently
    # --------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_THREADS
    ) as executor:

        futures = [
            executor.submit(
                process_episode,
                (
                    index,
                    episode
                )
            )
            for index, episode in enumerate(
                episodes
            )
        ]

        for future in as_completed(
            futures
        ):

            index, success, result = (
                future.result()
            )

            if success:

                replaced += 1

                safe_print(
                    f"      [OK] "
                    f"Episode {result['episode']} "
                    f"| Code: {result['code']} "
                    f"| Servers: "
                    f"{', '.join(result['servers'])}"
                )

            elif result:

                errors += 1

                safe_print(
                    f"      [ERROR] "
                    f"Episode {result['episode']} "
                    f"| {result['error']}"
                )

    # --------------------------------------------------------
    # Save modified JSON
    # --------------------------------------------------------

    os.makedirs(
        os.path.dirname(
            destination_info
        ),
        exist_ok=True
    )

    with open(
        destination_info,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=4
        )

    return (
        replaced > 0,
        replaced,
        errors
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 80)
    print("YONAPLAY MID SERVER REPLACER")
    print("=" * 80)

    print(
        f"Source  : {SOURCE_DIR}"
    )

    print(
        f"Output  : {DEST_DIR}"
    )

    print(
        f"Threads : {MAX_THREADS}"
    )

    print("=" * 80)
    print()

    # --------------------------------------------------------
    # Check source
    # --------------------------------------------------------

    if not os.path.exists(
        SOURCE_DIR
    ):

        print(
            "[ERROR] Source folder not found:"
        )

        print(
            SOURCE_DIR
        )

        return

    os.makedirs(
        DEST_DIR,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Find all info.json
    # --------------------------------------------------------

    info_files = []

    for root, dirs, files in os.walk(
        SOURCE_DIR
    ):

        if "info.json" in files:

            info_files.append(
                os.path.join(
                    root,
                    "info.json"
                )
            )

    total_info = len(
        info_files
    )

    print(
        f"Found {total_info} info.json files"
    )

    modified_info = 0
    total_replaced = 0
    total_errors = 0

    # --------------------------------------------------------
    # Process every info.json
    # --------------------------------------------------------

    for counter, source_info in enumerate(
        info_files,
        1
    ):

        root = os.path.dirname(
            source_info
        )

        relative_path = os.path.relpath(
            root,
            SOURCE_DIR
        )

        destination_folder = os.path.join(
            DEST_DIR,
            relative_path
        )

        destination_info = os.path.join(
            destination_folder,
            "info.json"
        )

        print()
        print("=" * 80)

        print(
            f"[{counter}/{total_info}] "
            f"{relative_path}"
        )

        print("=" * 80)

        # ----------------------------------------------------
        # Copy entire folder
        # ----------------------------------------------------

        try:

            if not os.path.exists(
                destination_folder
            ):

                shutil.copytree(
                    root,
                    destination_folder
                )

        except Exception as e:

            print(
                f"[COPY ERROR] {e}"
            )

            total_errors += 1

            continue

        # ----------------------------------------------------
        # Process info.json
        # ----------------------------------------------------

        try:

            changed, replaced, errors = (
                process_info_json(
                    source_info,
                    destination_info
                )
            )

            if changed:

                modified_info += 1
                total_replaced += replaced

                print()
                print(
                    f"    [DONE] "
                    f"Replaced: {replaced}"
                )

            else:

                print(
                    "    [SKIP] "
                    "No yonaplay server found"
                )

            total_errors += errors

        except Exception as e:

            print(
                f"[ERROR] {e}"
            )

            total_errors += 1

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    print()
    print()
    print("=" * 80)
    print("DONE")
    print("=" * 80)

    print(
        f"info.json files   : {total_info}"
    )

    print(
        f"Files modified    : {modified_info}"
    )

    print(
        f"Episodes replaced : {total_replaced}"
    )

    print(
        f"Errors             : {total_errors}"
    )

    print(
        f"Threads            : {MAX_THREADS}"
    )

    print()
    print(
        "Output:"
    )

    print(
        DEST_DIR
    )

    print("=" * 80)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()