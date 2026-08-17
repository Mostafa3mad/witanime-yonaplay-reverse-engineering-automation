import base64
import json
import re
import shutil
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


# ============================================================
# CONFIG
# ============================================================

OUTPUT_DIR = Path(
    r"E:\Series Automation\witanime_you\output"
)

FINISHED_DIR = Path(
    r"E:\Series Automation\witanime_you\finished"
)

MAX_WORKERS = 5

TIMEOUT = 30

SAVE_BACKUP = True


# ============================================================
# PRINT LOCK
# ============================================================

print_lock = threading.Lock()


def safe_print(*args, **kwargs):

    with print_lock:

        print(
            *args,
            **kwargs
        )


# ============================================================
# CREATE SESSION
# ============================================================

def create_session():

    session = requests.Session()

    session.headers.update({

        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/151.0.0.0 Safari/537.36"
        ),

        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,image/avif,"
            "image/webp,image/apng,*/*;q=0.8"
        ),

        "Accept-Language":
            "en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7",

        "Cache-Control":
            "no-cache",

        "Pragma":
            "no-cache",

        "Upgrade-Insecure-Requests":
            "1",

    })

    return session


# ============================================================
# GET PAGE
# ============================================================

def get_page(
    session,
    url
):

    response = session.get(
        url,
        timeout=TIMEOUT
    )

    return response


# ============================================================
# CLEAN TEXT
# ============================================================

def clean_text(
    value
):

    if not value:
        return ""

    return re.sub(
        r"\s+",
        " ",
        str(value)
    ).strip()


# ============================================================
# CLEAN URL
# ============================================================

def clean_url(
    value
):

    if not value:
        return ""

    value = str(
        value
    ).strip()

    # --------------------------------------------------------
    # Markdown:
    #
    # [https://example.com](https://example.com)
    # --------------------------------------------------------

    match = re.match(
        r"^\[.*?\]\((https?://[^)]+)\)$",
        value
    )

    if match:

        return match.group(
            1
        )

    return value


# ============================================================
# DURATION
# ============================================================

def parse_duration(
    text
):

    if not text:
        return 0

    text = str(
        text
    ).strip()

    hours = 0
    minutes = 0
    seconds = 0

    # --------------------------------------------------------
    # Hours
    # --------------------------------------------------------

    match = re.search(
        r"(\d+)\s*ساعة",
        text
    )

    if match:

        hours = int(
            match.group(1)
        )

    # --------------------------------------------------------
    # Minutes
    # --------------------------------------------------------

    match = re.search(
        r"(\d+)\s*دقيقة",
        text
    )

    if match:

        minutes = int(
            match.group(1)
        )

    # --------------------------------------------------------
    # Seconds
    # --------------------------------------------------------

    match = re.search(
        r"(\d+)\s*ثانية",
        text
    )

    if match:

        seconds = int(
            match.group(1)
        )

    # --------------------------------------------------------
    # Return seconds
    # --------------------------------------------------------

    return (
        hours * 3600
        + minutes * 60
        + seconds
    )


# ============================================================
# SERVER NAME
# ============================================================

def get_server_name(
    url
):

    try:

        hostname = urlparse(
            url
        ).hostname

        if not hostname:
            return ""

        hostname = hostname.lower()

        parts = hostname.split(
            "."
        )

        ignored = {

            "www",
            "ww",
            "wwa",

            "ww1",
            "ww2",
            "ww3",
            "ww4",

            "hd",
            "hd1",
            "hd2",
            "hd3",

            "m"

        }

        parts = [
            part
            for part in parts
            if part not in ignored
        ]

        if len(parts) >= 2:

            return parts[-2]

        if parts:

            return parts[0]

        return ""

    except Exception:

        return ""


# ============================================================
# GET SERIES URL
# ============================================================

def get_series_url(
    info
):

    possible_keys = [

        "url",

        "series_url",

        "anime_url",

        "link"

    ]

    for key in possible_keys:

        value = info.get(
            key
        )

        if not isinstance(
            value,
            str
        ):

            continue

        value = clean_url(
            value
        )

        if value.startswith(
            "http"
        ):

            return value

    return ""


# ============================================================
# EXTRACT EPISODE DATA
# ============================================================

def extract_processed_episode_data(
    html
):

    patterns = [

        r"var\s+processedEpisodeData\s*=\s*'([^']+)'",

        r'var\s+processedEpisodeData\s*=\s*"([^"]+)"'

    ]

    for pattern in patterns:

        match = re.search(
            pattern,
            html
        )

        if match:

            return match.group(
                1
            )

    raise ValueError(
        "processedEpisodeData not found"
    )


# ============================================================
# DECODE EPISODE DATA
# ============================================================

def decode_episode_data(
    encoded_data
):

    try:

        part1, part2 = (
            encoded_data.split(
                ".",
                1
            )
        )

    except ValueError as error:

        raise ValueError(
            "Invalid processedEpisodeData format"
        ) from error

    data = base64.b64decode(
        part1
    )

    key = base64.b64decode(
        part2
    )

    if not key:

        raise ValueError(
            "XOR key is empty"
        )

    decoded = bytes(

        value ^
        key[
            index % len(key)
        ]

        for index, value
        in enumerate(data)

    )

    decoded_text = decoded.decode(
        "utf-8"
    )

    return json.loads(
        decoded_text
    )


# ============================================================
# EXTRACT EPISODES
# ============================================================

def extract_episodes(
    html
):

    encoded_data = (
        extract_processed_episode_data(
            html
        )
    )

    data = decode_episode_data(
        encoded_data
    )

    if not isinstance(
        data,
        list
    ):

        return []

    return data


# ============================================================
# EXTRACT SERVER REGISTRIES
# ============================================================

def extract_server_registries(
    html
):

    resource_match = re.search(
        r'var\s+_zT\s*=\s*"([^"]+)"',
        html
    )

    config_match = re.search(
        r'var\s+_zV\s*=\s*"([^"]+)"',
        html
    )

    if not resource_match:

        raise ValueError(
            "_zT not found"
        )

    if not config_match:

        raise ValueError(
            "_zV not found"
        )

    resource_registry = json.loads(

        base64.b64decode(
            resource_match.group(1)
        ).decode(
            "utf-8"
        )

    )

    config_registry = json.loads(

        base64.b64decode(
            config_match.group(1)
        ).decode(
            "utf-8"
        )

    )

    return (
        resource_registry,
        config_registry
    )


# ============================================================
# PARAMETER OFFSET
# ============================================================

def get_parameter_offset(
    config_settings
):

    index_key = base64.b64decode(
        config_settings["k"]
    ).decode(
        "utf-8"
    )

    index = int(
        index_key
    )

    return config_settings["d"][
        index
    ]


# ============================================================
# DECODE SERVER RESOURCE
# ============================================================

def decode_server_resource(
    resource_data,
    config_settings
):

    resource_data = (
        resource_data[::-1]
    )

    resource_data = re.sub(
        r"[^A-Za-z0-9+/=]",
        "",
        resource_data
    )

    param_offset = (
        get_parameter_offset(
            config_settings
        )
    )

    decoded_resource = (
        base64.b64decode(
            resource_data
        ).decode(
            "utf-8"
        )
    )

    if param_offset:

        decoded_resource = (
            decoded_resource[
                :-param_offset
            ]
        )

    return decoded_resource


# ============================================================
# EXTRACT SERVERS
# ============================================================

def extract_servers(
    html
):

    (
        resource_registry,
        config_registry
    ) = extract_server_registries(
        html
    )

    servers = {}

    for server_id, resource_data in enumerate(
        resource_registry
    ):

        if server_id >= len(
            config_registry
        ):

            continue

        config_settings = (
            config_registry[
                server_id
            ]
        )

        try:

            url = decode_server_resource(
                resource_data,
                config_settings
            )

        except Exception:

            continue

        if not url:
            continue

        url = clean_url(
            url
        )

        if not url.startswith(
            "http"
        ):

            continue

        server_name = get_server_name(
            url
        )

        if not server_name:
            continue

        # ----------------------------------------------------
        # Duplicate name
        # ----------------------------------------------------

        if server_name in servers:

            original_name = (
                server_name
            )

            suffix = 2

            while server_name in servers:

                server_name = (
                    f"{original_name}_{suffix}"
                )

                suffix += 1

        servers[
            server_name
        ] = url

    return servers


# ============================================================
# EXTRACT ANIME INFORMATION
# ============================================================

def extract_anime_info(
    html
):

    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    details = soup.select_one(
        ".anime-details"
    )

    if not details:

        return {}

    # --------------------------------------------------------
    # Title
    # --------------------------------------------------------

    title_element = (
        details.select_one(
            ".anime-details-title"
        )
    )

    title = ""

    if title_element:

        title = clean_text(
            title_element.get_text(
                " ",
                strip=True
            )
        )

    # --------------------------------------------------------
    # Description
    # --------------------------------------------------------

    story_element = (
        details.select_one(
            ".anime-story"
        )
    )

    description = ""

    if story_element:

        description = clean_text(
            story_element.get_text(
                " ",
                strip=True
            )
        )

    anime = {

        "title":
            title,

        "description":
            description,

        "status":
            "",

        "episodes_count":
            0,

        "duration":
            0

    }

    # --------------------------------------------------------
    # Information rows
    # --------------------------------------------------------

    for info in details.select(
        ".anime-info"
    ):

        span = info.select_one(
            "span"
        )

        if not span:
            continue

        label = clean_text(
            span.get_text(
                " ",
                strip=True
            )
        )

        value = clean_text(
            info.get_text(
                " ",
                strip=True
            )
        )

        value = value.replace(
            label,
            "",
            1
        ).strip()

        # ----------------------------------------------------
        # Status
        # ----------------------------------------------------

        if label == "حالة الأنمي:":

            anime[
                "status"
            ] = value

        # ----------------------------------------------------
        # Episode count
        # ----------------------------------------------------

        elif label == "عدد الحلقات:":

            numbers = re.findall(
                r"\d+",
                value
            )

            if numbers:

                anime[
                    "episodes_count"
                ] = int(
                    numbers[0]
                )

        # ----------------------------------------------------
        # Duration
        # ----------------------------------------------------

        elif label == "مدة الحلقة:":

            anime[
                "duration"
            ] = parse_duration(
                value
            )

    return anime


# ============================================================
# EXTRACT THUMBNAIL
# ============================================================

def extract_thumbnail(
    html,
    fallback=""
):

    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    # --------------------------------------------------------
    # OG image
    # --------------------------------------------------------

    og_image = soup.select_one(
        'meta[property="og:image"]'
    )

    if og_image:

        value = og_image.get(
            "content"
        )

        if value:

            return clean_url(
                value
            )

    # --------------------------------------------------------
    # Normal image
    # --------------------------------------------------------

    selectors = [

        ".episode img",

        ".anime-card-poster img",

        ".anime-details img",

        "article img"

    ]

    for selector in selectors:

        image = soup.select_one(
            selector
        )

        if image:

            value = (

                image.get(
                    "src"
                )

                or image.get(
                    "data-src"
                )

            )

            if value:

                return clean_url(
                    value
                )

    return clean_url(
        fallback
    )


# ============================================================
# EXTRACT EPISODE DESCRIPTION
# ============================================================

def extract_episode_description(
    html
):

    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    selectors = [

        ".episode-description",

        ".episode-story",

        ".episode-details",

        ".post-content",

        ".entry-content",

        ".description"

    ]

    for selector in selectors:

        element = soup.select_one(
            selector
        )

        if not element:
            continue

        text = clean_text(
            element.get_text(
                " ",
                strip=True
            )
        )

        if text:

            return text

    return ""


# ============================================================
# EXTRACT EPISODE TITLE
# ============================================================

def extract_episode_title(
    html,
    fallback
):

    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    selectors = [

        ".episodes-card-title h3",

        ".episode-title",

        "h1",

        "title"

    ]

    for selector in selectors:

        element = soup.select_one(
            selector
        )

        if not element:
            continue

        text = clean_text(
            element.get_text(
                " ",
                strip=True
            )
        )

        if text:

            return text

    return fallback


# ============================================================
# BUILD EPISODE
# ============================================================

def build_episode(
    session,
    episode_data,
    anime_title,
    duration,
    cover,
    existing_episode=None
):

    number = episode_data.get(
        "number"
    )

    episode_url = clean_url(
        episode_data.get(
            "url"
        )
    )

    if number is None:
        return None

    if not episode_url:
        return None

    try:

        number = int(
            number
        )

    except Exception:

        return None

    old = (
        existing_episode
        if existing_episode
        else {}
    )

    thread_name = (
        threading.current_thread().name
    )

    safe_print(
        f"[{thread_name}] "
        f"Episode {number}: "
        f"{episode_url}"
    )

    # ========================================================
    # GET EPISODE PAGE
    # ========================================================

    episode_html = None

    try:

        response = get_page(
            session,
            episode_url
        )

        if response.status_code == 200:

            episode_html = (
                response.text
            )

        else:

            safe_print(
                f"[{thread_name}] "
                f"Episode {number} "
                f"HTTP {response.status_code}"
            )

    except Exception as error:

        safe_print(
            f"[{thread_name}] "
            f"Episode {number} "
            f"ERROR: {error}"
        )

    # ========================================================
    # SERVERS
    # ========================================================

    servers = {}

    if episode_html:

        try:

            servers = extract_servers(
                episode_html
            )

        except Exception as error:

            safe_print(
                f"[{thread_name}] "
                f"Episode {number} "
                f"Servers ERROR: "
                f"{error}"
            )

    # ========================================================
    # TITLE
    # ========================================================

    default_title = (
        f"مسلسل {anime_title} "
        f"الحلقة {number}"
    )

    title = old.get(
        "title"
    ) or default_title

    if episode_html:

        extracted_title = (
            extract_episode_title(
                episode_html,
                ""
            )
        )

        if extracted_title:

            title = extracted_title

    # ========================================================
    # DURATION
    # ========================================================

    episode_duration = old.get(
        "duration"
    )

    if (
        episode_duration is None
        or episode_duration == ""
        or episode_duration == 0
    ):

        episode_duration = duration

    if isinstance(
        episode_duration,
        str
    ):

        episode_duration = (
            parse_duration(
                episode_duration
            )
        )

    # ========================================================
    # THUMBNAIL
    # ========================================================

    thumbnail = clean_url(
        old.get(
            "thumbnail",
            ""
        )
    )

    if (
        not thumbnail
        and episode_html
    ):

        thumbnail = extract_thumbnail(
            episode_html,
            cover
        )

    if not thumbnail:

        thumbnail = clean_url(
            cover
        )

    # ========================================================
    # DESCRIPTION
    # ========================================================

    description = old.get(
        "description",
        ""
    )

    if (
        not description
        and episode_html
    ):

        description = (
            extract_episode_description(
                episode_html
            )
        )

    # ========================================================
    # KEEP OLD SERVERS IF REQUEST FAILED
    # ========================================================

    if not servers:

        servers = old.get(
            "servers",
            {}
        )

    # ========================================================
    # CLEAN SERVER URLS
    # ========================================================

    servers = {

        str(name):
            clean_url(url)

        for name, url
        in servers.items()

        if clean_url(url)

    }

    # ========================================================
    # TAGS
    # ========================================================

    tags = [

        (
            f"{anime_title} "
            f"الحلقة {number}"
        ),

        (
            f"{anime_title} "
            f"اون لاين"
        )

    ]

    # ========================================================
    # FINAL EPISODE
    # ========================================================

    return {

        "title":
            title,

        "episode":
            number,

        "url":
            episode_url,

        "duration":
            episode_duration,

        "thumbnail":
            thumbnail,

        "servers":
            servers,

        "description":
            description,

        "tags":
            tags

    }


# ============================================================
# MOVE ANIME TO FINISHED
# ============================================================

def move_to_finished(
    info_path
):

    # --------------------------------------------------------
    # Anime folder
    # --------------------------------------------------------

    anime_dir = (
        info_path.parent
    )

    # --------------------------------------------------------
    # Relative path from output
    #
    # Example:
    #
    # output/Movie/Anime 1
    #
    # becomes:
    #
    # Movie/Anime 1
    # --------------------------------------------------------

    relative_path = (
        anime_dir.relative_to(
            OUTPUT_DIR
        )
    )

    # --------------------------------------------------------
    # Destination
    # --------------------------------------------------------

    destination = (
        FINISHED_DIR
        / relative_path
    )

    # --------------------------------------------------------
    # Create parent
    # --------------------------------------------------------

    destination.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Already exists
    # --------------------------------------------------------

    if destination.exists():

        safe_print(
            "[MOVE SKIPPED]"
        )

        safe_print(
            "Already exists:",
            destination
        )

        return False

    # --------------------------------------------------------
    # Move complete folder
    # --------------------------------------------------------

    shutil.move(
        str(anime_dir),
        str(destination)
    )

    safe_print(
        "[MOVED]"
    )

    safe_print(
        "FROM:",
        anime_dir
    )

    safe_print(
        "TO:",
        destination
    )

    return True


# ============================================================
# PROCESS INFO.JSON
# ============================================================

def process_info(
    info_path
):

    thread_name = (
        threading.current_thread().name
    )

    safe_print()
    safe_print(
        "=" * 100
    )

    safe_print(
        f"[{thread_name}] "
        f"PROCESSING"
    )

    safe_print(
        info_path
    )

    safe_print(
        "=" * 100
    )

    # ========================================================
    # READ JSON
    # ========================================================

    try:

        with open(
            info_path,
            "r",
            encoding="utf-8"
        ) as file:

            info = json.load(
                file
            )

    except Exception as error:

        safe_print(
            f"[{thread_name}] "
            f"JSON ERROR: {error}"
        )

        return False

    # ========================================================
    # GET SERIES URL
    # ========================================================

    series_url = get_series_url(
        info
    )

    if not series_url:

        safe_print(
            f"[{thread_name}] "
            f"NO SERIES URL"
        )

        safe_print(
            "Add 'url' to info.json:"
        )

        safe_print(
            info_path
        )

        return False

    safe_print(
        f"[{thread_name}] "
        f"Series URL:"
    )

    safe_print(
        series_url
    )

    # ========================================================
    # CREATE SESSION
    # ========================================================

    session = create_session()

    try:

        # ====================================================
        # GET SERIES
        # ====================================================

        try:

            response = get_page(
                session,
                series_url
            )

        except Exception as error:

            safe_print(
                f"[{thread_name}] "
                f"SERIES ERROR: "
                f"{error}"
            )

            return False

        if response.status_code != 200:

            safe_print(
                f"[{thread_name}] "
                f"SERIES HTTP "
                f"{response.status_code}"
            )

            return False

        html = response.text

        # ====================================================
        # EXISTING DATA
        # ====================================================

        anime_title = clean_text(
            info.get(
                "title",
                info_path.parent.name
            )
        )

        cover = clean_url(
            info.get(
                "cover",
                ""
            )
        )

        banner = clean_url(
            info.get(
                "banner",
                ""
            )
        )

        genres = info.get(
            "genres",
            []
        )

        if not isinstance(
            genres,
            list
        ):

            genres = []

        # ====================================================
        # ANIME INFO
        # ====================================================

        try:

            anime_info = (
                extract_anime_info(
                    html
                )
            )

        except Exception as error:

            safe_print(
                f"[{thread_name}] "
                f"ANIME INFO ERROR: "
                f"{error}"
            )

            anime_info = {}

        # ====================================================
        # TITLE
        # ========================================================

        source_title = anime_info.get(
            "title"
        )

        if source_title:

            anime_title = source_title

        # ====================================================
        # DESCRIPTION
        # ====================================================

        source_description = (
            anime_info.get(
                "description",
                ""
            )
        )

        # ====================================================
        # STATUS
        # ====================================================

        source_status = (
            anime_info.get(
                "status",
                ""
            )
        )

        # ====================================================
        # DURATION
        # ====================================================

        duration = anime_info.get(
            "duration",
            0
        )

        safe_print(
            f"[{thread_name}] "
            f"Duration: "
            f"{duration} seconds"
        )

        # ====================================================
        # EPISODES
        # ====================================================

        try:

            episodes_data = (
                extract_episodes(
                    html
                )
            )

        except Exception as error:

            safe_print(
                f"[{thread_name}] "
                f"EPISODES ERROR: "
                f"{error}"
            )

            return False

        safe_print(
            f"[{thread_name}] "
            f"Found episodes: "
            f"{len(episodes_data)}"
        )

        # ====================================================
        # EXISTING EPISODES
        # ====================================================

        existing_map = {}

        existing_episodes = info.get(
            "episodes",
            []
        )

        if not isinstance(
            existing_episodes,
            list
        ):

            existing_episodes = []

        for episode in existing_episodes:

            if not isinstance(
                episode,
                dict
            ):

                continue

            number = episode.get(
                "episode"
            )

            if number is None:
                continue

            try:

                number = int(
                    number
                )

            except Exception:

                continue

            existing_map[
                number
            ] = episode

        # ====================================================
        # BUILD EPISODES
        # ====================================================

        final_episodes = []

        for episode_data in episodes_data:

            episode = build_episode(

                session=session,

                episode_data=episode_data,

                anime_title=anime_title,

                duration=duration,

                cover=cover,

                existing_episode=(
                    existing_map.get(
                        episode_data.get(
                            "number"
                        )
                    )
                )

            )

            if episode:

                final_episodes.append(
                    episode
                )

        # ====================================================
        # KEEP OLD EPISODES
        # ========================================================

        found_numbers = {

            episode[
                "episode"
            ]

            for episode
            in final_episodes

        }

        for number, old_episode in existing_map.items():

            if number not in found_numbers:

                safe_print(
                    f"[{thread_name}] "
                    f"KEEP OLD EPISODE "
                    f"{number}"
                )

                final_episodes.append(
                    old_episode
                )

        # ====================================================
        # SORT
        # ====================================================

        final_episodes.sort(

            key=lambda episode:
            episode.get(
                "episode",
                0
            )

        )

        # ====================================================
        # BACKUP
        # ====================================================

        if SAVE_BACKUP:

            backup_path = (
                info_path.with_suffix(
                    ".backup.json"
                )
            )

            try:

                with open(
                    backup_path,
                    "w",
                    encoding="utf-8"
                ) as file:

                    json.dump(
                        info,
                        file,
                        ensure_ascii=False,
                        indent=4
                    )

            except Exception as error:

                safe_print(
                    f"[{thread_name}] "
                    f"BACKUP ERROR: "
                    f"{error}"
                )

        # ====================================================
        # FINAL JSON
        # ====================================================

        final_data = {

            "title":
                anime_title,

            "url":
                series_url,

            "description":
                (
                    info.get(
                        "description"
                    )
                    or source_description
                    or ""
                ),

            "status":
                (
                    info.get(
                        "status"
                    )
                    or source_status
                    or "En Emisión"
                ),

            "cover":
                cover,

            "banner":
                banner,

            "genres":
                genres,

            "episodes_count":
                len(
                    final_episodes
                ),

            "episodes":
                final_episodes

        }

        # ====================================================
        # SAVE JSON
        # ====================================================

        with open(
            info_path,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                final_data,
                file,
                ensure_ascii=False,
                indent=4
            )

            file.write(
                "\n"
            )

        safe_print()
        safe_print(
            f"[{thread_name}] "
            f"SAVED"
        )

        safe_print(
            info_path
        )

        safe_print(
            f"[{thread_name}] "
            f"Episodes: "
            f"{len(final_episodes)}"
        )

        # ====================================================
        # MOVE TO FINISHED
        # ====================================================

        try:

            moved = move_to_finished(
                info_path
            )

            if moved:

                safe_print(
                    f"[{thread_name}] "
                    f"FINISHED SUCCESSFULLY"
                )

            else:

                safe_print(
                    f"[{thread_name}] "
                    f"FINISHED MOVE SKIPPED"
                )

        except Exception as error:

            safe_print(
                f"[{thread_name}] "
                f"MOVE ERROR:"
            )

            safe_print(
                error
            )

            # ------------------------------------------------
            # JSON was successfully processed,
            # but move failed.
            # ------------------------------------------------

            return False

        return True

    finally:

        session.close()


# ============================================================
# THREAD WORKER
# ============================================================

def process_one_info(
    info_path
):

    thread_name = (
        threading.current_thread().name
    )

    try:

        safe_print()
        safe_print(
            f"[{thread_name}] "
            f"START"
        )

        safe_print(
            info_path.parent
        )

        result = process_info(
            info_path
        )

        if result:

            safe_print(
                f"[{thread_name}] "
                f"SUCCESS"
            )

            safe_print(
                info_path.parent.name
            )

        else:

            safe_print(
                f"[{thread_name}] "
                f"FAILED"
            )

            safe_print(
                info_path.parent.name
            )

        return result

    except Exception as error:

        safe_print()
        safe_print(
            f"[{thread_name}] "
            f"FATAL ERROR"
        )

        safe_print(
            info_path
        )

        safe_print(
            error
        )

        return False


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print(
        "=" * 100
    )

    print(
        "WITANIME INFO.JSON UPDATER"
    )

    print(
        "Input:",
        OUTPUT_DIR
    )

    print(
        "Finished:",
        FINISHED_DIR
    )

    print(
        "Threads:",
        MAX_WORKERS
    )

    print(
        "Timeout:",
        TIMEOUT
    )

    print(
        "=" * 100
    )

    # ========================================================
    # CHECK OUTPUT
    # ========================================================

    if not OUTPUT_DIR.exists():

        print()
        print(
            "[ERROR] Output directory does not exist:"
        )

        print(
            OUTPUT_DIR
        )

        return

    # ========================================================
    # CREATE FINISHED
    # ========================================================

    FINISHED_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # ========================================================
    # FIND INFO.JSON
    # ========================================================

    info_files = sorted(
        OUTPUT_DIR.rglob(
            "info.json"
        )
    )

    print()
    print(
        "Found info.json:",
        len(info_files)
    )

    if not info_files:

        print()
        print(
            "No info.json found."
        )

        return

    # ========================================================
    # PRINT FILES
    # ========================================================

    print()
    print(
        "=" * 100
    )

    print(
        "FILES TO PROCESS"
    )

    print(
        "=" * 100
    )

    for index, info_path in enumerate(
        info_files,
        start=1
    ):

        print(
            f"[{index}]",
            info_path
        )

    # ========================================================
    # START THREADS
    # ========================================================

    print()
    print(
        "=" * 100
    )

    print(
        "STARTING THREADS"
    )

    print(
        "=" * 100
    )

    success = 0
    failed = 0

    # ========================================================
    # THREAD POOL
    # ========================================================

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS,
        thread_name_prefix="WITANIME"
    ) as executor:

        futures = {

            executor.submit(
                process_one_info,
                info_path
            ):
                info_path

            for info_path
            in info_files

        }

        # ====================================================
        # RESULTS
        # ====================================================

        for index, future in enumerate(
            as_completed(futures),
            start=1
        ):

            info_path = futures[
                future
            ]

            try:

                result = future.result()

                if result:

                    success += 1

                else:

                    failed += 1

            except Exception as error:

                failed += 1

                safe_print()
                safe_print(
                    "[THREAD ERROR]"
                )

                safe_print(
                    info_path
                )

                safe_print(
                    error
                )

            safe_print()
            safe_print(
                f"PROGRESS: "
                f"{index}/"
                f"{len(info_files)}"
            )

    # ========================================================
    # SUMMARY
    # ========================================================

    print()
    print(
        "=" * 100
    )

    print(
        "DONE"
    )

    print(
        "=" * 100
    )

    print(
        "TOTAL:",
        len(info_files)
    )

    print(
        "SUCCESS:",
        success
    )

    print(
        "FAILED:",
        failed
    )

    print(
        "THREADS:",
        MAX_WORKERS
    )

    print(
        "FINISHED:",
        FINISHED_DIR
    )

    print(
        "=" * 100
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()