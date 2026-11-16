#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
Unified Smart Series Downloader – Dynamic Adaptive Parallel Engine
================================================================================
A modular, resilient, and fully automated series extraction and parallel
download management system.

Features:
- Central Global Queue: all seasons & series queued together with Fair Scheduling
- Round-Robin season distribution: worker 1 gets S1, worker 2 gets S2, etc.
- Continuous Dynamic Scheduler: workers immediately pull next task upon completion
- Adaptive Concurrency Controller: monitors speed, error rates, CPU/RAM, scaling workers
- Deadlock-Free FFmpeg process management with background stderr reader
- Intelligent Retry Manager with exponential backoff and jitter
- JSON-only state management (Strictly zero database engines) with atomic writes & auto-backups
- Rich live terminal dashboard with real-time stats and adaptive controller telemetry
- Graceful shutdown handling (Ctrl+C) with temp cleanup and state preservation
================================================================================
"""

# ==============================================================================
# 1. IMPORTS
# ==============================================================================

import argparse
import collections
import datetime
import json
import logging
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

# System monitoring
try:
    import psutil
except ImportError:
    psutil = None

# Network and HTML Parsing
try:
    from bs4 import BeautifulSoup
    from curl_cffi import requests as cffi_requests
    from jsbeautifier.unpackers import packer
except ImportError as exc:
    print(
        f"[FATAL] Missing required core package: {exc}\n"
        f"Please install dependencies with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)

# Terminal Dashboard and Formatting
try:
    from rich.align import Align
    from rich.console import Console, Group
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )
    from rich.table import Table
    from rich.text import Text
except ImportError:
    Console = None
    Live = None


# ==============================================================================
# 2. CONFIGURATION
# ==============================================================================

DEFAULT_CONFIG: Dict[str, Any] = {
    "paths": {
        "output_dir": "Downloaded_Series",
        "data_dir": "data",
        "logs_dir": "logs",
        "series_file": "series.txt",
    },
    "downloads": {
        "min_workers": 2,
        "initial_workers": 5,
        "max_workers": 15,
        "adaptive_enabled": True,
        "scale_up_threshold": 0.8,
        "scale_down_threshold": 0.3,
        "monitoring_interval": 5,
        "max_retries": 3,
        "retry_backoff_factor": 2.0,
        "retry_jitter": 1.0,
        "timeout": 25,
        "ffmpeg_timeout": 10800,  # 3 hours max per episode
        "resume": True,
        "validate_videos": True,
        "min_disk_free_mb": 1000,
    },
    "extractor": {
        "request_timeout": 30,
        "impersonate": "chrome",
    },
    "servers": {
        "enabled": ["1vid", "vidspeed"],
        "priority": ["1vid", "vidspeed"],
    },
    "network": {
        "cookies": {
            "PHPSESSID": "YOUR_PHPSESSID",
            "watched_video_list": "MTQ4MDU5",
            "pm_elastic_player": "normal",
        },
        "headers": {
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
        },
    },
    "monitoring": {
        "enabled": True,
        "dashboard": True,
        "refresh_rate_hz": 4,
    },
    "state": {
        "auto_backup": True,
        "backup_interval": 10,
        "max_backups": 10,
    },
}


class Config:
    """Manages application settings loaded from config.json with fallback defaults."""

    def __init__(self, config_path: Optional[Path] = None, base_dir: Optional[Path] = None):
        self.base_dir = (
            base_dir if base_dir else Path(__file__).resolve().parent
        )
        self.config_path = (
            config_path if config_path else self.base_dir / "config.json"
        )
        self.data: Dict[str, Any] = self._load_and_merge()

    def _load_and_merge(self) -> Dict[str, Any]:
        loaded = {}
        if self.config_path.is_file():
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
            except Exception as e:
                print(f"[!] Warning: Could not read {self.config_path} ({e}). Using defaults.")

        return self._deep_merge(DEFAULT_CONFIG, loaded)

    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(base)
        for k, v in override.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = self._deep_merge(result[k], v)
            else:
                result[k] = v
        return result

    def get_path(self, key: str) -> Path:
        raw = self.data["paths"].get(key, "")
        p = Path(raw)
        return p if p.is_absolute() else (self.base_dir / p).resolve()

    @property
    def output_dir(self) -> Path:
        return self.get_path("output_dir")

    @property
    def data_dir(self) -> Path:
        return self.get_path("data_dir")

    @property
    def logs_dir(self) -> Path:
        return self.get_path("logs_dir")

    @property
    def series_file(self) -> Path:
        return self.get_path("series_file")

    @property
    def min_workers(self) -> int:
        return int(self.data["downloads"].get("min_workers", 2))

    @property
    def initial_workers(self) -> int:
        return int(self.data["downloads"].get("initial_workers", 5))

    @property
    def max_workers(self) -> int:
        return int(self.data["downloads"].get("max_workers", 15))

    @property
    def adaptive_enabled(self) -> bool:
        return bool(self.data["downloads"].get("adaptive_enabled", True))

    @property
    def monitoring_interval(self) -> int:
        return int(self.data["downloads"].get("monitoring_interval", 5))

    @property
    def max_retries(self) -> int:
        return int(self.data["downloads"].get("max_retries", 3))

    @property
    def backoff_factor(self) -> float:
        return float(self.data["downloads"].get("retry_backoff_factor", 2.0))

    @property
    def retry_jitter(self) -> float:
        return float(self.data["downloads"].get("retry_jitter", 1.0))

    @property
    def request_timeout(self) -> int:
        return int(self.data["extractor"].get("request_timeout", 30))

    @property
    def ffmpeg_timeout(self) -> int:
        return int(self.data["downloads"].get("ffmpeg_timeout", 10800))

    @property
    def resume_enabled(self) -> bool:
        return bool(self.data["downloads"].get("resume", True))

    @property
    def validate_videos(self) -> bool:
        return bool(self.data["downloads"].get("validate_videos", True))

    @property
    def min_disk_free_mb(self) -> int:
        return int(self.data["downloads"].get("min_disk_free_mb", 1000))

    @property
    def cookies(self) -> Dict[str, str]:
        return dict(self.data["network"].get("cookies", {}))

    @property
    def headers(self) -> Dict[str, str]:
        return dict(self.data["network"].get("headers", {}))

    @property
    def impersonate(self) -> str:
        return str(self.data["extractor"].get("impersonate", "chrome"))

    @property
    def enabled_servers(self) -> List[str]:
        return list(self.data["servers"].get("enabled", ["1vid"]))

    @property
    def server_priority(self) -> List[str]:
        return list(self.data["servers"].get("priority", ["1vid"]))

    @property
    def dashboard_enabled(self) -> bool:
        return bool(self.data["monitoring"].get("dashboard", True)) and Console is not None

    @property
    def auto_backup(self) -> bool:
        return bool(self.data["state"].get("auto_backup", True))

    @property
    def max_backups(self) -> int:
        return int(self.data["state"].get("max_backups", 10))


# ==============================================================================
# 3. DATA MODELS
# ==============================================================================

class EpisodeStatus:
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class SeriesStatus:
    PENDING = "pending"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    FAILED = "failed"


@dataclass
class EpisodeItem:
    episode_number: int
    episode_title: str
    url: str
    status: str = EpisodeStatus.PENDING
    output_path: Optional[str] = None
    file_size: int = 0
    retry_count: int = 0
    last_error: Optional[str] = None
    updated_at: Optional[str] = None
    download_start_time: Optional[str] = None
    completion_time: Optional[str] = None
    average_speed: Optional[str] = None
    current_attempt: int = 0


@dataclass
class SeasonItem:
    series_name: str
    season_number: int
    season_name: str
    total_episodes: int
    episodes: List[Dict[str, Any]] = field(default_factory=list)
    filepath: Optional[str] = None


@dataclass
class SeriesItem:
    series_url: str
    series_name: str = "Unknown Series"
    status: str = SeriesStatus.PENDING
    last_extracted: Optional[str] = None
    total_seasons: int = 0
    total_episodes: int = 0
    error: Optional[str] = None
    seasons: Dict[int, SeasonItem] = field(default_factory=dict)


@dataclass
class DownloadTask:
    series_name: str
    series_url: str
    season_number: int
    episode_number: int
    episode_title: str
    episode_url: str
    output_dir: Path
    output_file: Path
    temp_file: Path
    target_server: Optional[str] = None
    retry_count: int = 0
    last_error: Optional[str] = None
    next_retry_at: float = 0.0  # Timestamp when eligible for retry
    in_flight: bool = False
    start_time: Optional[float] = None
    completion_time: Optional[float] = None
    current_speed_bytes: float = 0.0
    downloaded_bytes: int = 0
    total_bytes: int = 0

    @property
    def unique_key(self) -> Tuple[str, int, int]:
        return (self.series_name, self.season_number, self.episode_number)

    @property
    def group_key(self) -> Tuple[str, int]:
        return (self.series_name, self.season_number)


@dataclass
class WorkerProgress:
    worker_id: int
    series_name: str = ""
    season_number: int = 0
    episode_number: int = 0
    episode_title: str = ""
    server_name: str = ""
    status: str = "Idle"
    percentage: float = 0.0
    downloaded_bytes: int = 0
    total_bytes: int = 0
    speed: str = "0 KB/s"
    speed_bytes: float = 0.0
    elapsed_sec: float = 0.0
    eta_sec: Optional[float] = None


# ==============================================================================
# 4. JSON STATE MANAGER (SAFE & ATOMIC OPERATIONS)
# ==============================================================================

class SafeJsonManager:
    """
    Guarantees zero data loss with thread-safe atomic writes,
    automated backups, and transparent recovery from corrupted JSON files.
    Strictly uses standard JSON files (No SQLite or DB engine).
    """

    def __init__(self, data_dir: Path, auto_backup: bool = True, max_backups: int = 10):
        self.data_dir = data_dir
        self.backups_dir = self.data_dir / "backups"
        self.auto_backup = auto_backup
        self.max_backups = max_backups
        self.lock = threading.RLock()

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.backups_dir.mkdir(parents=True, exist_ok=True)

        self.extraction_state_file = self.data_dir / "extraction_state.json"
        self.download_state_file = self.data_dir / "download_state.json"

    def atomic_write(self, target_file: Path, data: Any) -> None:
        """Writes to a temporary file in the same directory, flushes, fsyncs, and replaces atomically."""
        with self.lock:
            target_file.parent.mkdir(parents=True, exist_ok=True)
            temp_file = target_file.with_name(
                f"{target_file.stem}.tmp.{os.getpid()}.{threading.get_ident()}{target_file.suffix}"
            )
            try:
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())

                if self.auto_backup and target_file.exists():
                    self._create_backup(target_file)

                temp_file.replace(target_file)
            except Exception:
                if temp_file.exists():
                    temp_file.unlink(missing_ok=True)
                raise

    def _create_backup(self, filepath: Path) -> None:
        """Maintains timestamped rotating backups of a state file."""
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{filepath.stem}_{timestamp}{filepath.suffix}"
            backup_path = self.backups_dir / backup_name
            shutil.copy2(filepath, backup_path)

            pattern = f"{filepath.stem}_*{filepath.suffix}"
            backups = sorted(
                self.backups_dir.glob(pattern),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for old_backup in backups[self.max_backups :]:
                old_backup.unlink(missing_ok=True)
        except Exception:
            pass

    def load_json_safe(self, filepath: Path, default_factory: Callable[[], Any]) -> Any:
        """Loads JSON; if corrupt or missing, automatically recovers from the latest valid backup."""
        with self.lock:
            if not filepath.exists() or filepath.stat().st_size == 0:
                recovered = self._try_recover_from_backup(filepath)
                if recovered is not None:
                    return recovered
                val = default_factory()
                self.atomic_write(filepath, val)
                return val

            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                recovered = self._try_recover_from_backup(filepath)
                if recovered is not None:
                    return recovered
                val = default_factory()
                self.atomic_write(filepath, val)
                return val

    def _try_recover_from_backup(self, filepath: Path) -> Optional[Any]:
        pattern = f"{filepath.stem}_*{filepath.suffix}"
        backups = sorted(
            self.backups_dir.glob(pattern),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for backup_file in backups:
            try:
                with open(backup_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.atomic_write(filepath, data)
                return data
            except Exception:
                continue
        return None

    # --- Extraction State Helpers ---

    def get_extraction_state(self) -> Dict[str, Any]:
        return self.load_json_safe(
            self.extraction_state_file,
            lambda: {"series": {}, "last_updated": None},
        )

    def update_series_extraction(
        self,
        series_url: str,
        series_name: str,
        status: str,
        total_seasons: int = 0,
        total_episodes: int = 0,
        error: Optional[str] = None,
        season_stats: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.lock:
            state = self.get_extraction_state()
            series_dict = state.setdefault("series", {})
            entry = series_dict.setdefault(series_url, {})
            entry.update(
                {
                    "series_name": series_name,
                    "series_url": series_url,
                    "status": status,
                    "last_extracted": datetime.datetime.now().isoformat(),
                    "total_seasons": total_seasons,
                    "total_episodes": total_episodes,
                    "error": error,
                }
            )
            if season_stats is not None:
                entry["seasons"] = season_stats
            state["last_updated"] = datetime.datetime.now().isoformat()
            self.atomic_write(self.extraction_state_file, state)

    # --- Download State Helpers ---

    def get_download_state(self) -> Dict[str, Any]:
        return self.load_json_safe(
            self.download_state_file,
            lambda: {"series": {}, "last_updated": None},
        )

    def recover_stale_in_progress(self) -> int:
        """
        Scans download_state.json on startup:
        Resets any episode that was 'downloading' when interrupted back to 'pending'.
        """
        with self.lock:
            state = self.get_download_state()
            recovered = 0
            for s_data in state.get("series", {}).values():
                for se_data in s_data.get("seasons", {}).values():
                    for ep_data in se_data.get("episodes", {}).values():
                        if ep_data.get("status") == EpisodeStatus.DOWNLOADING:
                            ep_data["status"] = EpisodeStatus.PENDING
                            ep_data["last_error"] = "Interrupted prior to completion"
                            recovered += 1
            if recovered > 0:
                self.atomic_write(self.download_state_file, state)
            return recovered

    def update_episode_download(
        self,
        series_name: str,
        series_url: str,
        season_number: int,
        episode_number: int,
        episode_title: str,
        episode_url: str,
        status: str,
        output_path: Optional[str] = None,
        file_size: int = 0,
        error: Optional[str] = None,
        increment_retry: bool = False,
        start_time: Optional[str] = None,
        completion_time: Optional[str] = None,
        avg_speed: Optional[str] = None,
        server_name: Optional[str] = None,
    ) -> None:
        with self.lock:
            state = self.get_download_state()
            s_entry = state.setdefault("series", {}).setdefault(
                series_name,
                {
                    "series_name": series_name,
                    "series_url": series_url,
                    "status": "in_progress",
                    "seasons": {},
                },
            )
            season_str = str(season_number)
            ep_str = str(episode_number)
            season_entry = s_entry["seasons"].setdefault(season_str, {"episodes": {}})
            ep_entry = season_entry["episodes"].setdefault(
                ep_str,
                {
                    "episode_title": episode_title,
                    "episode_url": episode_url,
                    "status": EpisodeStatus.PENDING,
                    "output_path": output_path,
                    "file_size": file_size,
                    "retry_count": 0,
                    "last_error": None,
                },
            )

            ep_entry["status"] = status
            if output_path:
                ep_entry["output_path"] = output_path
            if file_size > 0:
                ep_entry["file_size"] = file_size
            if error is not None:
                ep_entry["last_error"] = str(error)
            elif status == EpisodeStatus.COMPLETED:
                ep_entry["last_error"] = None

            if increment_retry:
                ep_entry["retry_count"] = ep_entry.get("retry_count", 0) + 1

            ep_entry["current_attempt"] = ep_entry.get("retry_count", 0)

            if start_time:
                ep_entry["download_start_time"] = start_time
            if completion_time:
                ep_entry["completion_time"] = completion_time
            if avg_speed:
                ep_entry["average_speed"] = avg_speed
            if server_name:
                ep_entry["server"] = server_name

            ep_entry["last_updated"] = datetime.datetime.now().isoformat()
            state["last_updated"] = datetime.datetime.now().isoformat()
            self.atomic_write(self.download_state_file, state)

    # --- Season JSON File Handling ---

    def save_season_json(
        self,
        season_dir: Path,
        series_name: str,
        season_number: int,
        episodes: List[Dict[str, Any]],
    ) -> Path:
        season_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{series_name} - Season {season_number}.json"
        filepath = season_dir / filename

        data = {
            "series_name": series_name,
            "season_number": season_number,
            "season_name": f"{series_name} الموسم {season_number}",
            "total_episodes": len(episodes),
            "episodes": [
                {
                    "episode_number": ep.get("episode_number", ep.get("number")),
                    "episode_title": ep.get("episode_title")
                    or f"{ep.get('title', '').strip()} الموسم {season_number}",
                    "url": ep["url"],
                }
                for ep in episodes
            ],
        }

        self.atomic_write(filepath, data)
        return filepath


# ==============================================================================
# 5. LOGGER & DIAGNOSTICS
# ==============================================================================

class AppLogger:
    """Unified application logger with clean handler management and file output."""

    def __init__(self, logs_dir: Path):
        self.logs_dir = logs_dir
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.logs_dir / "downloader.log"

        self.logger = logging.getLogger("SeriesDownloader")
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

        if self.logger.hasHandlers():
            for h in list(self.logger.handlers):
                try:
                    h.close()
                except Exception:
                    pass
            self.logger.handlers.clear()

        file_handler = logging.FileHandler(self.log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def debug(self, message: str) -> None:
        self.logger.debug(message)


# ==============================================================================
# 6. HTTP CLIENT (CURL_CFFI THREAD-SAFE ENGINE)
# ==============================================================================

class HttpClient:
    """Robust HTTP client wrapping curl_cffi with impersonate="chrome"."""

    def __init__(self, config: Config, logger: AppLogger):
        self.config = config
        self.logger = logger

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> Any:
        timeout = kwargs.pop("timeout", self.config.request_timeout)
        headers = dict(self.config.headers)
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        cookies = dict(self.config.cookies)
        if "cookies" in kwargs:
            cookies.update(kwargs.pop("cookies"))

        max_attempts = 3
        backoff = 1.5

        for attempt in range(1, max_attempts + 1):
            try:
                response = cffi_requests.get(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    impersonate=self.config.impersonate,
                    timeout=timeout,
                    **kwargs,
                )
                response.raise_for_status()
                return response
            except Exception as e:
                if attempt == max_attempts:
                    self.logger.error(f"[HTTP] Failed request to {url}: {e}")
                    raise
                self.logger.warning(
                    f"[HTTP] Attempt {attempt} failed for {url}: {e}. Retrying in {backoff:.1f}s..."
                )
                time.sleep(backoff)
                backoff *= 2.0


# ==============================================================================
# 7. SERIES EXTRACTOR (FROM EXTRACT_ALLLINKS.PY)
# ==============================================================================

class SeriesExtractor:
    """Preserves and enhances extraction logic from extract_alllinks.py."""

    def __init__(self, http_client: HttpClient, logger: AppLogger):
        self.http = http_client
        self.logger = logger

    @staticmethod
    def clean_filename(name: str) -> str:
        name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", str(name))
        name = re.sub(r"\s+", " ", name)
        return name.strip(" ._")

    def get_series_name(self, soup: BeautifulSoup) -> str:
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        title = re.split(r"\s*[-|]\s*", title)[0].strip()
        if title:
            return self.clean_filename(title)

        h1 = soup.find("h1")
        if h1:
            return self.clean_filename(h1.get_text(" ", strip=True))

        return "Unknown Series"

    @staticmethod
    def get_episode_number(title: str, fallback: int) -> int:
        match = re.search(r"الحلقة\s*(\d+)", title)
        if match:
            return int(match.group(1))

        match = re.search(r"\bEpisode\s*(\d+)\b", title, re.I)
        if match:
            return int(match.group(1))

        return fallback

    def extract_seasons(
        self, soup: BeautifulSoup, base_url: str
    ) -> Dict[int, List[Dict[str, Any]]]:
        result: Dict[int, List[Dict[str, Any]]] = {}
        season_containers = soup.select(".SeasonsEpisodesMains .tabcontent")

        for index, container in enumerate(season_containers, 1):
            season_id = container.get("id", "")
            match = re.search(r"Season(\d+)", season_id, re.I)
            season_number = int(match.group(1)) if match else index

            episodes = []
            seen: Set[str] = set()

            for link in container.select('a[href*="watch.php"]'):
                href = link.get("href", "").strip()
                if not href:
                    continue

                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)

                if parsed.path.endswith("/watch.php"):
                    parsed = parsed._replace(
                        path=parsed.path.replace("watch.php", "see.php")
                    )

                full_url = parsed.geturl()
                video_id = parse_qs(parsed.query).get("vid", [None])[0]

                if not video_id or video_id in seen:
                    continue

                seen.add(video_id)

                title = (
                    link.get("title") or link.get_text(" ", strip=True)
                ).strip()

                if not title:
                    image = link.find("img")
                    if image:
                        title = image.get("alt", "")

                episode_number = self.get_episode_number(
                    title, len(episodes) + 1
                )

                episodes.append(
                    {
                        "number": episode_number,
                        "title": title or f"Episode {episode_number}",
                        "url": full_url,
                    }
                )

            episodes.sort(key=lambda x: x["number"])
            if episodes:
                result[season_number] = episodes

        return result

    def fetch_and_extract(self, series_url: str) -> Tuple[str, Dict[int, List[Dict[str, Any]]]]:
        self.logger.info(f"[EXTRACT] Downloading series page: {series_url}")
        response = self.http.get(series_url)
        soup = BeautifulSoup(response.text, "html.parser")

        series_name = self.get_series_name(soup)
        seasons = self.extract_seasons(soup, series_url)
        return series_name, seasons


# ==============================================================================
# 8. MODULAR SERVER EXTRACTORS (STRATEGY PATTERN & 1VID LOGIC)
# ==============================================================================

class BaseServerExtractor(ABC):
    """Abstract Strategy interface for video host stream extractors."""

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def extract_m3u8(self, embed_url: str, http_client: HttpClient) -> List[str]:
        pass


class OneVidExtractor(BaseServerExtractor):
    """Preserves exact 1vid logic with Dean Edwards unpacker and m3u8 regex."""

    aliases = {"1vid", "1vid.xyz", "onevid"}

    @property
    def name(self) -> str:
        return "1vid"

    def extract_m3u8(self, embed_url: str, http_client: HttpClient) -> List[str]:
        response = http_client.get(embed_url)
        soup = BeautifulSoup(response.text, "html.parser")
        scripts = [s.get_text() or "" for s in soup.find_all("script")]
        source = soup.decode()

        packer.beginstr = packer.endstr = ""

        for script in scripts:
            if "eval(function" in script:
                try:
                    source += packer.unpack(script)
                except Exception:
                    continue

        pattern = r"""https?://[^"'\\\s<>]+?\.m3u8(?:\?[^"'\\\s<>]*)?"""
        matches = re.findall(pattern, source, re.IGNORECASE)
        return list(dict.fromkeys(matches))


class VidSpeedExtractor(BaseServerExtractor):
    """
    VidSpeed server extractor.
    Extracts M3U8 links using Dean Edwards packer unpacking and regex.
    """

    aliases = {"vidspeed", "vidspeeds", "vidspeed.org", "vidspeeds.org"}

    @property
    def name(self) -> str:
        return "vidspeed"

    def extract_m3u8(self, embed_url: str, http_client: HttpClient) -> List[str]:
        response = http_client.get(embed_url)
        soup = BeautifulSoup(response.text, "html.parser")
        scripts = [s.get_text() or "" for s in soup.find_all("script")]
        source = soup.decode()

        packer.beginstr = packer.endstr = ""

        for script in scripts:
            if "eval(function" in script:
                try:
                    source += packer.unpack(script)
                except Exception:
                    continue

        pattern = r"""https?://[^"'\\\s<>]+?\.m3u8(?:\?[^"'\\\s<>]*)?"""
        matches = re.findall(pattern, source, re.IGNORECASE)
        return list(dict.fromkeys(matches))


# ==============================================================================
# 9. SERVER REGISTRY (DYNAMIC EXTENSIBILITY)
# ==============================================================================

class ServerRegistry:
    """Registry maintaining active and prioritized server extractors."""

    def __init__(self):
        self._extractors: Dict[str, BaseServerExtractor] = {}

    def register(self, extractor: BaseServerExtractor) -> None:
        self._extractors[extractor.name.casefold().strip()] = extractor

    def get(self, name: str) -> Optional[BaseServerExtractor]:
        return self.get_extractor(name)

    def get_extractor(self, server_name: str) -> Optional[BaseServerExtractor]:
        name_clean = server_name.casefold().strip()
        # Direct key match
        if name_clean in self._extractors:
            return self._extractors[name_clean]
        # Match by aliases or substring
        for ext in self._extractors.values():
            aliases = getattr(ext, "aliases", set())
            if any(a == name_clean or a in name_clean or name_clean in a for a in aliases):
                return ext
            if ext.name.casefold() in name_clean or name_clean in ext.name.casefold():
                return ext
        return None

    def get_supported_names(self) -> List[str]:
        return list(self._extractors.keys())

    def prioritize_servers(
        self,
        available_servers: Dict[str, str],
        enabled_servers: List[str],
        priority_order: List[str],
    ) -> List[Tuple[str, str, BaseServerExtractor]]:
        """
        Sorts available servers by configured priority and enabled list.
        Returns list of (server_name, embed_url, extractor).
        """
        enabled_set = {s.casefold().strip() for s in enabled_servers}
        priority_map = {
            s.casefold().strip(): idx for idx, s in enumerate(priority_order)
        }

        selected = []
        for s_name, embed_url in available_servers.items():
            extractor = self.get_extractor(s_name)
            if not extractor:
                continue

            ext_name = extractor.name.casefold()
            s_name_clean = s_name.casefold().strip()

            is_enabled = (
                ext_name in enabled_set
                or s_name_clean in enabled_set
                or any(a in enabled_set for a in getattr(extractor, "aliases", set()))
            )

            if is_enabled:
                prio = 9999
                for prio_name, idx in priority_map.items():
                    if (
                        prio_name == s_name_clean
                        or prio_name == ext_name
                        or prio_name in getattr(extractor, "aliases", set())
                    ):
                        prio = min(prio, idx)
                selected.append((prio, s_name, embed_url, extractor))

        selected.sort(key=lambda x: x[0])
        return [(item[1], item[2], item[3]) for item in selected]


class EpisodeServerParser:
    """Parses available servers for an episode from see.php."""

    def __init__(self, http_client: HttpClient, logger: AppLogger):
        self.http = http_client
        self.logger = logger

    def get_servers(self, episode_url: str) -> Dict[str, str]:
        parsed = urlparse(episode_url)
        video_id = parse_qs(parsed.query).get("vid", [None])[0]
        if not video_id:
            raise ValueError(f"Invalid episode URL missing vid parameter: {episode_url}")

        base = f"{parsed.scheme}://{parsed.netloc}"
        endpoint = f"{base}/see.php" if parsed.netloc else "https://yam.ahwaktv.net/see.php"

        response = self.http.get(endpoint, params={"vid": video_id})
        soup = BeautifulSoup(response.text, "html.parser")
        servers: Dict[str, str] = {}

        for li in soup.select("ul.WatchList li[data-embed-url]"):
            name = li.get_text(strip=True)
            embed_url = li.get("data-embed-url")
            if name and embed_url:
                servers[name] = embed_url

        return servers


# ==============================================================================
# 10. VIDEO VALIDATOR (FFPROBE STREAM & INTEGRITY CHECK)
# ==============================================================================

class VideoValidator:
    """Ensures video files are valid, not empty, and contain playable streams."""

    @staticmethod
    def is_valid_video(path: Path) -> bool:
        if not path.is_file() or path.stat().st_size < 1024 * 100:  # < 100 KB
            return False

        if not shutil.which("ffprobe"):
            return True

        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration:stream=codec_type",
                    "-of",
                    "json",
                    str(path),
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            if result.returncode != 0:
                return False

            data = json.loads(result.stdout or "{}")
            duration = float(data.get("format", {}).get("duration", 0) or 0)
            has_video = any(
                stream.get("codec_type") == "video"
                for stream in data.get("streams", [])
            )
            return has_video and duration > 0
        except Exception:
            return False

    @staticmethod
    def get_stream_duration(url: str) -> Optional[float]:
        if not shutil.which("ffprobe"):
            return None
        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    url,
                ],
                capture_output=True,
                text=True,
                timeout=25,
            )
            if result.returncode == 0:
                return float(result.stdout.strip())
        except Exception:
            pass
        return None


# ==============================================================================
# 11. FFMPEG DOWNLOADER (DEADLOCK-FREE PROCESS ENGINE)
# ==============================================================================

class FFmpegDownloader:
    """
    Handles video stream downloading via FFmpeg with deadlock-free pipe reading,
    concurrent stderr draining, and clean process termination.
    """

    def __init__(self, config: Config, logger: AppLogger):
        self.config = config
        self.logger = logger
        self._active_processes: Set[subprocess.Popen] = set()
        self._lock = threading.Lock()

    def register_process(self, proc: subprocess.Popen) -> None:
        with self._lock:
            self._active_processes.add(proc)

    def unregister_process(self, proc: subprocess.Popen) -> None:
        with self._lock:
            self._active_processes.discard(proc)

    def kill_all_processes(self) -> None:
        with self._lock:
            for proc in list(self._active_processes):
                try:
                    if proc.poll() is None:
                        proc.terminate()
                        try:
                            proc.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                except Exception:
                    pass
            self._active_processes.clear()

    def download(
        self,
        m3u8_url: str,
        output_file: Path,
        temp_file: Path,
        progress_callback: Optional[Callable[[float, int, str, float, float], None]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> bool:
        if not shutil.which("ffmpeg"):
            raise RuntimeError("ffmpeg binary not found in system PATH")

        temp_file.unlink(missing_ok=True)
        duration = VideoValidator.get_stream_duration(m3u8_url)

        user_agent = self.config.headers.get(
            "user-agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-user_agent",
            user_agent,
            "-rw_timeout",
            "20000000",
            "-reconnect",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_delay_max",
            "5",
            "-progress",
            "pipe:1",
            "-stats_period",
            "1",
            "-nostats",
            "-i",
            m3u8_url,
            "-map",
            "0:v?",
            "-map",
            "0:a?",
            "-c",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            "-movflags",
            "+faststart",
            str(temp_file),
        ]

        proc = None
        start_time = time.time()
        last_out_time = 0.0
        total_size = 0
        speed_str = "0x"
        last_speed_bytes = 0.0
        last_metric_time = start_time
        last_metric_size = 0

        # Background thread captures stderr to prevent 64KB pipe buffer deadlock
        stderr_buffer: Deque[str] = collections.deque(maxlen=100)

        def drain_stderr(pipe):
            try:
                for line in iter(pipe.readline, ""):
                    if line:
                        stderr_buffer.append(line.strip())
            except Exception:
                pass
            finally:
                pipe.close()

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )
            self.register_process(proc)

            stderr_thread = threading.Thread(
                target=drain_stderr, args=(proc.stderr,), daemon=True
            )
            stderr_thread.start()

            for line in proc.stdout:
                if cancel_event and cancel_event.is_set():
                    raise KeyboardInterrupt("Cancelled by user")

                # Check process total timeout
                elapsed_now = time.time() - start_time
                if elapsed_now > self.config.ffmpeg_timeout:
                    raise subprocess.TimeoutExpired(cmd, self.config.ffmpeg_timeout)

                key, _, value = line.strip().partition("=")
                if key == "out_time_us":
                    try:
                        current_time = int(value) / 1_000_000
                        if current_time > last_out_time:
                            last_out_time = current_time
                    except ValueError:
                        pass
                elif key == "total_size":
                    try:
                        total_size = int(value)
                    except ValueError:
                        pass
                elif key == "speed":
                    speed_str = value
                elif key == "progress":
                    pct = 0.0
                    if duration and duration > 0:
                        pct = min(100.0, (last_out_time / duration) * 100.0)
                    now = time.time()
                    time_diff = now - last_metric_time
                    if time_diff >= 1.0:
                        size_diff = total_size - last_metric_size
                        last_speed_bytes = max(0.0, size_diff / time_diff)
                        last_metric_time = now
                        last_metric_size = total_size

                    if progress_callback:
                        progress_callback(
                            pct, total_size, speed_str, elapsed_now, last_speed_bytes
                        )

            retcode = proc.wait(timeout=10)
            stderr_text = "\n".join(stderr_buffer)

            if retcode != 0:
                raise RuntimeError(
                    (stderr_text or f"FFmpeg failed with exit code {retcode}").strip()[-800:]
                )

            if self.config.validate_videos and not VideoValidator.is_valid_video(temp_file):
                raise RuntimeError("Downloaded video failed integrity check (corrupt or unplayable)")

            temp_file.replace(output_file)
            return True

        except subprocess.TimeoutExpired:
            if proc and proc.poll() is None:
                proc.kill()
            raise RuntimeError("FFmpeg download timed out")
        finally:
            if proc:
                self.unregister_process(proc)
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
            temp_file.unlink(missing_ok=True)


# ==============================================================================
# 12. INTELLIGENT RETRY MANAGER (EXPONENTIAL BACKOFF & JITTER)
# ==============================================================================

class RetryManager:
    """Manages retry evaluations, error classification, exponential backoff, and jitter."""

    def __init__(
        self,
        max_retries: int,
        backoff_factor: float,
        jitter: float,
        logger: AppLogger,
    ):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.logger = logger

    def is_transient_error(self, error: Exception) -> bool:
        err_msg = str(error).lower()
        non_transient_indicators = [
            "404 not found",
            "disk full",
            "no space left on device",
            "permission denied",
        ]
        if any(ind in err_msg for ind in non_transient_indicators):
            return False
        return True

    def calculate_delay(self, retry_count: int) -> float:
        base = self.backoff_factor ** retry_count
        jitter_val = random.uniform(0.1, self.jitter)
        return base + jitter_val

    def get_retry_delay(self, task: DownloadTask, error: Exception) -> Optional[float]:
        """Returns delay in seconds if task should be retried, or None if retries exhausted."""
        if task.retry_count >= self.max_retries:
            return None

        if not self.is_transient_error(error):
            self.logger.warning(
                f"[RETRY] Non-transient error detected for {task.episode_title}: {error}. Not retrying."
            )
            return None

        delay = self.calculate_delay(task.retry_count)
        self.logger.warning(
            f"[RETRY] {task.episode_title}: Attempt {task.retry_count + 1}/{self.max_retries} failed ({error}). "
            f"Scheduling retry in {delay:.1f}s..."
        )
        return delay


# ==============================================================================
# 13. CENTRAL GLOBAL FAIR QUEUE (ALL SEASONS & ROUND-ROBIN SCHEDULING)
# ==============================================================================

class GlobalFairQueue:
    """
    Unified central queue maintaining all episodes across all series and seasons.
    Implements Round-Robin Fair Scheduling across season groups so all seasons
    download in parallel without any season waiting for another to finish.
    """

    def __init__(self, logger: AppLogger):
        self.logger = logger
        self._lock = threading.RLock()
        self._groups: Dict[Tuple[str, int], Deque[DownloadTask]] = collections.defaultdict(
            collections.deque
        )
        self._group_keys: Deque[Tuple[str, int]] = collections.deque()
        self._in_flight: Set[Tuple[str, int, int]] = set()
        self._delayed_retries: List[DownloadTask] = []
        self._all_tasks: Dict[Tuple[str, int, int], DownloadTask] = {}
        self._completed_keys: Set[Tuple[str, int, int]] = set()
        self._failed_keys: Set[Tuple[str, int, int]] = set()

    def add_task(self, task: DownloadTask) -> bool:
        with self._lock:
            key = task.unique_key
            if key in self._all_tasks or key in self._completed_keys:
                return False

            self._all_tasks[key] = task
            group = task.group_key
            if group not in self._group_keys:
                self._group_keys.append(group)
            self._groups[group].append(task)
            return True

    def get_next_task(self) -> Optional[DownloadTask]:
        """
        Pulls the next eligible task using Fair Round-Robin across seasons.
        Immediately activates delayed retries whose timer has elapsed.
        """
        with self._lock:
            now = time.time()

            # Check and reactivate matured delayed retries
            ready_retries = []
            remaining_delayed = []
            for d_task in self._delayed_retries:
                if now >= d_task.next_retry_at:
                    ready_retries.append(d_task)
                else:
                    remaining_delayed.append(d_task)
            self._delayed_retries = remaining_delayed

            for r_task in ready_retries:
                group = r_task.group_key
                if group not in self._group_keys:
                    self._group_keys.append(group)
                self._groups[group].appendleft(r_task)

            # Round-robin cycle across active groups
            num_groups = len(self._group_keys)
            for _ in range(num_groups):
                group = self._group_keys[0]
                self._group_keys.rotate(-1)  # Rotate group to back for fair interleaving

                tasks_deque = self._groups.get(group)
                if not tasks_deque:
                    continue

                for _ in range(len(tasks_deque)):
                    candidate = tasks_deque.popleft()
                    if candidate.unique_key in self._in_flight:
                        tasks_deque.append(candidate)
                        continue

                    # Claim task
                    candidate.in_flight = True
                    candidate.start_time = time.time()
                    self._in_flight.add(candidate.unique_key)
                    return candidate

            return None

    def mark_completed(self, task: DownloadTask) -> None:
        with self._lock:
            self._in_flight.discard(task.unique_key)
            self._completed_keys.add(task.unique_key)

    def mark_failed(self, task: DownloadTask, error: str, retry_delay: Optional[float]) -> None:
        with self._lock:
            self._in_flight.discard(task.unique_key)
            task.in_flight = False
            task.last_error = error

            if retry_delay is not None:
                task.retry_count += 1
                task.next_retry_at = time.time() + retry_delay
                self._delayed_retries.append(task)
            else:
                self._failed_keys.add(task.unique_key)

    def has_work(self) -> bool:
        """Returns True if any tasks are in-flight, pending, or waiting for retry delay."""
        with self._lock:
            if self._in_flight:
                return True
            if self._delayed_retries:
                return True
            for q in self._groups.values():
                if q:
                    return True
            return False

    def stats(self) -> Dict[str, int]:
        with self._lock:
            pending_count = sum(len(q) for q in self._groups.values())
            return {
                "total": len(self._all_tasks),
                "in_flight": len(self._in_flight),
                "delayed_retry": len(self._delayed_retries),
                "pending": pending_count,
                "completed": len(self._completed_keys),
                "failed": len(self._failed_keys),
            }


# ==============================================================================
# 14. ADAPTIVE CONCURRENCY CONTROLLER
# ==============================================================================

class AdaptiveConcurrencyController:
    """
    Monitors speed, start times, HTTP/FFmpeg errors, CPU, and RAM metrics.
    Dynamically adjusts concurrency (current_workers) between min_workers and max_workers.
    """

    def __init__(
        self,
        min_workers: int = 2,
        initial_workers: int = 5,
        max_workers: int = 15,
        monitoring_interval: int = 5,
        enabled: bool = True,
        logger: Optional[AppLogger] = None,
    ):
        self.min_workers = max(1, min_workers)
        self.max_workers = max(self.min_workers, max_workers)
        self.current_workers = min(self.max_workers, max(self.min_workers, initial_workers))
        self.monitoring_interval = monitoring_interval
        self.enabled = enabled
        self.logger = logger
        self.status_reason = "Initialized"

        self._lock = threading.Lock()
        self._last_eval_time = time.time()
        self._recent_errors = 0
        self._recent_success = 0
        self._recent_speeds: Deque[float] = collections.deque(maxlen=6)

    def record_error(self) -> None:
        with self._lock:
            self._recent_errors += 1

    def record_success(self) -> None:
        with self._lock:
            self._recent_success += 1

    def evaluate(self, total_speed_bytes: float, active_workers_count: int) -> int:
        if not self.enabled:
            return self.current_workers

        with self._lock:
            now = time.time()
            if now - self._last_eval_time < self.monitoring_interval:
                return self.current_workers

            self._last_eval_time = now
            self._recent_speeds.append(total_speed_bytes)

            cpu_pct = psutil.cpu_percent() if psutil else 30.0
            ram_pct = psutil.virtual_memory().percent if psutil else 40.0

            # Rule 1: High system load throttling
            if cpu_pct > 85.0 or ram_pct > 90.0:
                if self.current_workers > self.min_workers:
                    self.current_workers -= 1
                    self.status_reason = f"Throttling (CPU: {cpu_pct:.0f}%, RAM: {ram_pct:.0f}%)"
                    if self.logger:
                        self.logger.warning(
                            f"[ADAPTIVE] {self.status_reason}. Concurrency -> {self.current_workers}"
                        )
                return self.current_workers

            # Rule 2: High error rate (HTTP 429, resets, timeouts)
            if self._recent_errors >= 2:
                if self.current_workers > self.min_workers:
                    self.current_workers = max(self.min_workers, self.current_workers - 1)
                    self.status_reason = f"Reducing concurrency ({self._recent_errors} errors/throttles)"
                    if self.logger:
                        self.logger.warning(
                            f"[ADAPTIVE] {self.status_reason}. Concurrency -> {self.current_workers}"
                        )
                self._recent_errors = 0
                return self.current_workers

            # Rule 3: Speed stability & capacity to scale up
            if len(self._recent_speeds) >= 3 and self._recent_errors == 0:
                avg_speed = sum(self._recent_speeds) / len(self._recent_speeds)
                speed_mb = avg_speed / (1024 * 1024)

                # If workers are busy, speed is healthy, and system is calm
                if active_workers_count >= self.current_workers - 1 and cpu_pct < 75.0:
                    if self.current_workers < self.max_workers:
                        self.current_workers += 1
                        self.status_reason = f"Increasing concurrency (Speed stable {speed_mb:.1f} MB/s)"
                        if self.logger:
                            self.logger.info(
                                f"[ADAPTIVE] {self.status_reason}. Concurrency -> {self.current_workers}"
                            )
                    else:
                        self.status_reason = f"Optimal concurrency at max ({self.max_workers} workers)"
                else:
                    self.status_reason = f"Holding concurrency ({self.current_workers} workers, {speed_mb:.1f} MB/s)"

            self._recent_errors = 0
            self._recent_success = 0
            return self.current_workers


# ==============================================================================
# 15. LIVE DASHBOARD (RICH TERMINAL TELEMETRY)
# ==============================================================================

class LiveDashboard:
    """Renders real-time telemetry, adaptive controller status, and active downloads."""

    def __init__(self, max_workers: int, enabled: bool = True):
        self.enabled = enabled and Live is not None
        self.max_workers = max_workers
        self.console = Console() if self.enabled else None
        self.live: Optional[Live] = None
        self.lock = threading.Lock()

        # Telemetry
        self.total_series = 0
        self.extracted_series = 0
        self.total_seasons = 0
        self.total_episodes = 0
        self.completed_episodes = 0
        self.downloading_episodes = 0
        self.failed_episodes = 0
        self.skipped_episodes = 0
        self.pending_episodes = 0
        self.total_retries = 0
        self.start_time = time.time()
        self.disk_free_gb = 0.0

        # Adaptive Telemetry
        self.current_concurrency = 5
        self.adaptive_status = "Standby"
        self.total_speed_mb = 0.0
        self.cpu_pct = 0.0
        self.ram_pct = 0.0

        # Worker progress slots
        self.worker_slots: Dict[int, WorkerProgress] = {
            i: WorkerProgress(worker_id=i) for i in range(1, max_workers + 1)
        }

    def start(self) -> None:
        if not self.enabled or not self.console:
            return
        self.live = Live(
            self._generate_layout(),
            console=self.console,
            refresh_per_second=4,
            transient=False,
        )
        self.live.start()

    def stop(self) -> None:
        if self.live:
            try:
                self.live.stop()
            except Exception:
                pass
            self.live = None

    def update_stats(self, **kwargs) -> None:
        with self.lock:
            for k, v in kwargs.items():
                if hasattr(self, k):
                    setattr(self, k, v)
        if self.live:
            self.live.update(self._generate_layout())

    def update_worker(self, worker_id: int, **kwargs) -> None:
        with self.lock:
            if worker_id in self.worker_slots:
                wp = self.worker_slots[worker_id]
                for k, v in kwargs.items():
                    if hasattr(wp, k):
                        setattr(wp, k, v)
        if self.live:
            self.live.update(self._generate_layout())

    def clear_worker(self, worker_id: int) -> None:
        with self.lock:
            if worker_id in self.worker_slots:
                self.worker_slots[worker_id] = WorkerProgress(worker_id=worker_id, status="Idle")
        if self.live:
            self.live.update(self._generate_layout())

    def _generate_layout(self) -> Panel:
        with self.lock:
            elapsed = time.time() - self.start_time
            mins, secs = divmod(int(elapsed), 60)
            elapsed_str = f"{mins:02d}:{secs:02d}"

            # Calculate total speed from active workers
            total_bytes_per_sec = sum(
                wp.speed_bytes for wp in self.worker_slots.values() if wp.status != "Idle"
            )
            self.total_speed_mb = total_bytes_per_sec / (1024 * 1024)

            # Overview Grid
            overview = Table.grid(expand=True, padding=(0, 2))
            overview.add_column(style="cyan bold")
            overview.add_column(style="white")
            overview.add_column(style="cyan bold")
            overview.add_column(style="white")

            processed = self.completed_episodes + self.skipped_episodes
            overall_pct = (
                (processed / self.total_episodes * 100.0)
                if self.total_episodes > 0
                else 0.0
            )

            overview.add_row(
                "Series Extracted:",
                f"{self.extracted_series}/{self.total_series}",
                "Completed Episodes:",
                f"[green]{self.completed_episodes}[/green]",
            )
            overview.add_row(
                "Total Seasons:",
                f"{self.total_seasons}",
                "Skipped (Completed):",
                f"[dim]{self.skipped_episodes}[/dim]",
            )
            overview.add_row(
                "Total Episodes:",
                f"{self.total_episodes}",
                "Downloading / Pending:",
                f"[yellow]{self.downloading_episodes}[/yellow] / [blue]{self.pending_episodes}[/blue]",
            )
            overview.add_row(
                "Overall Progress:",
                f"[bold cyan]{overall_pct:5.1f}%[/bold cyan]",
                "Failed / Retries:",
                f"[red]{self.failed_episodes}[/red] / [magenta]{self.total_retries}[/magenta]",
            )

            # Adaptive Controller Banner
            ctrl_banner = Table.grid(expand=True, padding=(0, 2))
            ctrl_banner.add_column(style="bold yellow")
            ctrl_banner.add_column(style="white")
            ctrl_banner.add_row(
                f"Adaptive Workers: {self.current_concurrency}/{self.max_workers} | Speed: {self.total_speed_mb:.2f} MB/s",
                f"System: CPU {self.cpu_pct:.0f}% | RAM {self.ram_pct:.0f}% | Disk Free {self.disk_free_gb:.1f} GB",
            )
            ctrl_banner.add_row(
                "Adaptive Status:",
                f"[italic cyan]{self.adaptive_status}[/italic cyan]",
            )

            # Active Workers Table (Visible up to current_concurrency)
            active_table = Table(
                expand=True,
                box=None,
                header_style="bold magenta",
                padding=(0, 1),
            )
            active_table.add_column("#", width=3, justify="right")
            active_table.add_column("Series", width=16, no_wrap=True)
            active_table.add_column("S/Ep", width=7, justify="center")
            active_table.add_column("Progress", width=22)
            active_table.add_column("Speed", width=11, justify="right")
            active_table.add_column("Size", width=11, justify="right")
            active_table.add_column("Server", width=8, justify="center")
            active_table.add_column("Status", width=14)

            active_count = 0
            for w_id in range(1, self.current_concurrency + 1):
                wp = self.worker_slots[w_id]
                if wp.status != "Idle":
                    active_count += 1
                    bar_blocks = int(wp.percentage / 10.0)
                    prog_bar = f"[{'█' * bar_blocks}{'░' * (10 - bar_blocks)}] {wp.percentage:5.1f}%"
                    size_mb = wp.downloaded_bytes / (1024 * 1024)
                    size_str = f"{size_mb:5.1f} MB"

                    status_color = (
                        "green"
                        if "Done" in wp.status
                        else "red"
                        if "Fail" in wp.status
                        else "yellow"
                    )

                    s_name = (
                        wp.series_name
                        if len(wp.series_name) <= 16
                        else wp.series_name[:13] + "..."
                    )

                    active_table.add_row(
                        str(w_id),
                        s_name,
                        f"S{wp.season_number} E{wp.episode_number}",
                        prog_bar,
                        wp.speed,
                        size_str,
                        wp.server_name or "-",
                        f"[{status_color}]{wp.status}[/{status_color}]",
                    )
                else:
                    active_table.add_row(
                        str(w_id),
                        "-",
                        "-",
                        "-",
                        "-",
                        "-",
                        "-",
                        "[dim]Idle (Ready)[/dim]",
                    )

            footer_text = Text.from_markup(
                f"[bold cyan]Active Downloads:[/bold cyan] {active_count}/{self.current_concurrency}  |  "
                f"[bold cyan]Elapsed:[/bold cyan] {elapsed_str}  |  "
                f"[dim]Press Ctrl+C to safely pause and save state[/dim]"
            )

            group = Group(
                Panel(overview, title="[bold green]📊 Central Global Overview[/bold green]"),
                Panel(ctrl_banner, title="[bold yellow]⚡ Adaptive Concurrency Controller[/bold yellow]"),
                Panel(active_table, title="[bold blue]🚀 Active Workers (Fair Scheduled Across Seasons)[/bold blue]"),
                Align.center(footer_text),
            )

            return Panel(
                group,
                title="[bold yellow]Unified Smart Series Downloader[/bold yellow]",
                border_style="cyan",
            )


# ==============================================================================
# 16. CONTINUOUS DYNAMIC SCHEDULER & ORCHESTRATOR
# ==============================================================================

class UnifiedOrchestrator:
    """Coordinates execution across all phases with continuous scheduling."""

    def __init__(self, config: Config):
        self.config = config
        self.logger = AppLogger(self.config.logs_dir)
        self.state_mgr = SafeJsonManager(
            self.config.data_dir,
            auto_backup=self.config.auto_backup,
            max_backups=self.config.max_backups,
        )
        self.http_client = HttpClient(self.config, self.logger)
        self.series_extractor = SeriesExtractor(self.http_client, self.logger)
        self.server_parser = EpisodeServerParser(self.http_client, self.logger)
        self.server_registry = ServerRegistry()
        self.ffmpeg_downloader = FFmpegDownloader(self.config, self.logger)
        self.retry_manager = RetryManager(
            max_retries=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            jitter=self.config.retry_jitter,
            logger=self.logger,
        )
        self.global_queue = GlobalFairQueue(self.logger)
        self.adaptive_controller = AdaptiveConcurrencyController(
            min_workers=self.config.min_workers,
            initial_workers=self.config.initial_workers,
            max_workers=self.config.max_workers,
            monitoring_interval=self.config.monitoring_interval,
            enabled=self.config.adaptive_enabled,
            logger=self.logger,
        )
        self.dashboard = LiveDashboard(
            self.config.max_workers, enabled=self.config.dashboard_enabled
        )

        self._shutdown_requested = threading.Event()
        self._threads: List[threading.Thread] = []

        # Register server extractors
        self.server_registry.register(OneVidExtractor())
        self.server_registry.register(VidSpeedExtractor())

    def register_signal_handlers(self) -> None:
        def handler(sig, frame):
            print("\n[!] Ctrl+C received. Gracefully saving state and stopping...")
            self.shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def shutdown(self) -> None:
        self._shutdown_requested.set()
        self.ffmpeg_downloader.kill_all_processes()
        self.dashboard.stop()

    # --------------------------------------------------------------------------
    # PHASE 1: INITIALIZATION
    # --------------------------------------------------------------------------

    def phase_1_initialize(self) -> List[str]:
        self.logger.info("=" * 60)
        self.logger.info("PHASE 1: INITIALIZATION & ENVIRONMENT SYNC")
        self.logger.info("=" * 60)

        if not shutil.which("ffmpeg"):
            raise RuntimeError("ffmpeg binary is missing. Install ffmpeg to proceed.")
        if not shutil.which("ffprobe"):
            self.logger.warning("ffprobe not found. Video validation will rely on file sizes.")

        # Check disk space
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(self.config.output_dir)
        free_mb = usage.free / (1024 * 1024)
        self.dashboard.disk_free_gb = usage.free / (1024 * 1024 * 1024)

        if free_mb < self.config.min_disk_free_mb:
            raise RuntimeError(
                f"Insufficient disk space ({free_mb:.1f} MB free). Required minimum: {self.config.min_disk_free_mb} MB"
            )

        # Recover stale in-progress downloads
        recovered = self.state_mgr.recover_stale_in_progress()
        if recovered > 0:
            self.logger.info(f"[INIT] Recovered {recovered} previously interrupted downloads to 'pending'.")

        # Read series.txt
        series_file = self.config.series_file
        if not series_file.is_file():
            self.logger.warning(f"Series file {series_file} not found. Creating default template.")
            series_file.parent.mkdir(parents=True, exist_ok=True)
            series_file.write_text("# Add series URLs here, one per line\n", encoding="utf-8")
            return []

        urls: List[str] = []
        with open(series_file, "r", encoding="utf-8") as f:
            for line in f:
                cleaned = line.strip()
                if not cleaned or cleaned.startswith("#"):
                    continue
                if cleaned.startswith(("http://", "https://")):
                    if cleaned not in urls:
                        urls.append(cleaned)

        self.logger.info(f"[INIT] Loaded {len(urls)} unique series URLs from {series_file.name}")
        self.dashboard.total_series = len(urls)
        return urls

    # --------------------------------------------------------------------------
    # PHASE 2: SERIES EXTRACTION
    # --------------------------------------------------------------------------

    def phase_2_extract_series(self, series_urls: List[str], force: bool = False) -> List[Path]:
        self.logger.info("=" * 60)
        self.logger.info("PHASE 2: SERIES & SEASON METADATA EXTRACTION")
        self.logger.info("=" * 60)

        extraction_state = self.state_mgr.get_extraction_state()
        saved_season_files: List[Path] = []

        for idx, url in enumerate(series_urls, 1):
            if self._shutdown_requested.is_set():
                break

            cached = extraction_state.get("series", {}).get(url)
            if (
                not force
                and cached
                and cached.get("status") == SeriesStatus.EXTRACTED
                and cached.get("total_episodes", 0) > 0
            ):
                self.logger.info(
                    f"[{idx}/{len(series_urls)}] Skipping previously extracted series: {cached.get('series_name')} ({url})"
                )
                self.dashboard.extracted_series += 1
                s_name = cached.get("series_name")
                s_folder = self.config.output_dir / s_name
                if s_folder.is_dir():
                    saved_season_files.extend(list(s_folder.rglob("*.json")))
                continue

            self.logger.info(f"[{idx}/{len(series_urls)}] Extracting series from: {url}")
            try:
                series_name, seasons = self.series_extractor.fetch_and_extract(url)
                if not seasons:
                    self.logger.warning(f"No seasons or episodes discovered for: {url}")
                    self.state_mgr.update_series_extraction(
                        series_url=url,
                        series_name=series_name,
                        status=SeriesStatus.FAILED,
                        error="No seasons/episodes found",
                    )
                    continue

                total_eps = sum(len(eps) for eps in seasons.values())
                series_folder = self.config.output_dir / series_name
                series_folder.mkdir(parents=True, exist_ok=True)

                season_stats: Dict[str, Any] = {}
                for season_num, episodes in sorted(seasons.items()):
                    season_folder = (
                        series_folder / f"{series_name} الموسم {season_num}"
                    )
                    json_path = self.state_mgr.save_season_json(
                        season_dir=season_folder,
                        series_name=series_name,
                        season_number=season_num,
                        episodes=episodes,
                    )
                    saved_season_files.append(json_path)
                    season_stats[str(season_num)] = {
                        "episodes_count": len(episodes),
                        "json_file": str(json_path),
                    }

                self.state_mgr.update_series_extraction(
                    series_url=url,
                    series_name=series_name,
                    status=SeriesStatus.EXTRACTED,
                    total_seasons=len(seasons),
                    total_episodes=total_eps,
                    season_stats=season_stats,
                )

                self.logger.info(
                    f"[+] Successfully extracted {series_name}: {len(seasons)} seasons, {total_eps} episodes"
                )
                self.dashboard.extracted_series += 1

            except Exception as e:
                self.logger.error(f"[-] Extraction failed for {url}: {e}")
                self.state_mgr.update_series_extraction(
                    series_url=url,
                    series_name="Unknown",
                    status=SeriesStatus.FAILED,
                    error=str(e),
                )

        return saved_season_files

    # --------------------------------------------------------------------------
    # PHASE 3: COMPILE CENTRAL GLOBAL QUEUE (ALL SEASONS TOGETHER)
    # --------------------------------------------------------------------------

    def phase_3_compile_global_queue(self, season_json_files: List[Path]) -> None:
        self.logger.info("=" * 60)
        self.logger.info("PHASE 3: COMPILING CENTRAL GLOBAL QUEUE (ALL SEASONS UNIFIED)")
        self.logger.info("=" * 60)

        total_episodes_found = 0
        total_seasons = len(season_json_files)
        skipped_count = 0
        completed_count = 0

        for season_file in season_json_files:
            try:
                with open(season_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                self.logger.error(f"Cannot read season JSON {season_file}: {e}")
                continue

            series_name = data.get("series_name", "Unknown Series")
            season_number = data.get("season_number", 1)
            episodes = data.get("episodes", [])
            season_dir = season_file.parent

            for ep in episodes:
                url = ep.get("url")
                if not url:
                    continue

                ep_num = ep.get("episode_number") or SeriesExtractor.get_episode_number(
                    ep.get("episode_title", ""), 0
                )
                title = ep.get("episode_title") or ep.get("title") or f"Episode {ep_num}"
                safe_title = SeriesExtractor.clean_filename(title) + ".mp4"
                output_file = season_dir / safe_title

                # Check alternate filename with series name prefix
                if not output_file.is_file():
                    alt_file = season_dir / (
                        SeriesExtractor.clean_filename(f"{series_name} {title}") + ".mp4"
                    )
                    if alt_file.is_file():
                        output_file = alt_file

                temp_file = season_dir / f"{output_file.stem}.part.mp4"

                # Check if already completed and valid
                if output_file.is_file():
                    if VideoValidator.is_valid_video(output_file):
                        self.logger.debug(f"[SKIP] Completed & valid on disk: {output_file.name}")
                        skipped_count += 1
                        self.state_mgr.update_episode_download(
                            series_name=series_name,
                            series_url="",
                            season_number=season_number,
                            episode_number=ep_num,
                            episode_title=title,
                            episode_url=url,
                            status=EpisodeStatus.COMPLETED,
                            output_path=str(output_file),
                            file_size=output_file.stat().st_size,
                        )
                        continue
                    else:
                        self.logger.warning(
                            f"[REPAIR] File is corrupt; removing: {output_file.name}"
                        )
                        output_file.unlink(missing_ok=True)

                task = DownloadTask(
                    series_name=series_name,
                    series_url="",
                    season_number=season_number,
                    episode_number=ep_num,
                    episode_title=title,
                    episode_url=url,
                    output_dir=season_dir,
                    output_file=output_file,
                    temp_file=temp_file,
                )
                if self.global_queue.add_task(task):
                    total_episodes_found += 1

        self.dashboard.total_seasons = total_seasons
        self.dashboard.total_episodes = total_episodes_found + skipped_count
        self.dashboard.completed_episodes = completed_count
        self.dashboard.skipped_episodes = skipped_count
        self.dashboard.pending_episodes = total_episodes_found

        self.logger.info(
            f"[GLOBAL QUEUE] Queued {total_episodes_found} episodes across all seasons "
            f"({skipped_count} already completed/skipped)."
        )

    # --------------------------------------------------------------------------
    # PHASE 4: CONTINUOUS ADAPTIVE SCHEDULER
    # --------------------------------------------------------------------------

    def _download_task_worker(self, worker_id: int, task: DownloadTask) -> bool:
        start_ts = datetime.datetime.now().isoformat()
        self.dashboard.update_worker(
            worker_id=worker_id,
            series_name=task.series_name,
            season_number=task.season_number,
            episode_number=task.episode_number,
            episode_title=task.episode_title,
            status="Finding servers",
            percentage=0.0,
            downloaded_bytes=0,
            speed="0 KB/s",
            speed_bytes=0.0,
        )

        self.state_mgr.update_episode_download(
            series_name=task.series_name,
            series_url=task.series_url,
            season_number=task.season_number,
            episode_number=task.episode_number,
            episode_title=task.episode_title,
            episode_url=task.episode_url,
            status=EpisodeStatus.DOWNLOADING,
            start_time=start_ts,
        )

        errors: List[str] = []

        try:
            available_servers = self.server_parser.get_servers(task.episode_url)
            if not available_servers:
                raise RuntimeError("No streaming servers found on episode page")

            candidates = self.server_registry.prioritize_servers(
                available_servers=available_servers,
                enabled_servers=self.config.enabled_servers,
                priority_order=self.config.server_priority,
            )

            if not candidates:
                raise RuntimeError(
                    f"No enabled servers found among available ({list(available_servers.keys())})"
                )

            for candidate_idx, (s_name, embed_url, extractor) in enumerate(candidates, 1):
                if self._shutdown_requested.is_set():
                    return False

                if candidate_idx > 1:
                    prev_name = candidates[candidate_idx - 2][0]
                    self.logger.info(
                        f"[FALLBACK] Worker {worker_id}: Primary server '{prev_name}' failed or had no stream for "
                        f"'{task.episode_title}'. Falling back to server '{s_name}' ({candidate_idx}/{len(candidates)})..."
                    )

                self.dashboard.update_worker(
                    worker_id=worker_id,
                    server_name=s_name,
                    status=f"Extracting M3U8 ({s_name})",
                )

                try:
                    m3u8_links = extractor.extract_m3u8(embed_url, self.http_client)
                    if not m3u8_links:
                        errors.append(f"{s_name}: No M3U8 found")
                        continue

                    for m3u8_url in m3u8_links:
                        if self._shutdown_requested.is_set():
                            return False

                        def progress_cb(
                            pct: float, d_bytes: int, speed: str, elapsed: float, speed_b: float
                        ):
                            task.downloaded_bytes = d_bytes
                            task.current_speed_bytes = speed_b
                            self.dashboard.update_worker(
                                worker_id=worker_id,
                                percentage=pct,
                                downloaded_bytes=d_bytes,
                                speed=speed,
                                speed_bytes=speed_b,
                                elapsed_sec=elapsed,
                                status="Downloading",
                            )

                        self.ffmpeg_downloader.download(
                            m3u8_url=m3u8_url,
                            output_file=task.output_file,
                            temp_file=task.temp_file,
                            progress_callback=progress_cb,
                            cancel_event=self._shutdown_requested,
                        )

                        # Success!
                        f_size = task.output_file.stat().st_size
                        comp_ts = datetime.datetime.now().isoformat()
                        avg_speed_str = (
                            f"{f_size / (max(1.0, time.time() - task.start_time) * 1024 * 1024):.2f} MB/s"
                            if task.start_time
                            else "N/A"
                        )

                        self.state_mgr.update_episode_download(
                            series_name=task.series_name,
                            series_url=task.series_url,
                            season_number=task.season_number,
                            episode_number=task.episode_number,
                            episode_title=task.episode_title,
                            episode_url=task.episode_url,
                            status=EpisodeStatus.COMPLETED,
                            output_path=str(task.output_file),
                            file_size=f_size,
                            completion_time=comp_ts,
                            avg_speed=avg_speed_str,
                            server_name=s_name,
                        )

                        task.output_file.with_suffix(".failed.txt").unlink(missing_ok=True)
                        self.global_queue.mark_completed(task)
                        self.adaptive_controller.record_success()

                        self.dashboard.update_worker(
                            worker_id=worker_id,
                            percentage=100.0,
                            speed_bytes=0.0,
                            status="Done!",
                        )
                        return True

                except Exception as stream_err:
                    errors.append(f"{s_name}: {stream_err}")

            raise RuntimeError(" | ".join(errors) if errors else "Failed on all candidate servers")

        except Exception as ep_err:
            self.logger.error(f"[WORKER {worker_id}] Episode failed ({task.episode_title}): {ep_err}")
            self.adaptive_controller.record_error()

            retry_delay = self.retry_manager.get_retry_delay(task, ep_err)
            self.global_queue.mark_failed(task, str(ep_err), retry_delay)

            if retry_delay is not None:
                self.dashboard.total_retries += 1
                self.state_mgr.update_episode_download(
                    series_name=task.series_name,
                    series_url=task.series_url,
                    season_number=task.season_number,
                    episode_number=task.episode_number,
                    episode_title=task.episode_title,
                    episode_url=task.episode_url,
                    status=EpisodeStatus.PENDING,
                    error=str(ep_err),
                    increment_retry=True,
                )
                self.dashboard.update_worker(
                    worker_id=worker_id,
                    speed_bytes=0.0,
                    status="Retry Scheduled",
                )
            else:
                self.state_mgr.update_episode_download(
                    series_name=task.series_name,
                    series_url=task.series_url,
                    season_number=task.season_number,
                    episode_number=task.episode_number,
                    episode_title=task.episode_title,
                    episode_url=task.episode_url,
                    status=EpisodeStatus.FAILED,
                    error=str(ep_err),
                    increment_retry=True,
                )
                task.output_file.with_suffix(".failed.txt").write_text(
                    "\n".join(errors) if errors else str(ep_err), encoding="utf-8"
                )
                self.dashboard.update_worker(
                    worker_id=worker_id,
                    speed_bytes=0.0,
                    status="Failed (Max Retries)",
                )

            return False
        finally:
            time.sleep(0.3)
            self.dashboard.clear_worker(worker_id)

    def phase_4_continuous_scheduling(self) -> Dict[str, int]:
        self.logger.info("=" * 60)
        self.logger.info("PHASE 4: CONTINUOUS ADAPTIVE PARALLEL SCHEDULER")
        self.logger.info("=" * 60)

        if not self.global_queue.has_work():
            self.logger.info("No episodes in queue to process. All up to date!")
            return {"success": 0, "failed": 0, "skipped": self.dashboard.skipped_episodes}

        self.dashboard.start()

        counts = {"success": 0, "failed": 0}
        active_workers_count = 0
        active_count_lock = threading.Lock()

        def worker_loop(w_id: int):
            nonlocal active_workers_count
            while not self._shutdown_requested.is_set():
                if not self.global_queue.has_work():
                    break

                # Obey dynamic concurrency scaling
                if w_id > self.adaptive_controller.current_workers:
                    self.dashboard.update_worker(w_id, status="Standby (Scaled)")
                    time.sleep(0.1)
                    continue

                task = self.global_queue.get_next_task()
                if task is None:
                    if not self.global_queue.has_work():
                        break
                    self.dashboard.update_worker(w_id, status="Waiting")
                    time.sleep(0.05)
                    continue

                with active_count_lock:
                    active_workers_count += 1

                try:
                    success = self._download_task_worker(w_id, task)
                    if success:
                        counts["success"] += 1
                        self.dashboard.completed_episodes += 1
                    elif task.retry_count >= self.config.max_retries:
                        counts["failed"] += 1
                        self.dashboard.failed_episodes += 1
                except Exception as exc:
                    self.logger.error(f"[WORKER {w_id}] Unhandled error: {exc}")
                finally:
                    with active_count_lock:
                        active_workers_count -= 1

                # Update live dashboard stats
                q_stats = self.global_queue.stats()
                self.dashboard.downloading_episodes = q_stats["in_flight"]
                self.dashboard.pending_episodes = q_stats["pending"] + q_stats["delayed_retry"]

        # Concurrency monitor loop (runs every monitoring_interval seconds)
        def monitor_loop():
            while not self._shutdown_requested.is_set() and self.global_queue.has_work():
                for _ in range(max(1, int(self.config.monitoring_interval * 5))):
                    if self._shutdown_requested.is_set() or not self.global_queue.has_work():
                        return
                    time.sleep(0.2)

                total_speed_b = sum(
                    wp.speed_bytes for wp in self.dashboard.worker_slots.values()
                )
                new_concurrency = self.adaptive_controller.evaluate(
                    total_speed_bytes=total_speed_b,
                    active_workers_count=active_workers_count,
                )
                self.dashboard.current_concurrency = new_concurrency
                self.dashboard.adaptive_status = self.adaptive_controller.status_reason
                if psutil:
                    self.dashboard.cpu_pct = psutil.cpu_percent()
                    self.dashboard.ram_pct = psutil.virtual_memory().percent

        # Start maximum possible worker threads (they dynamically throttle based on current_workers)
        max_limit = self.config.max_workers
        self._threads = []
        for i in range(1, max_limit + 1):
            t = threading.Thread(
                target=worker_loop,
                args=(i,),
                name=f"Worker-{i}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)

        monitor_t = threading.Thread(target=monitor_loop, name="AdaptiveMonitor", daemon=True)
        monitor_t.start()

        # Await completion of all worker threads
        for t in self._threads:
            t.join()

        self.dashboard.stop()
        return counts

    # --------------------------------------------------------------------------
    # PHASE 5: FINAL REPORT
    # --------------------------------------------------------------------------

    def phase_5_report(self, counts: Dict[str, int]) -> Path:
        self.logger.info("=" * 60)
        self.logger.info("PHASE 5: FINAL SUMMARY & AUDIT REPORT")
        self.logger.info("=" * 60)

        elapsed = time.time() - self.dashboard.start_time
        mins, secs = divmod(int(elapsed), 60)
        elapsed_str = f"{mins}m {secs}s"

        report_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "elapsed_time": elapsed_str,
            "total_series": self.dashboard.total_series,
            "extracted_series": self.dashboard.extracted_series,
            "total_seasons": self.dashboard.total_seasons,
            "total_episodes": self.dashboard.total_episodes,
            "completed": self.dashboard.completed_episodes,
            "skipped": self.dashboard.skipped_episodes,
            "failed": counts.get("failed", 0),
            "state_files": {
                "download_state": str(self.state_mgr.download_state_file),
                "extraction_state": str(self.state_mgr.extraction_state_file),
            },
        }

        report_file = self.config.output_dir / "download_report.json"
        self.state_mgr.atomic_write(report_file, report_data)

        if Console:
            c = Console()
            t = Table(title="🎉 Download Operation Completed", border_style="green")
            t.add_column("Metric", style="bold cyan")
            t.add_column("Value", style="bold white")
            t.add_row("Total Series in File", str(self.dashboard.total_series))
            t.add_row("Extracted Series", str(self.dashboard.extracted_series))
            t.add_row("Total Seasons", str(self.dashboard.total_seasons))
            t.add_row("Total Episodes Tracked", str(self.dashboard.total_episodes))
            t.add_row("Newly Completed", f"[green]{counts.get('success', 0)}[/green]")
            t.add_row("Skipped (Already Valid)", f"[dim]{self.dashboard.skipped_episodes}[/dim]")
            t.add_row("Failed Episodes", f"[red]{counts.get('failed', 0)}[/red]")
            t.add_row("Total Runtime", elapsed_str)
            t.add_row("Audit Report JSON", str(report_file))
            c.print(t)
        else:
            print("\n" + "=" * 55)
            print("DOWNLOAD SUMMARY")
            print("=" * 55)
            for k, v in report_data.items():
                print(f"{k:20}: {v}")
            print("=" * 55)

        return report_file

    def run(self, force_extract: bool = False) -> None:
        self.register_signal_handlers()

        # Phase 1
        urls = self.phase_1_initialize()
        if not urls:
            self.logger.info("No series URLs to process. Add URLs into series.txt and re-run.")
            return

        # Phase 2
        season_files = self.phase_2_extract_series(urls, force=force_extract)

        # Phase 3
        self.phase_3_compile_global_queue(season_files)

        # Phase 4
        counts = self.phase_4_continuous_scheduling()

        # Phase 5
        self.phase_5_report(counts)


# ==============================================================================
# 17. CLI ENTRY POINT & MAIN
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Unified Smart Series Downloader: Adaptive Parallel Engine"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to custom config.json.",
    )
    parser.add_argument(
        "--series-file",
        type=Path,
        default=None,
        help="Path to custom series.txt.",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Process a single series URL directly without reading series.txt.",
    )
    parser.add_argument(
        "--force-extract",
        action="store_true",
        help="Force re-extraction of series pages even if already cached.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Override max parallel worker threads.",
    )
    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="Disable the Rich live terminal dashboard.",
    )

    args = parser.parse_args()

    cfg = Config(config_path=args.config)
    if args.series_file:
        cfg.data["paths"]["series_file"] = str(args.series_file)
    if args.workers:
        cfg.data["downloads"]["max_workers"] = args.workers
        cfg.data["downloads"]["initial_workers"] = min(cfg.initial_workers, args.workers)
    if args.no_dashboard:
        cfg.data["monitoring"]["dashboard"] = False

    orchestrator = UnifiedOrchestrator(cfg)

    if args.url:
        cfg.config_path.parent.mkdir(parents=True, exist_ok=True)
        temp_series = cfg.output_dir / "temp_cli_series.txt"
        temp_series.write_text(args.url.strip(), encoding="utf-8")
        cfg.data["paths"]["series_file"] = str(temp_series)

    orchestrator.run(force_extract=args.force_extract)


if __name__ == "__main__":
    main()
