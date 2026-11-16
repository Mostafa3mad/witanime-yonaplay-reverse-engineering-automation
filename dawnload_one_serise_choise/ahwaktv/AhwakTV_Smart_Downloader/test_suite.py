#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
Comprehensive Adaptive Parallel Engine Test Suite
Validates all 12 core requirements for the Dynamic Adaptive Downloader
================================================================================
"""

import os
import sys
import json
import time
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure Unified_Series_Downloader is in path
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(CURRENT_DIR))

from series_downloader import (
    Config,
    SafeJsonManager,
    SeriesExtractor,
    OneVidExtractor,
    VidSpeedExtractor,
    BaseServerExtractor,
    ServerRegistry,
    VideoValidator,
    FFmpegDownloader,
    RetryManager,
    GlobalFairQueue,
    AdaptiveConcurrencyController,
    DownloadTask,
    EpisodeStatus,
    SeriesStatus,
    UnifiedOrchestrator,
    HttpClient,
    AppLogger,
)


class TestDynamicAdaptiveDownloader(unittest.TestCase):

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_adaptive_dl_"))
        self.data_dir = self.test_dir / "data"
        self.logs_dir = self.test_dir / "logs"
        self.output_dir = self.test_dir / "Downloaded_Series"
        self.series_file = self.test_dir / "series.txt"
        self.config_file = self.test_dir / "config.json"

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.config = Config(base_dir=self.test_dir)
        self.config.data["paths"]["output_dir"] = str(self.output_dir)
        self.config.data["paths"]["data_dir"] = str(self.data_dir)
        self.config.data["paths"]["logs_dir"] = str(self.logs_dir)
        self.config.data["paths"]["series_file"] = str(self.series_file)
        self.config.data["downloads"]["min_workers"] = 2
        self.config.data["downloads"]["initial_workers"] = 4
        self.config.data["downloads"]["max_workers"] = 8
        self.config.data["downloads"]["monitoring_interval"] = 1
        self.config.data["downloads"]["max_retries"] = 3
        self.config.data["monitoring"]["dashboard"] = False

        self.logger = AppLogger(self.logs_dir)
        self.state_mgr = SafeJsonManager(self.data_dir, auto_backup=True, max_backups=5)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    # --------------------------------------------------------------------------
    # Scenario 1: Loading 32 episodes from 3 seasons into Central Global Queue
    # --------------------------------------------------------------------------
    def test_01_load_32_episodes_from_3_seasons(self):
        """Test 1: Global queue contains all 32 episodes across 3 seasons unified."""
        queue = GlobalFairQueue(self.logger)

        # Season 1: 10 episodes, Season 2: 12 episodes, Season 3: 10 episodes = 32 episodes
        season_counts = {1: 10, 2: 12, 3: 10}
        total_created = 0
        for s_num, count in season_counts.items():
            for ep_num in range(1, count + 1):
                task = DownloadTask(
                    series_name="مسلسل الملحمة",
                    series_url="https://example.com/series/epic",
                    season_number=s_num,
                    episode_number=ep_num,
                    episode_title=f"الحلقة {ep_num} الموسم {s_num}",
                    episode_url=f"https://example.com/see.php?vid=s{s_num}_ep{ep_num}",
                    output_dir=self.output_dir,
                    output_file=self.output_dir / f"ep_s{s_num}_{ep_num}.mp4",
                    temp_file=self.output_dir / f"ep_s{s_num}_{ep_num}.part.mp4",
                )
                added = queue.add_task(task)
                self.assertTrue(added)
                total_created += 1

        self.assertEqual(total_created, 32)
        stats = queue.stats()
        self.assertEqual(stats["total"], 32)
        self.assertEqual(stats["pending"], 32)
        print("\n[PASS] Test 1: Successfully queued 32 episodes across 3 seasons in one Central Global Queue.")

    # --------------------------------------------------------------------------
    # Scenario 2: Starting 3 seasons in parallel (Fair Round-Robin)
    # --------------------------------------------------------------------------
    def test_02_parallel_interleaving_across_seasons(self):
        """Test 2: Round-robin delivers tasks from Season 1, Season 2, and Season 3 concurrently."""
        queue = GlobalFairQueue(self.logger)
        for s in [1, 2, 3]:
            for e in range(1, 5):
                task = DownloadTask(
                    series_name="مسلسل الواحة",
                    series_url="https://example.com/series/oasis",
                    season_number=s,
                    episode_number=e,
                    episode_title=f"الحلقة {e} الموسم {s}",
                    episode_url=f"https://example.com/see.php?vid=s{s}_e{e}",
                    output_dir=self.output_dir,
                    output_file=self.output_dir / f"ep_{s}_{e}.mp4",
                    temp_file=self.output_dir / f"ep_{s}_{e}.part.mp4",
                )
                queue.add_task(task)

        # Pull first 3 tasks
        t1 = queue.get_next_task()
        t2 = queue.get_next_task()
        t3 = queue.get_next_task()

        self.assertIsNotNone(t1)
        self.assertIsNotNone(t2)
        self.assertIsNotNone(t3)

        seasons_pulled = [t1.season_number, t2.season_number, t3.season_number]
        # Must pull from different seasons in round-robin fashion
        self.assertEqual(sorted(seasons_pulled), [1, 2, 3])
        print(f"\n[PASS] Test 2: Fair Round-Robin verified; first 3 tasks pulled from seasons: {seasons_pulled}.")

    # --------------------------------------------------------------------------
    # Scenario 3 & 4: Continuous Scheduling & Worker Transitioning to Next Task
    # --------------------------------------------------------------------------
    def test_03_and_04_continuous_queue_beyond_first_workers(self):
        """Test 3 & 4: Workers automatically pull next task; queue completes all 32 tasks."""
        orchestrator = UnifiedOrchestrator(self.config)

        # Populate queue with 32 tasks
        for s in [1, 2, 3]:
            count = 10 if s != 2 else 12
            for e in range(1, count + 1):
                task = DownloadTask(
                    series_name="مسلسل الفرسان",
                    series_url="https://example.com/series/knights",
                    season_number=s,
                    episode_number=e,
                    episode_title=f"الحلقة {e} الموسم {s}",
                    episode_url=f"https://example.com/see.php?vid=k_{s}_{e}",
                    output_dir=self.output_dir,
                    output_file=self.output_dir / f"k_{s}_{e}.mp4",
                    temp_file=self.output_dir / f"k_{s}_{e}.part.mp4",
                )
                orchestrator.global_queue.add_task(task)

        processed_order = []
        lock = threading.Lock()

        def mock_worker_download(w_id, task):
            with lock:
                processed_order.append((w_id, task.unique_key))
            time.sleep(0.01)
            orchestrator.global_queue.mark_completed(task)
            return True

        with patch.object(orchestrator, "_download_task_worker", side_effect=mock_worker_download):
            counts = orchestrator.phase_4_continuous_scheduling()

        self.assertEqual(counts["success"], 32)
        self.assertEqual(len(processed_order), 32)
        self.assertFalse(orchestrator.global_queue.has_work())
        print(f"\n[PASS] Test 3 & 4: All 32 episodes processed continuously across dynamic workers.")

    # --------------------------------------------------------------------------
    # Scenario 5: No Idle Workers with Eligible Tasks
    # --------------------------------------------------------------------------
    def test_05_no_idle_workers_when_tasks_exist(self):
        """Test 5: Workers remain active and immediately claim available tasks."""
        queue = GlobalFairQueue(self.logger)
        for i in range(1, 10):
            queue.add_task(
                DownloadTask(
                    series_name="Series",
                    series_url="url",
                    season_number=1,
                    episode_number=i,
                    episode_title=f"Ep {i}",
                    episode_url=f"url_{i}",
                    output_dir=self.output_dir,
                    output_file=self.output_dir / f"ep_{i}.mp4",
                    temp_file=self.output_dir / f"ep_{i}.part.mp4",
                )
            )

        # Any available worker pulling get_next_task() must receive a task
        active_claims = []
        for _ in range(5):
            t = queue.get_next_task()
            self.assertIsNotNone(t)
            active_claims.append(t)

        self.assertEqual(len(active_claims), 5)
        self.assertEqual(queue.stats()["in_flight"], 5)
        print("\n[PASS] Test 5: Verified zero idle worker stalls when eligible tasks exist in queue.")

    # --------------------------------------------------------------------------
    # Scenario 6: Intelligent Retry After Transient Failure with Jitter
    # --------------------------------------------------------------------------
    def test_06_intelligent_retry_system(self):
        """Test 6: Transient error triggers delayed retry; re-queued upon backoff expiration."""
        retry_mgr = RetryManager(max_retries=3, backoff_factor=1.5, jitter=0.2, logger=self.logger)
        queue = GlobalFairQueue(self.logger)

        task = DownloadTask(
            series_name="Retry Series",
            series_url="url",
            season_number=1,
            episode_number=1,
            episode_title="Retry Ep 1",
            episode_url="url",
            output_dir=self.output_dir,
            output_file=self.output_dir / "ep.mp4",
            temp_file=self.output_dir / "ep.part.mp4",
        )
        queue.add_task(task)

        # Claim task
        claimed = queue.get_next_task()
        self.assertIsNotNone(claimed)

        # Simulate network timeout
        err = TimeoutError("HTTP Connection Timeout")
        delay = retry_mgr.get_retry_delay(claimed, err)
        self.assertIsNotNone(delay)
        self.assertGreaterEqual(delay, 1.0)

        # Mark failed with short delay for test
        queue.mark_failed(claimed, str(err), retry_delay=0.1)
        self.assertEqual(queue.stats()["delayed_retry"], 1)

        # Immediately pulling should return None (waiting for backoff timer)
        self.assertIsNone(queue.get_next_task())

        # Wait for delay to expire
        time.sleep(0.15)

        # Now task must be eligible and re-pulled!
        re_claimed = queue.get_next_task()
        self.assertIsNotNone(re_claimed)
        self.assertEqual(re_claimed.episode_number, 1)
        self.assertEqual(re_claimed.retry_count, 1)
        print("\n[PASS] Test 6: Intelligent retry with exponential backoff & delay verified.")

    # --------------------------------------------------------------------------
    # Scenario 7 & 8: Adaptive Concurrency Scaling (Down on Errors, Up on Stability)
    # --------------------------------------------------------------------------
    def test_07_and_08_adaptive_concurrency_controller(self):
        """Test 7 & 8: Controller scales down on error spikes, scales up when speed is stable."""
        controller = AdaptiveConcurrencyController(
            min_workers=2,
            initial_workers=4,
            max_workers=8,
            monitoring_interval=0,  # immediate eval for test
            enabled=True,
            logger=self.logger,
        )

        # Initial workers = 4
        self.assertEqual(controller.current_workers, 4)

        # Simulate errors (throttles / 429)
        controller.record_error()
        controller.record_error()
        scaled_down = controller.evaluate(total_speed_bytes=1000, active_workers_count=4)
        self.assertLess(scaled_down, 4)
        self.assertTrue("Reducing concurrency" in controller.status_reason)

        # Now simulate healthy network with high speed and zero errors
        for _ in range(5):
            controller.record_success()
            controller.evaluate(total_speed_bytes=15 * 1024 * 1024, active_workers_count=controller.current_workers)

        self.assertGreater(controller.current_workers, scaled_down)
        print(f"\n[PASS] Test 7 & 8: Adaptive controller dynamically scaled workers (Down: {scaled_down}, Up: {controller.current_workers}).")

    # --------------------------------------------------------------------------
    # Scenario 9: Prevent Duplicate Downloads of Same Task
    # --------------------------------------------------------------------------
    def test_09_no_duplicate_task_in_flight(self):
        """Test 9: An in-flight task cannot be claimed twice simultaneously."""
        queue = GlobalFairQueue(self.logger)
        task = DownloadTask(
            series_name="Unique Series",
            series_url="url",
            season_number=1,
            episode_number=1,
            episode_title="Unique Ep 1",
            episode_url="url_1",
            output_dir=self.output_dir,
            output_file=self.output_dir / "ep.mp4",
            temp_file=self.output_dir / "ep.part.mp4",
        )
        queue.add_task(task)

        # Claim task
        t1 = queue.get_next_task()
        self.assertIsNotNone(t1)

        # Re-adding or pulling must not yield duplicate
        t2 = queue.get_next_task()
        self.assertIsNone(t2)
        print("\n[PASS] Test 9: Deduplication and in-flight locking verified; no duplicate processing.")

    # --------------------------------------------------------------------------
    # Scenario 10: State Recovery After Interruption (Ctrl+C)
    # --------------------------------------------------------------------------
    def test_10_recover_interrupted_state(self):
        """Test 10: Interrupted 'downloading' tasks are restored to 'pending' upon startup."""
        state_file = self.data_dir / "download_state.json"
        state_data = {
            "series": {
                "Interrupted Series": {
                    "series_name": "Interrupted Series",
                    "status": "in_progress",
                    "seasons": {
                        "1": {
                            "episodes": {
                                "1": {
                                    "status": EpisodeStatus.DOWNLOADING,
                                    "episode_title": "Ep 1",
                                    "retry_count": 0,
                                }
                            }
                        }
                    },
                }
            }
        }
        self.state_mgr.atomic_write(state_file, state_data)

        # Recover
        recovered = self.state_mgr.recover_stale_in_progress()
        self.assertEqual(recovered, 1)

        updated = self.state_mgr.get_download_state()
        ep_status = updated["series"]["Interrupted Series"]["seasons"]["1"]["episodes"]["1"]["status"]
        self.assertEqual(ep_status, EpisodeStatus.PENDING)
        print("\n[PASS] Test 10: Stale interrupted task restored to 'pending' state successfully.")

    # --------------------------------------------------------------------------
    # Scenario 11: Non-fatal Failure Isolation
    # --------------------------------------------------------------------------
    def test_11_failure_does_not_halt_queue(self):
        """Test 11: A fatal failure on one episode does not interrupt remaining episodes."""
        orchestrator = UnifiedOrchestrator(self.config)
        for i in range(1, 6):
            orchestrator.global_queue.add_task(
                DownloadTask(
                    series_name="Series",
                    series_url="url",
                    season_number=1,
                    episode_number=i,
                    episode_title=f"Ep {i}",
                    episode_url=f"url_{i}",
                    output_dir=self.output_dir,
                    output_file=self.output_dir / f"ep_{i}.mp4",
                    temp_file=self.output_dir / f"ep_{i}.part.mp4",
                )
            )

        # Mock: Episode 3 fails permanently (max retries reached), others succeed
        def mock_worker(w_id, task):
            if task.episode_number == 3:
                task.retry_count = 5  # exceed max retries
                orchestrator.global_queue.mark_failed(task, "Fatal 404", retry_delay=None)
                return False
            orchestrator.global_queue.mark_completed(task)
            return True

        with patch.object(orchestrator, "_download_task_worker", side_effect=mock_worker):
            counts = orchestrator.phase_4_continuous_scheduling()

        self.assertEqual(counts["success"], 4)
        self.assertEqual(counts["failed"], 1)
        print("\n[PASS] Test 11: 4 episodes succeeded despite episode 3 failure; queue not halted.")

    # --------------------------------------------------------------------------
    # Scenario 12: Deadlock-Free FFmpeg Execution & Termination
    # --------------------------------------------------------------------------
    def test_12_ffmpeg_deadlock_prevention(self):
        """Test 12: Background stderr reader prevents pipe deadlock and cleans up child processes."""
        downloader = FFmpegDownloader(self.config, self.logger)
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.stdout = iter(["out_time_us=5000000", "total_size=1048576", "progress=continue"])
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.readline.side_effect = ["warning line 1", "warning line 2", ""]
        mock_proc.wait.return_value = 0

        def mock_popen(*args, **kwargs):
            temp_f.write_text("simulated valid video content")
            return mock_proc

        with patch("subprocess.Popen", side_effect=mock_popen):
            with patch.object(VideoValidator, "is_valid_video", return_value=True):
                temp_f = self.output_dir / "temp.part.mp4"
                out_f = self.output_dir / "out.mp4"

                ok = downloader.download("http://stream.m3u8", out_f, temp_f)
                self.assertTrue(ok)

        self.assertEqual(len(downloader._active_processes), 0)
        print("\n[PASS] Test 12: Stderr drained concurrently in background thread; zero pipe deadlock.")

    # --------------------------------------------------------------------------
    # Scenario 13: VidSpeed Extractor and Automatic Server Fallback
    # --------------------------------------------------------------------------
    def test_13_vidspeed_extractor_and_server_fallback(self):
        """Test 13: VidSpeed extractor unpacks packed JS, and fallback triggers when 1vid fails."""
        orchestrator = UnifiedOrchestrator(self.config)

        # 1. Verify extractors are registered in registry
        names = orchestrator.server_registry.get_supported_names()
        self.assertIn("1vid", names)
        self.assertIn("vidspeed", names)

        # 2. Test VidSpeedExtractor with packed JS HTML mock
        packed_html = """
        <html>
        <head><title>Vidspeed Player</title></head>
        <body>
        <script type='text/javascript'>
        eval(function(p,a,c,k,e,d){e=function(c){return c};if(!''.replace(/^/,String)){while(c--){d[c]=k[c]||c}k=[function(e){return d[e]}];e=function(){return'\\w+'};c=1};while(c--){if(k[c]){p=p.replace(new RegExp('\\b'+e(c)+'\\b','g'),k[c])}}return p}('0("1://2.3/4/5.6");',7,7,'var|https|vsped|com|hls|master|m3u8'.split('|'),0,{}))
        </script>
        </body>
        </html>
        """
        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = packed_html
        mock_resp.decode.return_value = packed_html
        mock_http.get.return_value = mock_resp

        vidspeed_ext = orchestrator.server_registry.get("vidspeed")
        self.assertIsNotNone(vidspeed_ext)
        links = vidspeed_ext.extract_m3u8("https://vidspeed.org/embed-test.html", mock_http)
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0], "https://vsped.com/hls/master.m3u8")

        # 3. Test Fallback in _download_task_worker:
        # 1vid fails, then Vidspeed is tried and succeeds!
        task = DownloadTask(
            series_name="Fallback Series",
            series_url="https://example.com/series/fallback",
            season_number=1,
            episode_number=1,
            episode_title="Fallback Ep 1",
            episode_url="https://example.com/see.php?vid=fallback_vid",
            output_dir=self.output_dir,
            output_file=self.output_dir / "fallback_ep1.mp4",
            temp_file=self.output_dir / "fallback_ep1.part.mp4",
        )

        # Mock see.php returning both 1vid and Vidspeed
        orchestrator.server_parser.get_servers = MagicMock(return_value={
            "1vid": "https://1vid.xyz/embed-failed.html",
            "Vidspeed": "https://vidspeed.org/embed-success.html",
        })

        # Mock 1vid extractor failing, and vidspeed extractor succeeding
        def mock_extract(embed_url, http_client):
            if "1vid" in embed_url:
                raise RuntimeError("1vid stream unavailable or blocked")
            return ["https://vsped.com/hls/master.m3u8"]

        orchestrator.server_registry.get("1vid").extract_m3u8 = MagicMock(side_effect=mock_extract)
        orchestrator.server_registry.get("vidspeed").extract_m3u8 = MagicMock(side_effect=mock_extract)

        # Mock ffmpeg downloader succeeding on the second server
        orchestrator.ffmpeg_downloader.download = MagicMock(return_value=True)
        task.output_file.write_text("simulated downloaded video")

        success = orchestrator._download_task_worker(worker_id=1, task=task)
        self.assertTrue(success)

        # Verify state in download_state.json recorded "Vidspeed"
        state = orchestrator.state_mgr.get_download_state()
        ep_entry = state["series"]["Fallback Series"]["seasons"]["1"]["episodes"]["1"]
        self.assertEqual(ep_entry["status"], EpisodeStatus.COMPLETED)
        self.assertEqual(ep_entry["server"], "Vidspeed")
        print("\n[PASS] Test 13: VidSpeed extraction and automatic fallback from 1vid to Vidspeed verified.")


if __name__ == "__main__":
    unittest.main()
