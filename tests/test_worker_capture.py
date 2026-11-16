"""Offline unit tests for ci-stream-worker (synthetic FFmpeg + Fake Drive)."""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

import worker_capture


class _FakeReq:
    def __init__(self, fn) -> None:
        self._fn = fn

    def execute(self):
        return self._fn()


class _FakeUploadReq:
    def __init__(self, files_store: dict[str, dict[str, Any]], body: dict[str, Any], media_path: str, seq_fn) -> None:
        self._store = files_store
        self._body = body
        self._media_path = Path(media_path)
        self._seq_fn = seq_fn

    def next_chunk(self, num_retries: int = 3):
        raw = self._media_path.read_bytes()
        fid = f"file_{self._seq_fn()}"
        meta = {
            "id": fid,
            "name": self._body["name"],
            "parents": list(self._body.get("parents") or []),
            "size": str(len(raw)),
            "md5Checksum": hashlib.md5(raw).hexdigest(),
            "trashed": False,
            "mimeType": "video/mp4",
        }
        self._store[fid] = meta
        return None, meta


class FakeFilesResource:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.folder_paths: dict[str, dict[str, Any]] = {}
        self._seq = 0

    def _next(self) -> int:
        self._seq += 1
        return self._seq

    def list(self, **kwargs):
        q = kwargs.get("q", "")

        def _exec():
            if "application/vnd.google-apps.folder" in q:
                # Extract name = '...'
                import re

                m_name = re.search(r"name = '([^']+)'", q)
                m_par = re.search(r"'([^']+)' in parents", q)
                name = m_name.group(1) if m_name else ""
                par = m_par.group(1) if m_par else None
                for item in self.items.values():
                    if item.get("mimeType") == "application/vnd.google-apps.folder" and item.get("name") == name:
                        parents = item.get("parents") or []
                        if par is None or par in parents:
                            return {"files": [item]}
                return {"files": []}
            return {"files": []}

        return _FakeReq(_exec)

    def create(self, **kwargs):
        body = kwargs["body"]
        if "media_body" in kwargs:
            media = kwargs["media_body"]
            if hasattr(media, "_filename"):
                return _FakeUploadReq(self.items, body, media._filename, self._next)
            raw_mem = media.getbytes(0, media.size()) if hasattr(media, "size") else b""

            def _exec_mem():
                fid = f"file_{self._next()}"
                meta = {
                    "id": fid,
                    "name": body["name"],
                    "parents": list(body.get("parents") or []),
                    "mimeType": "text/plain",
                    "trashed": False,
                    "content": raw_mem.decode("utf-8", errors="replace"),
                }
                self.items[fid] = meta
                return meta

            return _FakeReq(_exec_mem)

        def _exec():
            fid = f"folder_{self._next()}"
            meta = {
                "id": fid,
                "name": body["name"],
                "parents": list(body.get("parents") or []),
                "mimeType": body["mimeType"],
                "trashed": False,
            }
            self.items[fid] = meta
            return meta

        return _FakeReq(_exec)

    def update(self, **kwargs):
        fid = kwargs["fileId"]
        media = kwargs.get("media_body")

        def _exec():
            if fid in self.items and media is not None:
                raw_mem = media.getbytes(0, media.size())
                self.items[fid]["content"] = raw_mem.decode("utf-8", errors="replace")
            return dict(self.items.get(fid, {"id": fid}))

        return _FakeReq(_exec)

    def get(self, **kwargs):
        fid = kwargs["fileId"]
        return _FakeReq(lambda: dict(self.items[fid]))

    def delete(self, **kwargs):
        fid = kwargs["fileId"]
        return _FakeReq(lambda: self.items.pop(fid, {}))


class FakeDriveService:
    def __init__(self) -> None:
        self._files = FakeFilesResource()

    def files(self) -> FakeFilesResource:
        return self._files


class WorkerCaptureTests(unittest.TestCase):
    def test_end_to_end_synthetic_segmentation_drive_folder_date_and_cleanup(self) -> None:
        fake_service = FakeDriveService()
        probes = [
            {"is_live": True, "title": "Test", "category": "Games", "transient_error": False},
            {"is_live": False, "error": "No playable streams found", "transient_error": False},
        ]

        def probe_fn(_s: str) -> dict[str, Any]:
            if len(probes) > 1:
                return probes.pop(0)
            return probes[0]

        def producer(_s: str, _q: str) -> list[str]:
            return [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=320x180:rate=30:duration=4",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=44100:duration=4",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-g",
                "30",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-f",
                "mpegts",
                "pipe:1",
            ]

        with tempfile.TemporaryDirectory() as td:
            tmp_dir = Path(td)
            rc = worker_capture.run_worker(
                streamer="https://www.twitch.tv/Alanzoka",
                quality="best",
                segment_seconds=2,
                project_folder="TWITCH_LIVES",
                drive_root="Youtube-Farm-Clips",
                block_num=1,
                max_runtime_seconds=30,
                offline_wait_seconds=1,
                temp_dir=tmp_dir,
                drive_service=fake_service,
                probe_fn=probe_fn,
                producer_cmd_factory=producer,
            )
            self.assertEqual(rc, 0)
            folders = [
                v["name"]
                for v in fake_service.files().items.values()
                if v.get("mimeType") == "application/vnd.google-apps.folder"
            ]
            self.assertIn("Youtube-Farm-Clips", folders)
            self.assertIn("TWITCH_LIVES", folders)
            self.assertIn("alanzoka", folders)
            self.assertIn("00_ORIGINAIS", folders)
            mp4s = [
                v
                for v in fake_service.files().items.values()
                if v.get("mimeType") == "video/mp4"
            ]
            self.assertGreaterEqual(len(mp4s), 2)
            leftovers = [p for p in tmp_dir.rglob("*") if p.is_file()]
            self.assertEqual(leftovers, [])
            logs = [
                v for v in fake_service.files().items.values() if v.get("name") == "worker_live_bloco1.log"
            ]
            self.assertEqual(len(logs), 1)
            self.assertIn("[UPLOAD OK + LIMPEZA LOCAL]", logs[0].get("content", ""))

    def test_fmp4_input_stream_auto_detected_and_segmented(self) -> None:
        fake_service = FakeDriveService()
        probes = [
            {"is_live": True, "title": "fMP4 Test", "category": "Games", "transient_error": False},
            {"is_live": False, "error": "No playable streams found", "transient_error": False},
        ]

        def probe_fn(_s: str) -> dict[str, Any]:
            return probes.pop(0) if len(probes) > 1 else probes[0]

        def producer_fmp4(_s: str, _q: str) -> list[str]:
            return [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=320x180:rate=30:duration=4",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=44100:duration=4",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-g",
                "30",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-f",
                "mp4",
                "-movflags",
                "frag_keyframe+empty_moov",
                "pipe:1",
            ]

        with tempfile.TemporaryDirectory() as td:
            rc = worker_capture.run_worker(
                streamer="alanzoka",
                quality="best",
                segment_seconds=2,
                project_folder="TWITCH_LIVES",
                drive_root="Youtube-Farm-Clips",
                block_num=1,
                max_runtime_seconds=30,
                offline_wait_seconds=1,
                temp_dir=Path(td),
                drive_service=fake_service,
                probe_fn=probe_fn,
                producer_cmd_factory=producer_fmp4,
            )
            self.assertEqual(rc, 0)
            mp4s = [
                v for v in fake_service.files().items.values() if v.get("mimeType") == "video/mp4"
            ]
            self.assertGreaterEqual(len(mp4s), 2)


if __name__ == "__main__":
    unittest.main()
