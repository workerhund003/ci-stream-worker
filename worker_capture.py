"""Autonomous cloud stream capture -> TS->MP4 remux -> Google Drive uploader."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaInMemoryUpload

import resumable_uploader

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]
BRT_TZ = ZoneInfo("America/Sao_Paulo")
TS_SEG_RE = re.compile(r"_seg(\d+)\.ts$")
TWITCH_LOGIN_RE = re.compile(r"^[a-z0-9_]{2,32}$")

_ACTIVE_LOG_SINK: Any = None


def log(msg: str) -> None:
    ts = datetime.now(BRT_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if _ACTIVE_LOG_SINK is not None:
        _ACTIVE_LOG_SINK.append(line)


def normalize_streamer(raw: str) -> str:
    val = str(raw or "").strip()
    val = re.sub(r"^https?://(?:www\.|m\.)?twitch\.tv/", "", val, flags=re.IGNORECASE)
    val = val.split("?", 1)[0].split("#", 1)[0].strip("/").lower()
    if not TWITCH_LOGIN_RE.match(val):
        raise ValueError(f"Streamer inválido: '{raw}'")
    return val


def sanitize_folder_name(raw: str, fallback: str = "TWITCH_LIVES") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(raw or "").strip()).strip("_")
    return cleaned or fallback


def compute_md5(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.md5()
    with path.open("rb") as fh:
        while True:
            buf = fh.read(chunk_size)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def probe_media(path: Path, ffprobe_bin: str = "ffprobe") -> dict[str, Any]:
    proc = subprocess.run(
        [
            ffprobe_bin,
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_type,codec_name",
            "-show_entries",
            "format=duration,size",
            "-of",
            "json",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe falhou em '{path.name}': {proc.stderr.strip()}")
    return json.loads(proc.stdout or "{}")


def remux_ts_to_mp4(
    ts_path: Path,
    mp4_path: Path,
    *,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
) -> dict[str, Any]:
    if not ts_path.is_file() or ts_path.stat().st_size <= 0:
        raise RuntimeError(f"Segmento TS vazio ou ausente: {ts_path}")

    src_probe = probe_media(ts_path, ffprobe_bin=ffprobe_bin)
    src_streams = src_probe.get("streams") or []
    src_has_video = any(s.get("codec_type") == "video" for s in src_streams)
    src_has_audio = any(s.get("codec_type") == "audio" for s in src_streams)
    if not src_has_video and not src_has_audio:
        raise RuntimeError(f"Segmento TS sem faixas A/V: {ts_path.name}")

    mp4_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_mp4 = mp4_path.with_name(f".{mp4_path.stem}.tmp.mp4")
    tmp_mp4.unlink(missing_ok=True)

    proc = subprocess.run(
        [
            ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-fflags",
            "+genpts",
            "-i",
            str(ts_path),
            "-c",
            "copy",
            "-map",
            "0:v?",
            "-map",
            "0:a?",
            "-movflags",
            "+faststart",
            str(tmp_mp4),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    if proc.returncode != 0 or not tmp_mp4.is_file() or tmp_mp4.stat().st_size <= 0:
        tmp_mp4.unlink(missing_ok=True)
        raise RuntimeError(
            f"FFmpeg remux TS->MP4 falhou (rc={proc.returncode}): {proc.stderr.strip()}"
        )

    dst_probe = probe_media(tmp_mp4, ffprobe_bin=ffprobe_bin)
    dst_streams = dst_probe.get("streams") or []
    dst_has_video = any(s.get("codec_type") == "video" for s in dst_streams)
    dst_has_audio = any(s.get("codec_type") == "audio" for s in dst_streams)
    if src_has_video and not dst_has_video:
        tmp_mp4.unlink(missing_ok=True)
        raise RuntimeError(f"Faixa de vídeo perdida no remux: {mp4_path.name}")
    if src_has_audio and not dst_has_audio:
        tmp_mp4.unlink(missing_ok=True)
        raise RuntimeError(f"Faixa de áudio perdida no remux: {mp4_path.name}")

    duration = float((dst_probe.get("format") or {}).get("duration") or 0.0)
    if duration <= 0.0:
        tmp_mp4.unlink(missing_ok=True)
        raise RuntimeError(f"Duração inválida ({duration}s) em {mp4_path.name}")

    os.replace(tmp_mp4, mp4_path)
    return {
        "size_bytes": mp4_path.stat().st_size,
        "md5_hex": compute_md5(mp4_path),
        "duration": round(duration, 2),
        "has_video": dst_has_video,
        "has_audio": dst_has_audio,
    }


def load_drive_credentials_from_env(
    *,
    token_env: str = "GDRIVE_TOKEN_JSON_B64",
    token_file: Path | None = None,
) -> Credentials:
    raw_b64 = os.environ.get(token_env, "").strip()
    if raw_b64:
        token_json_text = base64.b64decode(raw_b64).decode("utf-8")
        info = json.loads(token_json_text)
        creds = Credentials.from_authorized_user_info(info, scopes=DRIVE_SCOPES)
    elif token_file and token_file.is_file():
        creds = Credentials.from_authorized_user_file(str(token_file), scopes=DRIVE_SCOPES)
    else:
        raise RuntimeError(
            f"Credencial OAuth ausente: defina secret '{token_env}' ou '--token-file'."
        )

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds


def load_drive_service_from_env(
    *,
    token_env: str = "GDRIVE_TOKEN_JSON_B64",
    token_file: Path | None = None,
) -> Any:
    creds = load_drive_credentials_from_env(token_env=token_env, token_file=token_file)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


class DriveUploader:
    """Google Drive resumable uploader with folder hierarchy, dedup, and MD5 verification."""

    def __init__(
        self,
        service: Any,
        drive_folder_parts: list[str],
        *,
        credentials: Credentials | None = None,
    ) -> None:
        self.credentials = credentials
        self.service = (
            build("drive", "v3", credentials=credentials, cache_discovery=False)
            if credentials is not None
            else service
        )
        self.drive_folder_parts = drive_folder_parts
        self._target_folder_id: str | None = None
        self._lock = threading.Lock()
        self._creds_lock = threading.Lock()

    def _find_or_create_folder(self, name: str, parent_id: str | None) -> str:
        escaped = name.replace("'", "\\'")
        q_parts = [
            "mimeType = 'application/vnd.google-apps.folder'",
            f"name = '{escaped}'",
            "trashed = false",
        ]
        if parent_id:
            q_parts.append(f"'{parent_id}' in parents")
        res = (
            self.service.files()
            .list(
                q=" and ".join(q_parts),
                fields="files(id, name)",
                pageSize=5,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = res.get("files") or []
        if files:
            return str(files[0]["id"])

        body: dict[str, Any] = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if parent_id:
            body["parents"] = [parent_id]
        created = (
            self.service.files()
            .create(body=body, fields="id, name", supportsAllDrives=True)
            .execute()
        )
        return str(created["id"])

    def ensure_folder_path(self, parts: list[str]) -> str:
        parent_id: str | None = None
        for part in parts:
            parent_id = self._find_or_create_folder(part, parent_id)
        assert parent_id is not None
        return parent_id

    def ensure_target_folder(self) -> str:
        with self._lock:
            if self._target_folder_id:
                return self._target_folder_id
            self._target_folder_id = self.ensure_folder_path(self.drive_folder_parts)
            return self._target_folder_id

    def _find_existing_file(self, folder_id: str, filename: str) -> dict[str, Any] | None:
        escaped = filename.replace("'", "\\'")
        res = (
            self.service.files()
            .list(
                q=f"name = '{escaped}' and '{folder_id}' in parents and trashed = false",
                fields="files(id, name, size, md5Checksum, trashed)",
                pageSize=5,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = res.get("files") or []
        return files[0] if files else None

    def _verify_remote(
        self,
        file_id: str,
        *,
        expected_name: str,
        expected_size: int,
        expected_md5: str,
    ) -> dict[str, Any]:
        meta = (
            self.service.files()
            .get(
                fileId=file_id,
                fields="id, name, size, md5Checksum, trashed",
                supportsAllDrives=True,
            )
            .execute()
        )
        if meta.get("trashed"):
            raise RuntimeError("Arquivo remoto marcado como trashed.")
        if meta.get("name") != expected_name:
            raise RuntimeError(f"Nome divergente no Drive: {meta.get('name')} != {expected_name}")
        if int(meta.get("size") or -1) != expected_size:
            raise RuntimeError(f"Tamanho divergente no Drive: {meta.get('size')} != {expected_size}")
        if str(meta.get("md5Checksum") or "") != expected_md5:
            raise RuntimeError("MD5 divergente no Drive.")
        return meta

    def upload_and_verify(self, mp4_path: Path, meta: dict[str, Any], max_retries: int = 5) -> str:
        expected_size = int(meta["size_bytes"])
        expected_md5 = str(meta["md5_hex"])
        last_err: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                folder_id = self.ensure_target_folder()
                existing = self._find_existing_file(folder_id, mp4_path.name)
                if existing and existing.get("id"):
                    try:
                        self._verify_remote(
                            str(existing["id"]),
                            expected_name=mp4_path.name,
                            expected_size=expected_size,
                            expected_md5=expected_md5,
                        )
                        log(f"[DEDUP OK] {mp4_path.name} já confirmado no Drive (id={existing['id']}).")
                        return str(existing["id"])
                    except Exception:
                        self.service.files().delete(
                            fileId=str(existing["id"]), supportsAllDrives=True
                        ).execute()

                app_props = {
                    "pipeline_source": "ci_stream_worker",
                    "duration_seconds": str(meta.get("duration", "")),
                }
                if self.credentials is not None:
                    with self._creds_lock:
                        if self.credentials.expired and self.credentials.refresh_token:
                            self.credentials.refresh(Request())
                    with mp4_path.open("rb") as fh:
                        up_res = resumable_uploader.stream_to_drive(
                            creds=self.credentials,
                            stream=fh,
                            name=mp4_path.name,
                            folder_id=folder_id,
                            mime_type="video/mp4",
                            app_properties=app_props,
                            chunk_size=8 * 1024 * 1024,
                            max_retries=5,
                            timeout_s=120.0,
                        )
                    file_id = str(up_res.file_metadata.get("id") or "")
                else:
                    body = {
                        "name": mp4_path.name,
                        "parents": [folder_id],
                        "appProperties": app_props,
                    }
                    media = MediaFileUpload(
                        str(mp4_path),
                        mimetype="video/mp4",
                        chunksize=8 * 1024 * 1024,
                        resumable=True,
                    )
                    req = self.service.files().create(
                        body=body,
                        media_body=media,
                        fields="id, name, size, md5Checksum",
                        supportsAllDrives=True,
                    )
                    response = None
                    while response is None:
                        _status, response = req.next_chunk(num_retries=3)
                    file_id = str((response or {}).get("id") or "")

                if not file_id:
                    raise RuntimeError("Upload retornou resposta sem file ID.")
                self._verify_remote(
                    file_id,
                    expected_name=mp4_path.name,
                    expected_size=expected_size,
                    expected_md5=expected_md5,
                )
                return file_id
            except Exception as exc:
                last_err = exc
                backoff = min(60, 5 * attempt)
                log(
                    f"[UPLOAD ERRO] {mp4_path.name} tentativa {attempt}/{max_retries}: {exc}. "
                    f"Tentando novamente em {backoff}s..."
                )
                if self.credentials is not None:
                    self.service = build("drive", "v3", credentials=self.credentials, cache_discovery=False)
                if attempt < max_retries:
                    time.sleep(backoff)

        raise RuntimeError(f"Falha definitiva no upload de '{mp4_path.name}': {last_err}")


class DriveLiveLogSink:
    """Maintains a throttled live log file in Google Drive for real-time monitoring."""

    def __init__(
        self,
        uploader: DriveUploader,
        streamer_folder_parts: list[str],
        filename: str,
        flush_interval_seconds: float = 20.0,
        max_lines: int = 120,
    ) -> None:
        self.uploader = uploader
        # Dedicated Drive service so httplib2 never shares sockets with the upload thread.
        self._service = (
            build("drive", "v3", credentials=uploader.credentials, cache_discovery=False)
            if uploader.credentials is not None
            else uploader.service
        )
        self.streamer_folder_parts = streamer_folder_parts
        self.filename = filename
        self.flush_interval_seconds = flush_interval_seconds
        self._lines: deque[str] = deque(maxlen=max_lines)
        self._lock = threading.Lock()
        self._http_lock = threading.Lock()
        self._folder_id: str | None = None
        self._file_id: str | None = None
        self._last_flush = 0.0
        self._dirty = False

    def append(self, line: str) -> None:
        with self._lock:
            self._lines.append(line)
            self._dirty = True

    def _find_or_create_folder_local(self, name: str, parent_id: str | None) -> str:
        escaped = name.replace("'", "\\'")
        q_parts = [
            "mimeType = 'application/vnd.google-apps.folder'",
            f"name = '{escaped}'",
            "trashed = false",
        ]
        if parent_id:
            q_parts.append(f"'{parent_id}' in parents")
        res = (
            self._service.files()
            .list(
                q=" and ".join(q_parts),
                fields="files(id, name)",
                pageSize=5,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = res.get("files") or []
        if files:
            return str(files[0]["id"])
        body: dict[str, Any] = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_id:
            body["parents"] = [parent_id]
        created = (
            self._service.files()
            .create(body=body, fields="id, name", supportsAllDrives=True)
            .execute()
        )
        return str(created["id"])

    def flush(self, *, force: bool = False) -> None:
        with self._lock:
            if not self._dirty:
                return
            now = time.monotonic()
            if not force and (now - self._last_flush) < self.flush_interval_seconds:
                return
            payload = ("\n".join(self._lines) + "\n").encode("utf-8")
            self._dirty = False
            self._last_flush = now

        with self._http_lock:
            try:
                if self._folder_id is None:
                    parent_id: str | None = None
                    for part in self.streamer_folder_parts:
                        parent_id = self._find_or_create_folder_local(part, parent_id)
                    self._folder_id = parent_id
                if self._file_id is None and self._folder_id is not None:
                    escaped = self.filename.replace("'", "\\'")
                    res = (
                        self._service.files()
                        .list(
                            q=f"name = '{escaped}' and '{self._folder_id}' in parents and trashed = false",
                            fields="files(id, name)",
                            pageSize=5,
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True,
                        )
                        .execute()
                    )
                    existing = res.get("files") or []
                    if existing and existing[0].get("id"):
                        self._file_id = str(existing[0]["id"])
                media = MediaInMemoryUpload(payload, mimetype="text/plain", resumable=False)
                if self._file_id is None:
                    created = (
                        self._service.files()
                        .create(
                            body={"name": self.filename, "parents": [self._folder_id]},
                            media_body=media,
                            fields="id",
                            supportsAllDrives=True,
                        )
                        .execute()
                    )
                    self._file_id = str(created["id"])
                else:
                    self._service.files().update(
                        fileId=self._file_id,
                        media_body=media,
                        fields="id",
                        supportsAllDrives=True,
                    ).execute()
            except Exception:
                # Never interfere with capture/upload if live status mirror fails transiently.
                pass


def probe_twitch_live(streamer: str, *, streamlink_bin: str = "streamlink") -> dict[str, Any]:
    url = f"https://www.twitch.tv/{streamer}"
    proc = subprocess.run(
        [streamlink_bin, "--json", url],
        capture_output=True,
        text=True,
        check=False,
        timeout=25,
    )
    payload: dict[str, Any] = {}
    try:
        payload = json.loads(proc.stdout) if proc.stdout.strip() else {}
    except json.JSONDecodeError:
        payload = {}

    streams = payload.get("streams")
    if isinstance(streams, dict) and streams:
        meta = payload.get("metadata") or {}
        return {
            "is_live": True,
            "qualities": list(streams.keys()),
            "title": str(meta.get("title") or ""),
            "category": str(meta.get("category") or ""),
            "transient_error": False,
        }

    err = str(payload.get("error") or proc.stderr or proc.stdout or f"rc={proc.returncode}").strip()
    lower = err.lower()
    offline_markers = ("no playable streams found", "channel does not exist", "is offline")
    return {
        "is_live": False,
        "error": err,
        "transient_error": not any(m in lower for m in offline_markers),
    }


def _drain_stderr(pipe: Any, label: str) -> None:
    last_txt = ""
    suppressed = 0
    try:
        for line in iter(pipe.readline, b""):
            txt = line.decode("utf-8", errors="replace").strip()
            if not txt:
                continue
            if txt == last_txt:
                suppressed += 1
                continue
            if suppressed > 0:
                log(f"[{label}] (mensagem anterior repetida {suppressed}x)")
                suppressed = 0
            last_txt = txt
            log(f"[{label}] {txt}")
        if suppressed > 0:
            log(f"[{label}] (mensagem anterior repetida {suppressed}x)")
    except Exception:
        pass
    finally:
        try:
            pipe.close()
        except Exception:
            pass


def run_worker(
    *,
    streamer: str,
    quality: str,
    segment_seconds: int,
    project_folder: str,
    drive_root: str,
    block_num: int,
    max_runtime_seconds: int,
    offline_wait_seconds: int,
    temp_dir: Path,
    drive_service: Any = None,
    credentials: Credentials | None = None,
    probe_fn: Callable[[str], dict[str, Any]] | None = None,
    producer_cmd_factory: Callable[[str, str], list[str]] | None = None,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
    streamlink_bin: str = "streamlink",
) -> int:
    norm_streamer = normalize_streamer(streamer)
    norm_project = sanitize_folder_name(project_folder)
    today_brt = datetime.now(BRT_TZ).strftime("%Y-%m-%d")
    drive_parts = [drive_root, norm_project, norm_streamer, "00_ORIGINAIS", today_brt]

    temp_dir.mkdir(parents=True, exist_ok=True)
    uploader = DriveUploader(drive_service, drive_parts, credentials=credentials)
    probe_impl = probe_fn or (lambda s: probe_twitch_live(s, streamlink_bin=streamlink_bin))

    global _ACTIVE_LOG_SINK
    log_sink = DriveLiveLogSink(
        uploader=uploader,
        streamer_folder_parts=[drive_root, norm_project, norm_streamer],
        filename=f"worker_live_bloco{block_num}.log",
    )
    _ACTIVE_LOG_SINK = log_sink

    log(f"Iniciando CI Stream Worker | streamer={norm_streamer} qualidade={quality} corte={segment_seconds}s bloco={block_num}")
    log(f"Destino Google Drive: {'/'.join(drive_parts)}")

    try:
        target_folder_id = uploader.ensure_target_folder()
        log(f"Pasta Drive pronta (folder_id={target_folder_id})")
        log_sink.flush(force=True)
    except Exception as exc:
        log(f"[ERRO DRIVE INIT] Falha ao preparar pasta no Google Drive: {exc}")
        _ACTIVE_LOG_SINK = None
        return 1

    stop_event = threading.Event()
    prev_term = None
    prev_int = None
    if threading.current_thread() is threading.main_thread():
        def _on_sig(signum: int, _frame: Any) -> None:
            log(f"Sinal recebido ({signum}): encerrando captura com flush dos blocos pendentes...")
            stop_event.set()

        prev_term = signal.getsignal(signal.SIGTERM)
        prev_int = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGTERM, _on_sig)
        signal.signal(signal.SIGINT, _on_sig)

    upload_queue: queue.Queue[tuple[Path, dict[str, Any]] | None] = queue.Queue()
    upload_failures: list[str] = []
    uploaded_count = {"n": 0}
    retry_buffer: deque[tuple[Path, dict[str, Any], float]] = deque()

    def _try_upload_item(mp4_path: Path, meta: dict[str, Any]) -> bool:
        try:
            log(
                f"[UPLOAD INICIADO] {mp4_path.name} "
                f"({meta['size_bytes'] / (1024 * 1024):.2f} MB | {meta['duration']}s)"
            )
            file_id = uploader.upload_and_verify(mp4_path, meta, max_retries=5)
            mp4_path.unlink(missing_ok=True)
            uploaded_count["n"] += 1
            log(f"[UPLOAD OK + LIMPEZA LOCAL] {mp4_path.name} -> Drive ID {file_id}")
            log_sink.flush(force=True)
            return True
        except Exception as exc:
            log(f"[UPLOAD FALHOU - REAGENDADO LOCALMENTE] {mp4_path.name}: {exc}")
            log_sink.flush(force=True)
            return False

    def _upload_worker() -> None:
        while True:
            try:
                item = upload_queue.get(timeout=15.0)
            except queue.Empty:
                now_t = time.monotonic()
                if retry_buffer and now_t >= retry_buffer[0][2]:
                    mp4_p, m_info, _due = retry_buffer.popleft()
                    if mp4_p.is_file() and not _try_upload_item(mp4_p, m_info):
                        retry_buffer.append((mp4_p, m_info, time.monotonic() + 60.0))
                continue

            if item is None:
                upload_queue.task_done()
                break
            mp4_path, meta = item
            try:
                if not _try_upload_item(mp4_path, meta):
                    retry_buffer.append((mp4_path, meta, time.monotonic() + 60.0))
            finally:
                upload_queue.task_done()

        # Final retry pass before ending block.
        while retry_buffer:
            mp4_p, m_info, _due = retry_buffer.popleft()
            if mp4_p.is_file() and not _try_upload_item(mp4_p, m_info):
                upload_failures.append(mp4_p.name)

    up_thread = threading.Thread(target=_upload_worker, name="drive-uploader", daemon=False)
    up_thread.start()

    start_mono = time.monotonic()
    global_part = 1
    attempt = 0
    offline_since: float | None = None

    try:
        while not stop_event.is_set():
            elapsed = time.monotonic() - start_mono
            if elapsed >= max_runtime_seconds:
                log(f"Tempo limite seguro do bloco atingido ({elapsed:.0f}s >= {max_runtime_seconds}s). Encerrando bloco...")
                break

            disk_free_mb = shutil.disk_usage(temp_dir).free / (1024 * 1024)
            if disk_free_mb < 1536 and upload_queue.qsize() > 0:
                log(f"[DISCO BAIXO] {disk_free_mb:.0f} MB livres; aguardando uploads esvaziarem buffer...")
                stop_event.wait(timeout=5.0)
                continue

            probe_res = probe_impl(norm_streamer)
            if not probe_res.get("is_live"):
                now = time.monotonic()
                if probe_res.get("transient_error"):
                    log(f"[PROBE AVISO] Erro transitório checando {norm_streamer}: {probe_res.get('error')}")
                    stop_event.wait(timeout=15.0)
                    continue
                if offline_since is None:
                    offline_since = now
                    log(f"[OFFLINE] Canal {norm_streamer} offline ({probe_res.get('error')}). Aguardando até {offline_wait_seconds}s...")
                if (now - offline_since) >= offline_wait_seconds:
                    log(f"[OFFLINE CONFIRMADO] {norm_streamer} permaneceu offline por {offline_wait_seconds}s. Encerrando.")
                    break
                stop_event.wait(timeout=15.0)
                continue

            offline_since = None
            attempt += 1
            log(
                f"[LIVE DETECTADA] {norm_streamer} ao vivo | título='{probe_res.get('title', '')}' | "
                f"categoria='{probe_res.get('category', '')}' | tentativa={attempt}"
            )

            attempt_dir = temp_dir / f"attempt_{attempt:03d}"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            seg_pattern = attempt_dir / f"raw_seg%05d.ts"

            if producer_cmd_factory is not None:
                stream_cmd = producer_cmd_factory(norm_streamer, quality)
            else:
                stream_cmd = [
                    streamlink_bin,
                    "--stdout",
                    "--retry-streams",
                    "30",
                    "--retry-max",
                    "20",
                    f"https://www.twitch.tv/{norm_streamer}",
                    quality,
                ]

            ffmpeg_cmd = [
                ffmpeg_bin,
                "-hide_banner",
                "-loglevel",
                "warning",
                "-y",
                "-i",
                "pipe:0",
                "-c",
                "copy",
                "-map",
                "0:v?",
                "-map",
                "0:a?",
                "-f",
                "segment",
                "-segment_time",
                str(segment_seconds),
                "-segment_format",
                "mpegts",
                "-reset_timestamps",
                "1",
                str(seg_pattern),
            ]

            sl_proc = subprocess.Popen(stream_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
            assert sl_proc.stdout is not None and sl_proc.stderr is not None
            ff_proc = subprocess.Popen(
                ffmpeg_cmd,
                stdin=sl_proc.stdout,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            sl_proc.stdout.close()
            assert ff_proc.stderr is not None

            sl_log_th = threading.Thread(target=_drain_stderr, args=(sl_proc.stderr, "streamlink"), daemon=True)
            ff_log_th = threading.Thread(target=_drain_stderr, args=(ff_proc.stderr, "ffmpeg"), daemon=True)
            sl_log_th.start()
            ff_log_th.start()

            finalized_ts: set[Path] = set()

            def _process_ts(flush_all: bool) -> None:
                nonlocal global_part
                ts_files: list[tuple[int, Path]] = []
                for p in attempt_dir.glob("raw_seg*.ts"):
                    m = TS_SEG_RE.search(p.name)
                    if m:
                        ts_files.append((int(m.group(1)), p))
                ts_files.sort(key=lambda x: x[0])
                if not ts_files:
                    return
                for idx, ts_p in ts_files:
                    if ts_p in finalized_ts:
                        continue
                    has_next = any(n_idx > idx and n_p.exists() and n_p.stat().st_size > 0 for n_idx, n_p in ts_files)
                    if not (has_next or flush_all):
                        continue
                    finalized_ts.add(ts_p)
                    if not ts_p.is_file() or ts_p.stat().st_size <= 0:
                        ts_p.unlink(missing_ok=True)
                        continue
                    stamp = datetime.now(BRT_TZ).strftime("%Y%m%d_%H%M%S")
                    mp4_name = f"{norm_streamer}_{stamp}_bloco{block_num}_part{global_part:04d}.mp4"
                    mp4_path = attempt_dir / mp4_name
                    try:
                        meta = remux_ts_to_mp4(
                            ts_p, mp4_path, ffmpeg_bin=ffmpeg_bin, ffprobe_bin=ffprobe_bin
                        )
                        ts_p.unlink(missing_ok=True)
                        log(
                            f"[CORTE PRONTO] {mp4_name} | dur={meta['duration']}s | "
                            f"video={meta['has_video']} audio={meta['has_audio']}"
                        )
                        log_sink.flush(force=True)
                        upload_queue.put((mp4_path, meta))
                        global_part += 1
                    except Exception as exc:
                        log(f"[REMUX ERRO] {ts_p.name}: {exc}")

            last_heartbeat = time.monotonic()
            try:
                while True:
                    _process_ts(flush_all=False)
                    now_hb = time.monotonic()
                    if (now_hb - last_heartbeat) >= 30.0:
                        free_gb = shutil.disk_usage(temp_dir).free / (1024 * 1024 * 1024)
                        log(
                            f"[HEARTBEAT] bloco={block_num} ativo={int(now_hb - start_mono)}s | "
                            f"uploads_ok={uploaded_count['n']} | fila={upload_queue.qsize()} | "
                            f"disco_livre={free_gb:.2f}GB"
                        )
                        last_heartbeat = now_hb
                    log_sink.flush(force=False)
                    if stop_event.is_set() or (time.monotonic() - start_mono) >= max_runtime_seconds:
                        break
                    if shutil.disk_usage(temp_dir).free < 1024 * 1024 * 1024:
                        log("[DISCO CRÍTICO] Menos de 1 GB livre na VM; pausando captura para preservar cortes pendentes.")
                        break
                    sl_rc = sl_proc.poll()
                    ff_rc = ff_proc.poll()
                    if sl_rc is not None or ff_rc is not None:
                        if sl_rc is not None and ff_rc is None:
                            try:
                                ff_proc.wait(timeout=20)
                            except subprocess.TimeoutExpired:
                                ff_proc.terminate()
                        if ff_rc is not None and sl_rc is None:
                            sl_proc.terminate()
                        break
                    time.sleep(1.0)
            finally:
                if sl_proc.poll() is None:
                    sl_proc.terminate()
                    try:
                        sl_proc.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        sl_proc.kill()
                        sl_proc.wait(timeout=3)
                if ff_proc.poll() is None:
                    try:
                        ff_proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        ff_proc.terminate()
                        try:
                            ff_proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            ff_proc.kill()
                            ff_proc.wait(timeout=3)
                sl_log_th.join(timeout=2)
                ff_log_th.join(timeout=2)
                _process_ts(flush_all=True)

            if stop_event.is_set() or (time.monotonic() - start_mono) >= max_runtime_seconds:
                break

            log("[RECONEXÃO] Processo encerrado antes do limite do bloco; verificando novamente em 15s...")
            stop_event.wait(timeout=15.0)
    finally:
        if prev_term is not None and threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, prev_term)
        if prev_int is not None and threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, prev_int)

        log("Aguardando conclusão de todos os uploads restantes para o Google Drive...")
        upload_queue.put(None)
        up_thread.join()

        log(
            f"Bloco {block_num} finalizado | uploads_confirmados={uploaded_count['n']} | "
            f"falhas_upload={len(upload_failures)}"
        )
        log_sink.flush(force=True)
        _ACTIVE_LOG_SINK = None

    return 1 if upload_failures else 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Cloud Twitch live capture & Drive uploader")
    p.add_argument("--streamer", required=True)
    p.add_argument("--quality", default="best")
    p.add_argument("--segment-seconds", type=int, default=900)
    p.add_argument("--project-folder", default="TWITCH_LIVES")
    p.add_argument("--drive-root", default="Youtube-Farm-Clips")
    p.add_argument("--block", type=int, default=1)
    p.add_argument("--max-runtime-seconds", type=int, default=20880)
    p.add_argument("--offline-wait-seconds", type=int, default=180)
    p.add_argument("--temp-dir", default="/tmp/ci_capture")
    p.add_argument("--token-env", default="GDRIVE_TOKEN_JSON_B64")
    p.add_argument("--token-file", default="")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    token_file = Path(args.token_file) if args.token_file else None
    creds = load_drive_credentials_from_env(
        token_env=args.token_env,
        token_file=token_file,
    )
    return run_worker(
        streamer=args.streamer,
        quality=args.quality,
        segment_seconds=args.segment_seconds,
        project_folder=args.project_folder,
        drive_root=args.drive_root,
        block_num=args.block,
        max_runtime_seconds=args.max_runtime_seconds,
        offline_wait_seconds=args.offline_wait_seconds,
        temp_dir=Path(args.temp_dir),
        credentials=creds,
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
