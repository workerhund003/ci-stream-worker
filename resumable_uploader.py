"""RAM-buffered Google Drive v3 Resumable Uploader for non-seekable streams."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import random
import re
import time
from typing import Any, BinaryIO, Callable

from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials
import requests

DRIVE_RESUMABLE_UPLOAD_URL = (
    "https://www.googleapis.com/upload/drive/v3/files"
    "?uploadType=resumable&supportsAllDrives=true"
    "&fields=id,name,mimeType,size,md5Checksum,webViewLink,appProperties"
)
CHUNK_GRANULARITY = 262_144  # 256 KiB required by Google Drive resumable protocol
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB RAM buffer
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
RANGE_HEADER_RE = re.compile(r"bytes=0-(\d+)")


class ResumableUploadError(RuntimeError):
    """Raised when a streaming resumable upload fails or is aborted."""


@dataclass(frozen=True)
class UploadResult:
    """Metadata and checksum verified after streaming upload completion."""

    file_metadata: dict[str, Any]
    bytes_uploaded: int
    md5_hex: str


def _parse_acked_bytes(range_header: str | None) -> int:
    """Return total acknowledged bytes from a Drive 308 Range header."""
    if not range_header:
        return 0
    match = RANGE_HEADER_RE.search(range_header)
    if not match:
        return 0
    return int(match.group(1)) + 1


def _read_chunk_with_eof_peek(
    stream: BinaryIO,
    chunk_size: int,
    carry: bytes,
) -> tuple[bytes, bytes, bool]:
    """Read up to chunk_size bytes from a non-seekable stream and detect EOF safely."""
    buf = bytearray(carry)
    while len(buf) < chunk_size:
        piece = stream.read(chunk_size - len(buf))
        if not piece:
            return bytes(buf), b"", True
        buf.extend(piece)

    peek = stream.read(1)
    if not peek:
        return bytes(buf), b"", True
    return bytes(buf), peek, False


def _refresh_credentials_if_needed(creds: Credentials) -> None:
    """Refresh OAuth credentials when expired before/during long uploads."""
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())


def start_resumable_session(
    session: AuthorizedSession,
    name: str,
    folder_id: str,
    mime_type: str,
    app_properties: dict[str, str] | None = None,
    timeout_s: float = 30.0,
) -> str:
    """Initialize a Google Drive v3 resumable upload session and return session URI."""
    body: dict[str, Any] = {
        "name": name,
        "parents": [folder_id],
    }
    if app_properties:
        body["appProperties"] = app_properties

    headers = {
        "X-Upload-Content-Type": mime_type,
        "Content-Type": "application/json; charset=UTF-8",
    }
    response = session.post(
        DRIVE_RESUMABLE_UPLOAD_URL,
        json=body,
        headers=headers,
        timeout=timeout_s,
    )
    if response.status_code not in (200, 201):
        raise ResumableUploadError(
            f"Falha ao iniciar sessao resumable no Drive (HTTP {response.status_code})."
        )
    location = response.headers.get("Location")
    if not location:
        raise ResumableUploadError("Google Drive nao retornou cabecalho Location da sessao.")
    return location


def cancel_resumable_session(
    session: AuthorizedSession,
    session_uri: str,
    timeout_s: float = 15.0,
) -> None:
    """Cancel an incomplete resumable upload session on Google Drive."""
    try:
        session.delete(session_uri, timeout=timeout_s)
    except requests.RequestException:
        pass


def _query_session_status(
    session: AuthorizedSession,
    session_uri: str,
    total_size: int | None,
    timeout_s: float = 30.0,
) -> requests.Response:
    """Query Google Drive for bytes received so far in an interrupted session."""
    total_token = str(total_size) if total_size is not None else "*"
    return session.put(
        session_uri,
        data=b"",
        headers={
            "Content-Length": "0",
            "Content-Range": f"bytes */{total_token}",
        },
        timeout=timeout_s,
    )


def _upload_chunk_with_retries(
    session: AuthorizedSession,
    creds: Credentials,
    session_uri: str,
    chunk: bytes,
    offset: int,
    is_final: bool,
    mime_type: str,
    max_retries: int,
    timeout_s: float,
) -> dict[str, Any] | None:
    """Upload one chunk from RAM, recovering byte offset via 308 status on failures."""
    chunk_start = offset
    chunk_len = len(chunk)
    total_size = (chunk_start + chunk_len) if is_final else None
    total_token = str(total_size) if total_size is not None else "*"

    current_start = chunk_start
    payload_view = memoryview(chunk)

    for attempt in range(max_retries + 1):
        sub_view = payload_view[current_start - chunk_start :]
        sub_len = len(sub_view)
        end_byte = current_start + sub_len - 1

        headers = {
            "Content-Type": mime_type,
            "Content-Length": str(sub_len),
            "Content-Range": f"bytes {current_start}-{end_byte}/{total_token}",
        }

        try:
            _refresh_credentials_if_needed(creds)
            response = session.put(
                session_uri,
                data=sub_view,
                headers=headers,
                timeout=timeout_s,
            )
            status = response.status_code

            if not is_final and status == 308:
                acked = _parse_acked_bytes(response.headers.get("Range"))
                expected_end = chunk_start + chunk_len
                if acked >= expected_end:
                    return None
                if acked > current_start:
                    current_start = acked
                    continue
                return None

            if is_final and status in (200, 201):
                return response.json()

            if status == 401 and attempt < max_retries and creds.refresh_token:
                creds.refresh(Request())
                continue

            if status in RETRYABLE_STATUS_CODES and attempt < max_retries:
                pass
            elif status in (404, 410):
                raise ResumableUploadError(
                    f"Sessao resumable expirada ou invalida no Drive (HTTP {status})."
                )
            else:
                raise ResumableUploadError(
                    f"Erro inesperado no upload resumable (HTTP {status})."
                )

        except requests.RequestException as exc:
            if attempt >= max_retries:
                raise ResumableUploadError(
                    f"Falha de conexao persistente durante upload no Drive: {type(exc).__name__}"
                ) from exc

        delay = min(30.0, float(2**attempt)) + random.uniform(0.0, 0.5)
        time.sleep(delay)

        try:
            status_resp = _query_session_status(
                session=session,
                session_uri=session_uri,
                total_size=total_size,
                timeout_s=timeout_s,
            )
            if status_resp.status_code in (200, 201):
                return status_resp.json()
            if status_resp.status_code == 308:
                acked = _parse_acked_bytes(status_resp.headers.get("Range"))
                expected_end = chunk_start + chunk_len
                if acked >= expected_end:
                    return None if not is_final else status_resp.json()
                if acked > current_start:
                    current_start = acked
                elif acked < chunk_start:
                    raise ResumableUploadError(
                        "Dessincronizacao de offset no upload resumable."
                    )
            elif status_resp.status_code in (404, 410):
                raise ResumableUploadError(
                    f"Sessao resumable expirada durante recuperacao (HTTP {status_resp.status_code})."
                )
        except requests.RequestException:
            # Keep trying with current offset if status probe also failed transiently
            pass

    raise ResumableUploadError("Numero maximo de tentativas excedido no upload resumable.")


def stream_to_drive(
    creds: Credentials,
    stream: BinaryIO,
    name: str,
    folder_id: str,
    mime_type: str,
    app_properties: dict[str, str] | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_retries: int = 5,
    timeout_s: float = 120.0,
    on_progress: Callable[[int], None] | None = None,
    before_finalize: Callable[[], None] | None = None,
) -> UploadResult:
    """Stream bytes directly from a non-seekable stream to Google Drive in RAM chunks."""
    if chunk_size <= 0 or chunk_size % CHUNK_GRANULARITY != 0:
        raise ValueError(f"chunk_size deve ser multiplo positivo de {CHUNK_GRANULARITY} bytes.")

    _refresh_credentials_if_needed(creds)
    session = AuthorizedSession(creds)
    session_uri = start_resumable_session(
        session=session,
        name=name,
        folder_id=folder_id,
        mime_type=mime_type,
        app_properties=app_properties,
    )

    hasher = hashlib.md5()
    offset = 0
    carry = b""
    final_metadata: dict[str, Any] | None = None
    completed = False

    try:
        while True:
            chunk, carry, is_eof = _read_chunk_with_eof_peek(stream, chunk_size, carry)
            if not chunk:
                if offset == 0:
                    raise ResumableUploadError("Fluxo de entrada vazio (0 bytes); upload abortado.")
                raise ResumableUploadError("EOF inesperado sem dados finais para finalizar upload.")

            hasher.update(chunk)

            if is_eof and before_finalize is not None:
                before_finalize()

            result = _upload_chunk_with_retries(
                session=session,
                creds=creds,
                session_uri=session_uri,
                chunk=chunk,
                offset=offset,
                is_final=is_eof,
                mime_type=mime_type,
                max_retries=max_retries,
                timeout_s=timeout_s,
            )

            offset += len(chunk)
            if on_progress is not None:
                on_progress(offset)

            if is_eof:
                if not result or "id" not in result:
                    raise ResumableUploadError(
                        "Upload finalizado sem confirmacao de metadados pelo Google Drive."
                    )
                final_metadata = result
                completed = True
                break

        assert final_metadata is not None
        return UploadResult(
            file_metadata=final_metadata,
            bytes_uploaded=offset,
            md5_hex=hasher.hexdigest(),
        )
    finally:
        if not completed:
            cancel_resumable_session(session, session_uri)
        session.close()
