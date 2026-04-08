"""
Dropbox client — downloads the ledger file to a local cache.

Reads DROPBOX_ACCESS_TOKEN and LEDGER_DROPBOX_PATH from environment (.env).
Caches the file in .tmp/ next to the project root, and only re-downloads
when the remote file's server_modified timestamp is newer than the local
cache's mtime.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import dropbox
from dropbox.exceptions import ApiError, AuthError


class DropboxError(RuntimeError):
    pass


def download_ledger(
    dropbox_path: str | None = None,
    cache_dir: Path | None = None,
    token: str | None = None,
) -> Path:
    """Download the ledger from Dropbox into the local cache and return its path.

    Re-downloads only if the remote server_modified is newer than the local mtime.
    """
    token = token or os.environ.get("DROPBOX_ACCESS_TOKEN")
    dropbox_path = dropbox_path or os.environ.get("LEDGER_DROPBOX_PATH")

    if not token:
        raise DropboxError(
            "DROPBOX_ACCESS_TOKEN não definido. Configure o .env ou passe --local <arquivo>."
        )
    if not dropbox_path:
        raise DropboxError(
            "LEDGER_DROPBOX_PATH não definido. Configure o .env ou passe --local <arquivo>."
        )
    if not dropbox_path.startswith("/"):
        dropbox_path = "/" + dropbox_path

    cache_dir = cache_dir or (Path(__file__).resolve().parent.parent / ".tmp")
    cache_dir.mkdir(parents=True, exist_ok=True)
    local_path = cache_dir / Path(dropbox_path).name

    try:
        dbx = dropbox.Dropbox(token)
        meta = dbx.files_get_metadata(dropbox_path)
    except AuthError as e:
        raise DropboxError(f"Token Dropbox inválido: {e}") from e
    except ApiError as e:
        raise DropboxError(f"Erro ao acessar '{dropbox_path}' no Dropbox: {e}") from e

    remote_mtime = getattr(meta, "server_modified", None)
    if local_path.exists() and remote_mtime is not None:
        local_mtime = datetime.fromtimestamp(local_path.stat().st_mtime, tz=timezone.utc)
        if remote_mtime.replace(tzinfo=timezone.utc) <= local_mtime:
            return local_path  # cache is fresh

    try:
        dbx.files_download_to_file(str(local_path), dropbox_path)
    except ApiError as e:
        raise DropboxError(f"Falha ao baixar '{dropbox_path}': {e}") from e

    return local_path


def _client(token: str | None = None) -> "dropbox.Dropbox":
    token = token or os.environ.get("DROPBOX_ACCESS_TOKEN")
    if not token:
        raise DropboxError("DROPBOX_ACCESS_TOKEN não definido no .env.")
    return dropbox.Dropbox(token)


def list_xlsx_in_folder(folder_path: str = "", token: str | None = None) -> list[dict]:
    """List .xlsx files in a Dropbox folder. Returns [{path, name, modified}, ...]."""
    if folder_path and not folder_path.startswith("/"):
        folder_path = "/" + folder_path
    if folder_path == "/":
        folder_path = ""
    try:
        dbx = _client(token)
        res = dbx.files_list_folder(folder_path, recursive=False)
    except AuthError as e:
        raise DropboxError(f"Token Dropbox inválido: {e}") from e
    except ApiError as e:
        raise DropboxError(f"Erro ao listar '{folder_path or '/'}': {e}") from e

    out = []
    for entry in res.entries:
        if isinstance(entry, dropbox.files.FileMetadata) and entry.name.lower().endswith(".xlsx"):
            out.append({
                "path": entry.path_display,
                "name": entry.name,
                "modified": entry.server_modified,
            })
    out.sort(key=lambda x: x["modified"] or datetime.min, reverse=True)
    return out


def download_path(dropbox_path: str, cache_dir: Path | None = None, token: str | None = None) -> Path:
    """Download a specific Dropbox file path into the local cache and return its Path."""
    if not dropbox_path.startswith("/"):
        dropbox_path = "/" + dropbox_path
    cache_dir = cache_dir or (Path(__file__).resolve().parent.parent / ".tmp")
    cache_dir.mkdir(parents=True, exist_ok=True)
    local_path = cache_dir / Path(dropbox_path).name
    try:
        dbx = _client(token)
        meta = dbx.files_get_metadata(dropbox_path)
        remote_mtime = getattr(meta, "server_modified", None)
        if local_path.exists() and remote_mtime is not None:
            local_mtime = datetime.fromtimestamp(local_path.stat().st_mtime, tz=timezone.utc)
            if remote_mtime.replace(tzinfo=timezone.utc) <= local_mtime:
                return local_path
        dbx.files_download_to_file(str(local_path), dropbox_path)
    except AuthError as e:
        raise DropboxError(f"Token Dropbox inválido: {e}") from e
    except ApiError as e:
        raise DropboxError(f"Falha ao baixar '{dropbox_path}': {e}") from e
    return local_path
