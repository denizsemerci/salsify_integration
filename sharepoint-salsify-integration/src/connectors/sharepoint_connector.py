from __future__ import annotations

import io
from typing import Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..auth import AzureAuthenticator
from ..utils import get_logger


class SharePointConnector:
    """Connector for SharePoint via Microsoft Graph API."""

    GRAPH_BASE = "https://graph.microsoft.com/v1.0"

    def __init__(
        self,
        authenticator: AzureAuthenticator,
        site_id: str,
        folder_path: str,
        logger_name: str = "sharepoint",
    ) -> None:
        self.authenticator = authenticator
        self.site_id = site_id
        self.folder_path = folder_path.strip("/")
        self.logger = get_logger(logger_name)
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _headers(self) -> Dict[str, str]:
        token = self.authenticator.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    def _resolve_drive_and_item(self) -> Dict[str, str]:
        # Resolve the drive (default document library) and folder item id
        headers = self._headers()
        # Get default drive
        drive_url = f"{self.GRAPH_BASE}/sites/{self.site_id}/drive"
        resp = self.session.get(drive_url, headers=headers, timeout=30)
        resp.raise_for_status()
        drive_id = resp.json()["id"]

        # Resolve folder by path
        path_url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{self.folder_path}"
        resp2 = self.session.get(path_url, headers=headers, timeout=30)
        resp2.raise_for_status()
        item_id = resp2.json()["id"]
        return {"drive_id": drive_id, "item_id": item_id}

    def list_new_files(self, allowed_extensions: Optional[Iterable[str]] = None) -> List[Dict]:
        if allowed_extensions is None:
            allowed_extensions = {".tif", ".tiff", ".jpg", ".jpeg", ".png"}

        ids = self._resolve_drive_and_item()
        headers = self._headers()
        # List children of folder
        children_url = f"{self.GRAPH_BASE}/drives/{ids['drive_id']}/items/{ids['item_id']}/children?$select=id,name,file,createdDateTime,lastModifiedDateTime,size"
        resp = self.session.get(children_url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("value", [])
        files: List[Dict] = []
        for item in items:
            if "file" not in item:
                continue
            name: str = item.get("name", "")
            lower = name.lower()
            if not any(lower.endswith(ext) for ext in allowed_extensions):
                continue
            files.append(item)
        return files

    def download_file_stream(self, drive_id: str, item_id: str) -> io.BufferedReader:
        headers = self._headers()
        content_url = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"
        resp = self.session.get(content_url, headers=headers, stream=True, timeout=60)
        resp.raise_for_status()
        return resp.raw  # type: ignore[return-value]

    def get_file_metadata(self, drive_id: str, item_id: str) -> Dict:
        headers = self._headers()
        meta_url = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
        resp = self.session.get(meta_url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

