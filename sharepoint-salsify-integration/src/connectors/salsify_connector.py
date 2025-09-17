from __future__ import annotations

import io
from typing import Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..utils import get_logger


class SalsifyConnector:
    """Connector for Salsify Assets and Product association APIs.

    Note: Salsify API authentication can use API key header or Bearer token depending on org setup.
    """

    def __init__(
        self,
        base_url: str,
        org_id: str,
        api_key: str,
        auth_scheme: str = "Bearer",
        logger_name: str = "salsify",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.org_id = org_id
        self.api_key = api_key
        self.auth_scheme = auth_scheme
        self.logger = get_logger(logger_name)
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _headers(self) -> Dict[str, str]:
        if self.auth_scheme.lower() == "x-api-key":
            return {"X-API-KEY": self.api_key}
        return {"Authorization": f"Bearer {self.api_key}"}

    def upload_asset(self, file_stream: io.BufferedReader, filename: str, content_type: Optional[str] = None) -> Dict:
        url = f"{self.base_url}/v1/orgs/{self.org_id}/assets"
        headers = self._headers()
        files = {"file": (filename, file_stream, content_type or "application/octet-stream")}
        resp = self.session.post(url, headers=headers, files=files, timeout=120)
        resp.raise_for_status()
        return resp.json()

    def check_duplicate(self, product_code: str) -> bool:
        # Placeholder duplicate check; often handled via product assets listing or external tracking
        return False

    def update_product_association(self, product_code: str, asset_id: str) -> None:
        # Depending on Salsify setup, association may be done via product PATCH linking asset IDs
        # This is a placeholder to be tailored to the specific Salsify data model
        return None

