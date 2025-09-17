from __future__ import annotations

import threading
import time
from typing import Dict, Optional

import msal


class AzureAuthenticator:
    """
    Authenticates against Azure AD using OAuth2 Client Credentials via MSAL.

    Caches tokens in-memory and refreshes them proactively. Thread-safe.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        scopes: Optional[list[str]] = None,
        authority_host: str = "https://login.microsoftonline.com",
        token_refresh_margin_seconds: int = 60,
    ) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.authority = f"{authority_host}/{tenant_id}"
        self.scopes = scopes or ["https://graph.microsoft.com/.default"]
        self.token_refresh_margin_seconds = token_refresh_margin_seconds

        self._app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=self.authority,
        )
        self._cached_token: Optional[Dict[str, str]] = None
        self._token_expiry_epoch: float = 0.0
        self._lock = threading.Lock()

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        with self._lock:
            now = time.time()
            if (
                self._cached_token
                and now < self._token_expiry_epoch - self.token_refresh_margin_seconds
            ):
                return self._cached_token["access_token"]

            # Try acquire from cache (MSAL internal cache)
            accounts = self._app.get_accounts()
            if accounts:
                result = self._app.acquire_token_silent(scopes=self.scopes, account=accounts[0])
            else:
                result = self._app.acquire_token_silent(scopes=self.scopes, account=None)

            if not result:
                result = self._app.acquire_token_for_client(scopes=self.scopes)

            if "access_token" not in result:
                error_desc = result.get("error_description") if isinstance(result, dict) else None
                raise RuntimeError(f"Failed to acquire token from Azure AD: {error_desc}")

            self._cached_token = result
            expires_in = float(result.get("expires_in", 3000))
            self._token_expiry_epoch = now + expires_in
            return result["access_token"]

