from __future__ import annotations

import types

from src.auth.azure_auth import AzureAuthenticator
import msal


class DummyApp:
    def __init__(self) -> None:
        self.calls = 0

    def get_accounts(self):  # noqa: D401
        return []

    def acquire_token_silent(self, scopes, account=None):  # noqa: D401
        return None

    def acquire_token_for_client(self, scopes):  # noqa: D401
        self.calls += 1
        return {"access_token": "test", "expires_in": 3600}


def test_get_access_token_monkeypatch(monkeypatch):
    # Monkeypatch MSAL class to prevent real network calls during init
    monkeypatch.setattr(msal, "ConfidentialClientApplication", lambda **kwargs: DummyApp())

    auth = AzureAuthenticator("tenant", "client", "secret")
    dummy = auth._app  # type: ignore[attr-defined]

    token = auth.get_access_token()
    assert token == "test"
    assert dummy.calls == 1

