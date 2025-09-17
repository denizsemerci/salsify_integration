from __future__ import annotations

import io

import pytest

from src.connectors.salsify_connector import SalsifyConnector


def test_salsify_headers_bearer():
    c = SalsifyConnector(base_url="https://api.salsify.com", org_id="o", api_key="k", auth_scheme="Bearer")
    assert c._headers()["Authorization"] == "Bearer k"


def test_salsify_headers_x_api_key():
    c = SalsifyConnector(base_url="https://api.salsify.com", org_id="o", api_key="k", auth_scheme="X-API-KEY")
    assert c._headers()["X-API-KEY"] == "k"

