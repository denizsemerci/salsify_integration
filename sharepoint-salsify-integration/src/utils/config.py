from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class Settings(BaseModel):
    # Azure AD / Graph
    tenant_id: str = Field(alias="TENANT_ID")
    client_id: str = Field(alias="CLIENT_ID")
    client_secret: str = Field(alias="CLIENT_SECRET")
    graph_scopes: list[str] = ["https://graph.microsoft.com/.default"]

    # SharePoint
    site_id: str = Field(alias="SITE_ID")
    sharepoint_folder_path: str = Field("Shared Documents/SalsifyImages", alias="SHAREPOINT_FOLDER_PATH")

    # Salsify
    salsify_api_key: str = Field(alias="SALSIFY_API_KEY")
    salsify_org_id: str = Field(alias="SALSIFY_ORG_ID")
    salsify_base_url: str = Field(default="https://api.salsify.com", alias="SALSIFY_BASE_URL")
    salsify_auth_scheme: str = Field(default="Bearer", alias="SALSIFY_AUTH_SCHEME")  # or X-API-KEY
    salsify_association_mode: str = Field(default="none", alias="SALSIFY_PRODUCT_ASSOCIATION_MODE")

    # Service
    poll_interval: int = Field(300, alias="POLL_INTERVAL")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    max_retries: int = Field(3, alias="MAX_RETRIES")
    batch_size: int = Field(10, alias="BATCH_SIZE")
    circuit_threshold: int = Field(5, alias="CIRCUIT_THRESHOLD")
    circuit_reset_seconds: int = Field(60, alias="CIRCUIT_RESET_SECONDS")

    # Paths
    processed_files_path: str = Field(default=str(Path(__file__).resolve().parents[2] / "data" / "processed_files.json"))
    dead_letter_path: str = Field(default=str(Path(__file__).resolve().parents[2] / "data" / "dead_letter.jsonl"))

    class Config:
        populate_by_name = True


def _read_yaml_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_settings(project_root: Optional[Path] = None) -> Settings:
    """Load environment variables and YAML settings into a Settings object."""
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]

    # Load .env if present
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    # Load YAML
    yaml_path = project_root / "config" / "settings.yaml"
    yaml_data = _read_yaml_config(yaml_path)

    # Environment overrides YAML via pydantic model aliases
    return Settings(**yaml_data)

