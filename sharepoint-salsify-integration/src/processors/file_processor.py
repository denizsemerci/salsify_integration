from __future__ import annotations

import io
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from ..connectors import SalsifyConnector, SharePointConnector
from ..utils import get_logger


class FileProcessor:
    """Orchestrates file validation, parsing and transfer from SharePoint to Salsify."""

    IMAGE_EXTENSIONS = {".tif", ".tiff", ".jpg", ".jpeg", ".png"}

    def __init__(
        self,
        sp_connector: SharePointConnector,
        salsify_connector: SalsifyConnector,
        processed_files_path: Path,
    ) -> None:
        self.sp_connector = sp_connector
        self.salsify_connector = salsify_connector
        self.processed_files_path = processed_files_path
        self.logger = get_logger("processor")
        self._load_processed()

    def _load_processed(self) -> None:
        self.processed: Dict[str, bool] = {}
        try:
            if self.processed_files_path.exists():
                data = json.loads(self.processed_files_path.read_text(encoding="utf-8") or "[]")
                if isinstance(data, list):
                    self.processed = {str(x): True for x in data}
        except Exception:
            self.processed = {}

    def _save_processed(self) -> None:
        try:
            items = sorted(self.processed.keys())
            self.processed_files_path.parent.mkdir(parents=True, exist_ok=True)
            self.processed_files_path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def extract_product_code(self, filename: str) -> Tuple[str, str, str]:
        # Expected: PRODUCTCODE_TYPE_VERSION.ext
        name = Path(filename).stem
        parts = name.split("_")
        if len(parts) < 3:
            raise ValueError("Filename must be PRODUCTCODE_TYPE_VERSION.ext")
        return parts[0], parts[1], parts[2]

    def validate_file(self, item: Dict) -> bool:
        name = item.get("name", "")
        suffix = Path(name.lower()).suffix
        return suffix in self.IMAGE_EXTENSIONS

    def process_file(self, drive_id: str, item: Dict) -> Dict:
        item_id = item["id"]
        name = item.get("name", item_id)
        if name in self.processed:
            return {"status": "skipped", "reason": "already_processed", "name": name}

        if not self.validate_file(item):
            return {"status": "skipped", "reason": "invalid_extension", "name": name}

        product_code, image_type, version = self.extract_product_code(name)

        stream = self.sp_connector.download_file_stream(drive_id=drive_id, item_id=item_id)
        result = self.salsify_connector.upload_asset(stream, filename=name)
        asset_id = result.get("id") or result.get("asset_id") or ""

        # Optionally associate asset to product
        try:
            self.salsify_connector.update_product_association(product_code, asset_id)
        except Exception:
            pass

        self.processed[name] = True
        self._save_processed()
        return {"status": "success", "asset_id": asset_id, "name": name, "product_code": product_code}

