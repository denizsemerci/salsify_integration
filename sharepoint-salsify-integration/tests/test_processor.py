from __future__ import annotations

from pathlib import Path

from src.processors.file_processor import FileProcessor


class DummySP:
    def download_file_stream(self, drive_id, item_id):  # noqa: D401
        from io import BytesIO

        return BytesIO(b"data")


class DummySalsify:
    def upload_asset(self, file_stream, filename, content_type=None):  # noqa: D401
        return {"id": "asset123"}

    def update_product_association(self, product_code, asset_id):  # noqa: D401
        return None


def test_extract_product_code():
    fp = FileProcessor(DummySP(), DummySalsify(), Path("/tmp/processed_files.json"))
    code, typ, ver = fp.extract_product_code("ABC123_FRONT_01.jpg")
    assert code == "ABC123"
    assert typ == "FRONT"
    assert ver == "01"

