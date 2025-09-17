from __future__ import annotations

import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
from typing import List

from fastapi import FastAPI
from prometheus_client import Counter, Histogram, make_asgi_app
import uvicorn

from .auth import AzureAuthenticator
from .connectors import SalsifyConnector, SharePointConnector
from .processors import FileProcessor
from .utils import Settings, get_logger, load_settings


FILES_PROCESSED = Counter("files_processed_total", "Total files processed")
FILES_SUCCEEDED = Counter("files_succeeded_total", "Total files succeeded")
FILES_FAILED = Counter("files_failed_total", "Total files failed")
PROCESSING_SECONDS = Histogram("file_processing_seconds", "Time to process a file")


app = FastAPI()
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


shutdown_event = threading.Event()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


def run_daemon(settings: Settings) -> None:
    logger = get_logger("daemon", level=settings.log_level)
    logger.info("Service starting", extra={"extra": {"poll_interval": settings.poll_interval}})

    authenticator = AzureAuthenticator(
        tenant_id=settings.tenant_id,
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        scopes=settings.graph_scopes,
    )

    sp = SharePointConnector(
        authenticator=authenticator,
        site_id=settings.site_id,
        folder_path=settings.sharepoint_folder_path,
    )
    salsify = SalsifyConnector(
        base_url=settings.salsify_base_url,
        org_id=settings.salsify_org_id,
        api_key=settings.salsify_api_key,
        auth_scheme=settings.salsify_auth_scheme,
    )

    processor = FileProcessor(
        sp_connector=sp,
        salsify_connector=salsify,
        processed_files_path=Path(settings.processed_files_path),
    )

    failure_count = 0
    circuit_open_until = 0.0

    with ThreadPoolExecutor(max_workers=settings.batch_size) as executor:
        while not shutdown_event.is_set():
            try:
                now = time.time()
                if failure_count >= settings.circuit_threshold and now < circuit_open_until:
                    time_to_reset = int(circuit_open_until - now)
                    logger.warning("Circuit open, skipping poll", extra={"extra": {"retry_in_seconds": time_to_reset}})
                    shutdown_event.wait(timeout=min(settings.poll_interval, time_to_reset))
                    continue

                # Resolve drive and list files
                ids = sp._resolve_drive_and_item()
                drive_id = ids["drive_id"]
                files = sp.list_new_files()
                if not files:
                    logger.info("No new files found")
                else:
                    futures = []
                    for item in files[: settings.batch_size]:
                        futures.append(executor.submit(processor.process_file, drive_id, item))

                    for f in as_completed(futures):
                        FILES_PROCESSED.inc()
                        try:
                            res = f.result()
                            if res.get("status") == "success":
                                FILES_SUCCEEDED.inc()
                                logger.info("Processed file", extra={"extra": res})
                                failure_count = 0
                            else:
                                logger.info("Skipped file", extra={"extra": res})
                        except Exception as ex:  # noqa: BLE001
                            FILES_FAILED.inc()
                            logger.exception("File processing failed: %s", ex)
                            # Dead-letter this file failure
                            try:
                                dl = Path(settings.dead_letter_path)
                                dl.parent.mkdir(parents=True, exist_ok=True)
                                with dl.open("a", encoding="utf-8") as fh:
                                    fh.write(json.dumps({"error": str(ex), "time": time.time()}) + "\n")
                            except Exception:
                                pass
                            failure_count += 1

            except Exception as ex:  # noqa: BLE001
                logger.exception("Polling iteration failed: %s", ex)
                # Dead letter the iteration failure for analysis
                try:
                    dl = Path(settings.dead_letter_path)
                    dl.parent.mkdir(parents=True, exist_ok=True)
                    with dl.open("a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"error": str(ex), "time": time.time()}) + "\n")
                except Exception:
                    pass
                failure_count += 1

            if failure_count >= settings.circuit_threshold:
                circuit_open_until = time.time() + settings.circuit_reset_seconds

            # Sleep until next poll
            shutdown_event.wait(timeout=settings.poll_interval)


def _signal_handler(signum, frame):  # type: ignore[no-untyped-def]
    shutdown_event.set()


def main() -> None:
    settings = load_settings(Path(__file__).resolve().parents[1])
    logger = get_logger("main", level=settings.log_level)

    # Start daemon in background thread
    t = threading.Thread(target=run_daemon, args=(settings,), daemon=True)
    t.start()

    # Run ASGI app (health/metrics)
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level=settings.log_level.lower())


if __name__ == "__main__":
    main()

