"""Collection of aync data ingest classes."""

from __future__ import annotations

import asyncio
import csv
import gzip
from collections import defaultdict
from pathlib import Path
from random import random
from threading import Lock

import aiohttp
import yaml

from ..logger import logger
from .base import BaseIngest

_lock = Lock()

MetadataKeys = [
    "cmor_table",
    "dataset",
    "driving_model",
    "ensemble",
    "experiment",
    "format",
    "fs_type",
    "grid_id",
    "grid_label",
    "institute",
    "level_type",
    "model",
    "product",
    "project",
    "rcm_name",
    "rcm_version",
    "realm",
    "time_aggregation",
    "time_frequency",
    "user",
    "variable",
    "file",
    "uri",
]


class UploadError(Exception):
    """Raised when upload returns a non-2xx status."""

    def __init__(self, status, reason, body):
        super().__init__(f"Upload failed: {status} {reason}\n{body}")
        self.status = status
        self.reason = reason
        self.body = body


class FileIngest(BaseIngest):
    """Ingest metadat into an intake catalogue."""

    name = "file"

    def __init__(
        self,
        uri: str | None = None,
        batch_size: int = 2500,
        **kwargs: str,
    ) -> None:
        super().__init__(batch_size=batch_size)
        self._lock: None | asyncio.Lock() = None
        scheme, _, path = (uri or "").rpartition("://")
        scheme = scheme or "file"

        self.yaml_path = Path(uri).with_suffix(".yaml")
        self.yaml_path.parent.mkdir(exist_ok=True, parents=True)
        self._core_path = self.yaml_path.parent / "metadata-files.csv.gz"
        self._latest_path = self.yaml_path.parent / "metadata-latest.csv.gz"
        comp = int(kwargs.get("comp_level", 4))
        self._files = {}
        for name, path in (
            ("files", self._core_path),
            ("latest", self._latest_path),
        ):
            f = gzip.open(path, mode="wt", compresslevel=comp)
            writer = csv.DictWriter(
                f, fieldnames=MetadataKeys, extrasaction="ignore"
            )
            writer.writeheader()
            self._files[name] = (f, writer)

    def _create_catalogue_file(self) -> None:
        catalog = {
            "version": 1,
            "sources": {
                "files": {
                    "description": "All metadata versions, gzipped CSV",
                    "driver": "csv",
                    "args": {
                        "urlpath": str(self._core_path.absolute()),
                        "compression": "gzip",
                        "blocksize": None,
                    },
                },
                "latest": {
                    "description": "Latest metadata versions only, gzipped CSV",
                    "driver": "csv",
                    "args": {
                        "urlpath": str(self._latest_path.absolute()),
                        "compression": "gzip",
                        "blocksize": None,
                    },
                },
            },
        }
        with self.yaml_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(
                catalog,
                f,
                sort_keys=False,  # preserve our ordering
                default_flow_style=False,
            )

    def _write_metadata(self, submit: dict[str, list[dict[str, object]]]) -> None:
        """Write metadata in csv format."""
        for core, items in submit.items():
            csv_writer = self._files[core][-1]
            for obj in items:
                csv_writer.writerow(obj)

    async def close(self) -> None:
        """Close the files."""
        for _file, _ in self._files.values():
            _file.flush()
            _file.write("\n]")
            _file.close()
        self._create_catalogue_file()

    async def index(self) -> None:
        pass

    async def add(
        self,
        metadata_batch: list[
            tuple[str, dict[str, str | list[str] | list[float]]]
        ],
        flush: bool = True,
    ) -> None:
        submit: dict[str, list[dict[str, object]]] = defaultdict(list)

        for core, metadata in metadata_batch:
            submit[core].append(metadata)
        while _lock.locked():
            await asyncio.sleep(random())
        with _lock:
            self._write_metadata(submit)

    async def delete(self, flush: bool = True, **search_keys: str) -> None:
        query = []
