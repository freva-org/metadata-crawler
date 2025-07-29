"""Collection of aync data ingest classes."""

from __future__ import annotations

import asyncio
import gzip
import json
from collections import defaultdict
from pathlib import Path
from random import random
from threading import Lock
from typing import Optional, Tuple, Union

import yaml

from .base import BaseIngest

_lock = Lock()


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
        uri: Optional[str] = None,
        batch_size: int = 2500,
        cores: Tuple[str, str] = ("latest", "files"),
        **kwargs: str,
    ) -> None:
        super().__init__(batch_size=batch_size, cores=cores, **kwargs)
        self._lock: Optional[asyncio.Lock()] = None
        scheme, _, path = (uri or "").rpartition("://")
        scheme = scheme or "file"

        self.yaml_path = Path(uri).with_suffix(".yaml")
        self.yaml_path.parent.mkdir(exist_ok=True, parents=True)
        self._core_path = self.yaml_path.parent / "metadata-files.jsonl"
        self._latest_path = self.yaml_path.parent / "metadata-latest.jsonl"
        comp = int(kwargs.get("comp_level", 4))
        self._streams = {}
        for name, path in (
            (cores[0], self._latest_path),
            (cores[-1], self._core_path),
        ):
            # f = gzip.open(path, mode="wt", compresslevel=comp)
            self._streams[name] = open(path, mode="w")

    def _create_catalogue_file(self) -> None:
        catalog = {
            "version": 1,
            "sources": {
                self.cores[0]: {
                    "description": "Latest metadata versions, gzipped CSV",
                    "driver": "csv",
                    "metadata": {"facets": self.facets},
                    "args": {
                        "urlpath": str(self._latest_path.absolute()),
                        "compression": "gzip",
                        "blocksize": None,
                    },
                },
                self.cores[1]: {
                    "description": "All metadata versions only, gzipped CSV",
                    "driver": "csv",
                    "metadata": {"facets": self.facets},
                    "args": {
                        "urlpath": str(self._core_path.absolute()),
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

    async def close(self) -> None:
        """Close the files."""
        for _file in self._streams.values():
            _file.flush()
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
        while _lock.locked():
            await asyncio.sleep(random())
        with _lock:
            for core, metadata in metadata_batch:
                # self._files[core][-1].writerow(metadata)
                self._streams[core].write(
                    json.dumps(metadata, ensure_ascii=False) + "\n"
                )

    async def delete(self, flush: bool = True, **search_keys: str) -> None:
        query = []
