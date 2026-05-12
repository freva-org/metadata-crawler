"""Gzipped JSONLines metadata storage backend."""

from __future__ import annotations

import asyncio
import gzip
import multiprocessing as mp
import os
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Any,
    AsyncIterator,
    BinaryIO,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import orjson
import yaml

from ...utils import create_async_iterator, parse_batch
from ..config import SchemaField
from .base import (
    BackendWriter,
    IndexName,
    IndexStore,
    MetadataRecord,
    StorageOptions,
)


class JSONLineWriter(BackendWriter):
    """Write JSONLines to disk."""

    backend: ClassVar[str] = "JSONLines"

    def __post_init__(self) -> None:

        self._comp_level: int = self.storage_options.pop("comp_level", 4)
        self._f: Dict[str, BinaryIO] = {}
        self._stream_dict: Dict[str, str] = {s.name: s.path for s in self.streams}
        for _stream in self.streams:
            fs, _ = IndexStore.get_fs(_stream.path, **self.storage_options)
            parent = os.path.dirname(_stream.path).rstrip("/")
            try:
                fs.makedirs(parent, exist_ok=True)
            except Exception:  # pragma: no cover
                pass  # pragma: no cover
            self._f[_stream.name] = fs.open(_stream.path, mode="wb")

    @staticmethod
    def _encode_records(records: List[MetadataRecord]) -> bytes:
        """Serialize a list of dicts into one JSONL bytes blob."""
        parts = [orjson.dumps(rec) for rec in records]
        return b"".join(p + b"\n" for p in parts)

    def _gzip_once(self, payload: bytes) -> bytes:
        """Compress a whole JSONL blob into a single gz member (fast)."""
        return gzip.compress(payload, compresslevel=self._comp_level)

    def add(self, metadata_batch: List[Tuple[str, MetadataRecord]]) -> None:
        """Add a batch of metadata to the gzip store."""
        by_index: Dict[str, List[MetadataRecord]] = {
            name: [] for name in self._stream_dict
        }
        for index_name, metadata in metadata_batch:
            by_index[index_name].append(metadata)
        for index_name, records in by_index.items():
            if not records:
                continue
            payload = self._encode_records(records)
            gz = self._gzip_once(payload)
            self._f[index_name].write(gz)
            self.indexed_objects += len(records)

    def close(self) -> None:
        """Close the files."""
        for name, stream in self._f.items():
            try:
                stream.flush()
            except Exception:
                pass
            stream.close()
            if not self.indexed_objects:
                fs, _ = IndexStore.get_fs(
                    self._stream_dict[name], **self.storage_options
                )
                fs.rm(self._stream_dict[name])


class JSONLines(IndexStore):
    """Write metadata to gzipped JSONLines files."""

    suffix = ".json.gz"
    driver = "intake.source.jsonfiles.JSONLinesFileSource"

    def __init__(
        self,
        path: str,
        index_name: IndexName,
        schema: Dict[str, SchemaField],
        mode: Literal["w", "r"] = "r",
        storage_options: Optional[StorageOptions] = None,
        shadow: Optional[Union[str, List[str]]] = None,
        batch_size: int = 25_000,
        **kwargs: Any,
    ):
        super().__init__(
            path,
            index_name,
            schema,
            mode=mode,
            shadow=shadow,
            storage_options=storage_options,
            batch_size=batch_size,
        )
        _comp_level = int(kwargs.get("comp_level", "4"))
        self._proc: Optional[mp.process.BaseProcess] = None
        if mode == "w":
            args = (self.queue, self._sent) + tuple(self._paths)
            kwargs = {k: v for (k, v) in self.storage_options.items()}
            kwargs["comp_level"] = _comp_level
            self._proc = self._ctx.Process(
                target=JSONLineWriter.as_daemon,
                args=args,
                kwargs={"storage_options": kwargs},
                daemon=True,
            )
            self._proc.start()

    @property
    def proc(self) -> Optional[mp.process.BaseProcess]:
        """The writer process."""
        return self._proc

    def get_args(self, index_name: str) -> Dict[str, Any]:
        """Define the intake arguments."""
        path = self.get_path(index_name)
        return {
            "urlpath": path,
            "compression": "gzip",
            "text_mode": True,
            "storage_options": self.catalogue_storage_options(path),
        }

    @property
    def total_objects(self) -> int:
        """The number of total objects is always the the indexed objects."""
        return 0

    @classmethod
    def read_catalogue_metadata(cls, url: str, **kwargs: Any) -> MetadataRecord:
        """Load a intake yaml catalogue (remote or local)."""
        fs, _ = IndexStore.get_fs(url, **kwargs)
        cat_path = fs.unstrip_protocol(url)
        with fs.open(cat_path) as stream:
            cat: Dict[str, MetadataRecord] = yaml.safe_load(stream.read())
        return cat.get("metadata", {})

    async def read(
        self,
        index_name: str,
    ) -> AsyncIterator[List[MetadataRecord]]:
        """Yield batches of metadata records from a specific table.

        Parameters
        ^^^^^^^^^^
        index_name:
            The name of the index_name.

        Yields
        ^^^^^^^
        List[MetadataRecord]:
            Deserialised metadata records.
        """
        loop = asyncio.get_running_loop()
        ts_keys = self._timestamp_keys
        path = self.get_path(index_name)
        with (
            self._fs.open(
                path,
                mode="rt",
                compression="gzip",
                encoding="utf-8",
            ) as stream,
            ThreadPoolExecutor(max_workers=self.max_workers) as pool,
        ):
            raw_lines: List[str] = []
            async for line in create_async_iterator(stream):
                raw_lines.append(line)
                if len(raw_lines) >= self.batch_size:
                    batch = await loop.run_in_executor(
                        pool, parse_batch, raw_lines, ts_keys
                    )
                    yield batch
                    raw_lines.clear()
            if raw_lines:
                batch = await loop.run_in_executor(
                    pool, parse_batch, raw_lines, ts_keys
                )
                yield batch
