"""Gzipped JSONLines metadata storage backend."""

from __future__ import annotations

import asyncio
import gzip
import multiprocessing as mp
import os
from fnmatch import fnmatch
from itertools import islice
from tempfile import TemporaryDirectory
from typing import (
    Any,
    AsyncIterator,
    BinaryIO,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    TextIO,
    Tuple,
    Union,
    cast,
)

import orjson
import yaml

from ...logger import logger
from ...utils import parse_batch
from ..config import BaseType, SchemaField
from .base import (
    BackendWriter,
    FacetValue,
    IndexName,
    IndexStore,
    MetadataRecord,
    StorageOptions,
)

Record = Dict[str, Any]
Predicate = Callable[[MetadataRecord], bool]
_Batch = Tuple[List[MetadataRecord], bool]


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

    def _write_table(self, table_name: str, rows: List[MetadataRecord]) -> int:
        payload = self._encode_records(rows)
        gz = self._gzip_once(payload)
        self._f[table_name].write(gz)
        return len(rows)

    def _close(self) -> None:
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
        self._comp_level = int(kwargs.get("comp_level", "4"))
        self._proc: Optional[mp.process.BaseProcess] = None
        self.schema = schema
        self.mode = mode
        if mode == "w":
            kwargs = {k: v for (k, v) in self.storage_options.items()}
            kwargs["comp_level"] = self._comp_level
            args = (self.queue, self._sent, self.counter) + tuple(self._paths)
            self._proc = self._ctx.Process(
                target=JSONLineWriter.as_daemon,
                args=args,
                kwargs={"storage_options": kwargs},
                daemon=True,
            )
            self._proc.start()

    @staticmethod
    def _matches(field: SchemaField, value: Any, patterns: List[FacetValue]) -> bool:
        """Check a single record value against the patterns of one facet."""
        if value is None:
            return False
        values = value if isinstance(value, list) else [value]
        if field.base_type == BaseType.string:
            return any(
                fnmatch(str(v).lower(), str(p).lower())
                for v in values
                for p in patterns
            )
        targets = set()
        for pattern in patterns:
            try:
                targets.add(float(pattern))
            except (TypeError, ValueError):
                continue
        return any(isinstance(v, (int, float)) and float(v) in targets for v in values)

    def _make_predicate(self, grouped: Dict[str, List[FacetValue]]) -> Predicate:
        checks = [(key, self.schema[key], pats) for key, pats in grouped.items()]

        def predicate(record: MetadataRecord) -> bool:
            return all(
                self._matches(field, record.get(key), pats)
                for key, field, pats in checks
            )

        return predicate

    def _discard(self, path: str) -> None:
        try:
            if self._fs.exists(path):
                self._fs.rm(path)
        except Exception as error:  # pragma: no cover
            logger.warning("Could not remove temporary file %s: %s", path, error)

    def _filter_batch(
        self, batch: List[MetadataRecord], predicate: Predicate, encode: bool
    ) -> Tuple[bytes, int]:
        """Filter one batch; return the gzipped survivors and the removed count."""
        keep = [rec for rec in batch if not predicate(rec)]
        removed = len(batch) - len(keep)
        if not encode or not keep:
            return b"", removed
        payload = JSONLineWriter._encode_records(keep)
        return gzip.compress(payload, compresslevel=self._comp_level), removed

    async def _rewrite(
        self, index_name: str, path: str, predicate: Predicate, dry_run: bool
    ) -> int:
        """Filter one index file and replace it; return the number removed."""
        local_target = cast(
            Optional[str],
            self._fs._strip_protocol(path) if self._is_local_path else None,
        )
        tmp_parent = os.path.dirname(local_target) if local_target else None
        removed = 0
        with TemporaryDirectory(dir=tmp_parent, prefix=".mdc-") as tmp_dir:
            tmp = os.path.join(tmp_dir, os.path.basename(path))
            with open(tmp, "wb") as out:
                async for batch in self.read(index_name, parse_timestamps=False):
                    gz, n = await asyncio.to_thread(
                        self._filter_batch, batch, predicate, not dry_run
                    )
                    removed += n
                    if gz:
                        await asyncio.to_thread(out.write, gz)
            if dry_run or not removed:
                return removed
            if local_target:
                await asyncio.to_thread(os.replace, tmp, local_target)
            else:
                await asyncio.to_thread(self._fs.put_file, tmp, path)
        return removed

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
        return self.counter.value

    @classmethod
    def read_catalogue_metadata(cls, url: str, **kwargs: Any) -> MetadataRecord:
        """Load a intake yaml catalogue (remote or local)."""
        fs, _ = IndexStore.get_fs(url, **kwargs)
        cat_path = fs.unstrip_protocol(url)
        with fs.open(cat_path) as stream:
            cat: Dict[str, MetadataRecord] = yaml.safe_load(stream.read())
        return cat.get("metadata", {})

    def _read_batch(
        self,
        stream: TextIO,
        ts_keys: Set[str],
        predicate: Optional[Predicate] = None,
    ) -> Tuple[List[MetadataRecord], bool]:
        """Read and parse up to ``batch_size`` lines; runs in a worker thread."""
        raw = list(islice(stream, self.batch_size))
        eof = len(raw) < self.batch_size
        lines = [line for line in raw if line.strip()]
        records = parse_batch(lines, ts_keys) if lines else []
        if predicate is not None:
            records = [rec for rec in records if predicate(rec)]
        return records, eof

    async def _read(
        self,
        index_name: str,
        *,
        parse_timestamps: bool = True,
    ) -> AsyncIterator[List[MetadataRecord]]:
        """Yield batches of metadata records from a specific table."""
        ts_keys = self._timestamp_keys if parse_timestamps else set()
        path = self.get_path(index_name)
        stream = cast(
            TextIO,
            await asyncio.to_thread(
                self._fs.open, path, mode="rt", compression="gzip", encoding="utf-8"
            ),
        )
        pending: Optional[asyncio.Future[_Batch]] = None

        def schedule() -> asyncio.Future[_Batch]:
            return asyncio.ensure_future(
                asyncio.to_thread(self._read_batch, stream, ts_keys)
            )

        try:
            current = schedule()
            pending = current
            while True:
                records, eof = await current
                pending = None
                if not eof:
                    current = schedule()
                    pending = current
                if records:
                    yield records
                if eof:
                    break
        finally:
            if pending is not None:
                # A thread can't be cancelled; let it finish before closing the file.
                await asyncio.gather(pending, return_exceptions=True)
            await asyncio.to_thread(stream.close)

    async def _remove(self, grouped: Dict[str, List[FacetValue]], dry_run: bool) -> int:
        """Remove entries from intake catalogue with matching *grouped* facets."""
        predicate = self._make_predicate(grouped)
        streams = [s for s in self._paths if self._fs.exists(s.path)]
        counts = await asyncio.gather(
            *(self._rewrite(s.name, s.path, predicate, dry_run) for s in streams)
        )
        return sum(counts)
