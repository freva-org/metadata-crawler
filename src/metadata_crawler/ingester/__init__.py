from __future__ import annotations

from pathlib import Path
from typing import Optional, cast
from urllib.parse import urlparse

from ..utils import load_plugins
from .base import BaseIngest

__all__ = ["BaseIngest"]


def get_ingest_type_from_uri(uri: Optional[str]) -> str:
    """Get the storage type from a uri."""
    uri = uri or str(Path.cwd() / "metadata-store.json.gz")
    if uri.startswith("monogo://"):
        return "mongo"
    if uri.startswith("http://") or uri.startswith("https://"):
        parsed_uri = Path(urlparse(uri).path).lstrip("/")
        if parsed_uri.startswith("solr"):
            return "solr"
    match Path(uri.lower()).suffixes:
        case [".json", *_]:
            return "file"
        case [".csv", *_]:
            return "file"
        case [".yaml", *_]:
            return "file"
        case [".yml", *_]:
            return "file"
        case _:
            raise NotImplementedError(f"Could not get storage type for {uri}")


def get_ingest_instance(
    metadata_store: Path | str,
    batch_size: int = 2500,
    **kwargs: str,
) -> BaseIngest:
    """Create an instance of an ingest class.

    Parameters
    ----------
    uri: str
        Instead of declaring the ingest_type directly the ingest-type can be
        determined by a storage uri.
    **kwargs: str
        Additional arguments that are passed to create the instance of the
        cataloging system
    batch_size: int, default = 500
        The amount of metadata that is gathered before it is passed to the
        cataloging system
    n_consumers: int, default = 10
        How many instances of the consumer should run at a time

    Returns
    -------
    BaseIngest: An instance of the Ingestion class.
    """
    kwargs["uri"] = str(metadata_store)
    ingest_type = get_ingest_type_from_uri(metadata_store)
    ingest_instances = load_plugins("metadata_crawler.ingester")
    try:
        return cast(BaseIngest, ingest_instances[ingest_type](**kwargs))
    except KeyError:
        raise NotImplementedError(f"No such ingest type: {ingest_type}") from None
