"""test ingesting."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

import mock
import pytest
import requests
from pymongo import MongoClient

from metadata_crawler import add, delete, index


@pytest.mark.parametrize("backend", ["intake", "mongodb", "postgresql"])
def test_ingest_solr(
    backend: str,
    drs_config_path: Path,
    storage_options: Dict[str, str],
    solr_server: str,
    db_storage_options: dict[str, str],
    pg_cursor: Any,
    mongo_client: Any,
) -> None:
    """Test ingesting metadata to solr."""

    ports = {"mongodb": 27017, "postgresql": 5432}
    db = db_storage_options["database"]
    u = db_storage_options["username"]
    p = db_storage_options["password"]
    stores = []
    for ds in ("fs", "s3", "swift"):
        if backend == "intake":
            store = f"s3://test/metadata_crawler/tests/solr-{ds}.yml"
        else:
            store = f"{backend}://{u}:{p}@localhost:{ports[backend]}/{db}"
        stores.append(store)
        add(
            drs_config_path,
            store=store,
            data_store_prefix=f"s3://test/metadata_crawler/tests/solrdata-{ds}",
            data_set=[f"obs-{ds}"],
            storage_options=storage_options,
        )
    with mock.patch.dict(os.environ, {"MDC_INTERACTIVE": "1"}, clear=True):
        index(
            "solr",
            *stores,
            server=solr_server,
            storage_options=storage_options,
            batch_size=1,
        )
    res = requests.get(
        f"{solr_server}/solr/latest/select", params={"q": "*:*", "rows": 0}
    )
    num = res.json().get("response", {}).get("numFound", 0)
    assert num > 0


@pytest.mark.parametrize("backend", ["intake", "mongodb", "postgresql"])
def test_ingest_mongo(
    backend: str,
    drs_config_path: Path,
    mongo_server: Dict[str, str],
    db_storage_options: dict[str, str],
    pg_cursor: Any,
    mongo_client: Any,
) -> None:
    """Test ingesting metadata to mongo."""
    # ports = {"mongodb": 27017, "postgresql": 5432}
    db = db_storage_options["database"]
    u = db_storage_options["username"]
    p = db_storage_options["password"]
    stores = []
    with TemporaryDirectory() as temp_dir:
        for ds in ("fs", "s3", "swift"):
            if backend == "intake":
                store = Path(temp_dir) / f"mongo-{ds}.yml"
            else:
                store = f"{backend}://{u}:{p}@localhost/{db}"
            stores.append(store)
            add(
                drs_config_path,
                store=store,
                data_store_prefix=f"{ds}-mongodata",
                data_set=[f"obs-{ds}"],
                backend=backend,
            )
        index("mongo", *stores, url=mongo_server["url"], database="test")
    with MongoClient(mongo_server["url"]) as client:
        col = client["test"]["latest"]
        _f = list(col.find({}))
        assert len(_f) > 0


def test_delete_mongo(
    mongo_server: Dict[str, str],
    metadata: List[Dict[str, Any]],
) -> None:
    """Test deleting metadata from mongo."""

    with MongoClient(mongo_server["url"]) as client:
        db = client[mongo_server["database"]]
        for col in ("files", "latest"):
            collection = db[col]
            for md in metadata:
                collection.insert_one(md)
    delete("mongo", facets=[("project", "*")], **mongo_server)
    with MongoClient(mongo_server["url"]) as client:
        col = client[mongo_server["database"]]["latest"]
        assert len(list(col.find({}))) == 0
    delete("mongo", facets=[("project", "foo")], **mongo_server)
    delete("mongo", **mongo_server)


def test_delete_solr(
    solr_server: str,
    metadata: List[Dict[str, Any]],
) -> None:
    """Test deleting metadata from solr."""
    for core in ("latest", "files"):
        url = f"{solr_server}/solr/{core}/update/json?commit=true"
        res = requests.post(url, json=metadata)
        res.raise_for_status()
    delete("solr", facets=[("project", "*")], server=solr_server)
    res = requests.get(
        f"{solr_server}/solr/latest/select", params={"q": "*:*", "rows": 0}
    )
    num = res.json().get("response", {}).get("numFound", 0)
    assert num == 0
    delete("solr", facets=[("file", "/foo/*")], server=solr_server)
    delete("solr", facets=[("file", "/foo/*")], server=solr_server)
