"""test ingesting."""

import os
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime
import mock
import psycopg
import pymongo
import pytest
import requests
from pymongo import MongoClient
import intake
from metadata_crawler import delete, index
import yaml
import json


def _read_catalogues() -> List[Any]:
    data = []
    cur_dir = os.getcwd()
    os.chdir(Path(__file__).parent / "mock_crawls")
    for _file in Path(".").rglob("*.yml"):
        data.append(intake.open_catalog(_file).latest.read())
    os.chdir(cur_dir)
    return data


def _get_metadata() -> Dict[str, Any]:
    for _file in Path(Path(__file__).parent).rglob("*.yml"):
        return yaml.safe_load(_file.read_text())["metadata"]


def _convert(metadata: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for key, entry in metadata.items():
        if key == "time":
            out[key] = [
                datetime.fromisoformat(entry[0]),
                datetime.fromisoformat(entry[1]),
            ]
        else:
            out[key] = entry
    return out


@pytest.fixture()
def db_cleanup(request, db_storage_options):
    """Clean the relevant database before and after the test."""
    from metadata_crawler.api.stores.base import Stream
    from metadata_crawler.api.stores.postgresql import PostgreSQLWriter, PostgreSQL
    from metadata_crawler.api.stores.mongodb import MongoDBWriter, MongoDB

    cur_dir = os.getcwd()
    os.chdir(Path(__file__).parent / "mock_crawls")

    meta = _get_metadata()
    backend = request.param if hasattr(request, "param") else None
    data = _read_catalogues()

    def _add_data():
        if backend in (None, "postgresql"):
            url = "postgresql://localhost:5432"
            s = [Stream(name=n, path=url) for n in ("latest", "files")]
            writer = PostgreSQLWriter(
                *s,
                username=db_storage_options["username"],
                password=db_storage_options["password"],
                database=db_storage_options["database"],
                unique_key="file",
                schema_json=json.dumps(meta["schema"]),
            )
            for batch in data:
                for entry in map(_convert, batch):
                    writer.add([("latest", entry)])
                    writer.add([("files", entry)])
            url = "postgresql://localhost:5432"
            PostgreSQL._write_metadata(writer._engine, meta)
            writer._engine.dispose()
        if backend in (None, "mongodb"):
            url = "mongodb://localhost:27017"
            s = [Stream(name=n, path=url) for n in ("latest", "files")]
            writer = MongoDBWriter(
                *s,
                username=db_storage_options["username"],
                password=db_storage_options["password"],
                database=db_storage_options["database"],
                unique_key="file",
            )
            for batch in data:
                for entry in map(_convert, batch):
                    writer.add([("latest", entry)])
                    writer.add([("files", entry)])
            MongoDB._write_metadata(writer._client, meta)
            writer._client.close()

    def _cleanup():
        if backend in (None, "postgresql"):
            conn = psycopg.connect(
                host="localhost",
                port=5432,
                user=db_storage_options["username"],
                password=db_storage_options["password"],
                dbname=db_storage_options["database"],
            )
            cur = conn.cursor()
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            for (table,) in cur.fetchall():
                cur.execute(f"DELETE FROM {table}")
            conn.commit()
            cur.close()
            conn.close()
        if backend in (None, "mongodb"):
            client = pymongo.MongoClient(
                "mongodb://localhost:27017",
                username=db_storage_options["username"],
                password=db_storage_options["password"],
                authSource="admin",
            )
            db = client[db_storage_options["database"]]
            for name in db.list_collection_names():
                if not name.startswith("system."):
                    db[name].delete_many({})
            client.close()

    _cleanup()
    _add_data()
    yield
    _cleanup()
    os.chdir(cur_dir)


@pytest.mark.parametrize(
    "backend,db_cleanup",
    [
        ("intake", None),
        ("mongodb", "mongodb"),
        ("postgresql", "postgresql"),
    ],
    indirect=["db_cleanup"],
)
def test_ingest_solr(
    backend: str,
    db_cleanup: str,
    storage_options: Dict[str, str],
    solr_server: str,
    db_storage_options: dict[str, str],
) -> None:
    """Test ingesting metadata to solr."""

    ports = {"mongodb": 27017, "postgresql": 5432}
    db = db_storage_options["database"]
    u = db_storage_options["username"]
    p = db_storage_options["password"]
    stores = []

    for ds in ("fs", "s3", "swift"):
        if backend == "intake":
            store = (Path(__file__).parent / f"mock_crawls/{ds}-cat.yml").absolute()
            stores.append(str(store))
        else:
            stores = [f"{backend}://{u}:{p}@localhost:{ports[backend]}/{db}"]
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


@pytest.mark.parametrize(
    "backend,db_cleanup",
    [
        ("intake", None),
        ("mongodb", "mongodb"),
        ("postgresql", "postgresql"),
    ],
    indirect=["db_cleanup"],
)
def test_ingest_mongo(
    backend: str,
    db_cleanup: str,
    mongo_server: Dict[str, str],
    db_storage_options: dict[str, str],
) -> None:
    """Test ingesting metadata to mongo."""
    # ports = {"mongodb": 27017, "postgresql": 5432}
    db = db_storage_options["database"]
    u = db_storage_options["username"]
    p = db_storage_options["password"]
    stores = []
    for ds in ("fs", "s3", "swift"):
        if backend == "intake":
            store = (Path(__file__).parent / f"mock_crawls/{ds}-cat.yml").absolute()
            stores.append(str(store))
        else:
            stores = [f"{backend}://{u}:{p}@localhost/{db}"]
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
