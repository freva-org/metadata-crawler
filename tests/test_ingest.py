"""test ingesting."""

import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Set, Tuple
from unittest.mock import MagicMock

import intake
import mock
import psycopg
import pymongo
import pytest
import requests
import sqlalchemy as sa
import yaml
from pymongo import MongoClient

from metadata_crawler import delete, index


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="function")
def mock_dir() -> Iterator[Path]:

    cur_dir = os.getcwd()
    mock_dir = Path(__file__).parent / "mock_crawls"
    os.chdir(mock_dir)
    yield mock_dir
    os.chdir(cur_dir)


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


# ---- small Solr Core-Admin / query helpers used by the blue/green tests --- #
def _solr_cores(base: str) -> Set[str]:
    res = requests.get(
        f"{base}/solr/admin/cores", params={"action": "STATUS", "wt": "json"}
    )
    res.raise_for_status()
    return set(res.json().get("status", {}).keys())


def _solr_num_q(base: str, core: str, query: str = "*:*") -> int:
    res = requests.get(f"{base}/solr/{core}/select", params={"q": query, "rows": 0})
    res.raise_for_status()
    return int(res.json().get("response", {}).get("numFound", 0))


def _solr_seed(base: str, core: str, docs: List[Dict[str, Any]]) -> None:
    res = requests.post(f"{base}/solr/{core}/update/json?commit=true", json=docs)
    res.raise_for_status()


def _solr_unload(base: str, core: str) -> None:
    requests.get(
        f"{base}/solr/admin/cores",
        params={"action": "UNLOAD", "core": core, "deleteInstanceDir": "true"},
    )


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture()
def db_cleanup(request, db_storage_options):
    """Clean the relevant database before and after the test."""
    from metadata_crawler.api.stores.base import Stream
    from metadata_crawler.api.stores.mongodb import MongoDB, MongoDBWriter
    from metadata_crawler.api.stores.postgresql import PostgreSQL, PostgreSQLWriter

    cur_dir = os.getcwd()
    os.chdir(Path(__file__).parent / "mock_crawls")

    meta = _get_metadata()
    backend = request.param if hasattr(request, "param") else None
    data = _read_catalogues()
    counter = MagicMock()
    counter.value = 0
    counter.get_lock.return_value = MagicMock()

    def _add_data():
        if backend in (None, "postgresql"):
            url = "postgresql://localhost:5432"
            s = [Stream(name=n, path=url) for n in ("latest", "files")]
            writer = PostgreSQLWriter(
                counter,
                *s,
                username=db_storage_options["username"],
                password=db_storage_options["password"],
                database=db_storage_options["database"],
                unique_key="file",
                schema_json=json.dumps(meta["schema"]),
            )
            engine = sa.create_engine(writer._url, pool_pre_ping=True)
            for batch in data:
                for entry in map(_convert, batch):
                    writer.add([("latest", entry)])
                    writer.add([("files", entry)])
            writer.close()
            PostgreSQL._write_metadata(engine, meta, "metadata_crawler")
            engine.dispose()
        if backend in (None, "mongodb"):
            url = "mongodb://localhost:27017"
            s = [Stream(name=n, path=url) for n in ("latest", "files")]
            writer = MongoDBWriter(
                counter,
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
            writer.close()

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


@pytest.fixture(scope="session")
def solr_configset() -> str:
    """Configset the blue/green cores are created from.

    Skips the dependent tests if the configset is not usable on this Solr
    (e.g. a CI stack without the ``freva`` configset), so the suite stays
    green in environments that cannot exercise core creation.
    """
    base = "http://localhost:8983"
    name = os.environ.get("MDC_TEST_SOLR_CONFIGSET", "freva")
    probe = "mdc_cfg_probe"
    res = requests.get(
        f"{base}/solr/admin/cores",
        params={"action": "CREATE", "name": probe, "configSet": name},
    )
    if res.status_code >= 400:
        pytest.skip(f"Solr configset {name!r} not usable: {res.text[:200]}")
    _solr_unload(base, probe)
    return name


@pytest.fixture()
def bg_solr(solr_server: str, solr_configset: str) -> Iterator[Tuple[str, str, str]]:
    """(base, configset, suffix) for blue/green tests.

    The suffix is UNIQUE per test, mirroring production. Reusing a fixed
    suffix breaks the next rotation: after a SWAP the live core migrates into
    the ``<name><suffix>`` instance dir, so the following CREATE collides with
    it (and its held write.lock).
    """
    suffix = f"_bg{uuid.uuid4().hex[:8]}"
    try:
        yield solr_server, solr_configset, suffix
    finally:
        for core in (f"files{suffix}", f"latest{suffix}"):
            _solr_unload(solr_server, core)


@pytest.fixture()
def bg_mongo_db(mongo_server: Dict[str, str]) -> Iterator[Dict[str, str]]:
    """A dedicated, cleaned Mongo database for blue/green tests."""
    url = mongo_server["url"]
    dbname = "test_bluegreen"

    def _clean() -> None:
        with MongoClient(url) as client:
            db = client[dbname]
            for name in db.list_collection_names():
                db[name].drop()

    _clean()
    try:
        yield {"url": url, "database": dbname}
    finally:
        _clean()


# --------------------------------------------------------------------------- #
# ingest
# --------------------------------------------------------------------------- #
class TestIngestSolr:
    """Ingest metadata from every backend into Solr."""

    @pytest.mark.parametrize(
        "backend,db_cleanup",
        [
            ("intake", None),
            ("mongodb", "mongodb"),
            ("postgresql", "postgresql"),
        ],
        indirect=["db_cleanup"],
    )
    def test_ingest(
        self,
        backend: str,
        db_cleanup: str,
        storage_options: Dict[str, str],
        solr_server: str,
        db_storage_options: Dict[str, str],
    ) -> None:
        """Test ingesting metadata to solr."""
        ports = {"mongodb": 27017, "postgresql": 5432}
        db = db_storage_options["database"]
        u = db_storage_options["username"]
        p = db_storage_options["password"]
        stores: List[str] = []
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
        num = _solr_num_q(solr_server, "latest")
        assert num > 0


class TestIngestMongo:
    """Ingest metadata from every backend into MongoDB."""

    @pytest.mark.parametrize(
        "backend,db_cleanup",
        [
            ("intake", None),
            ("mongodb", "mongodb"),
            ("postgresql", "postgresql"),
        ],
        indirect=["db_cleanup"],
    )
    def test_ingest(
        self,
        backend: str,
        db_cleanup: str,
        mongo_server: Dict[str, str],
        db_storage_options: Dict[str, str],
    ) -> None:
        """Test ingesting metadata to mongo."""
        db = db_storage_options["database"]
        u = db_storage_options["username"]
        p = db_storage_options["password"]
        stores: List[str] = []
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


# --------------------------------------------------------------------------- #
# delete
# --------------------------------------------------------------------------- #
class TestDelete:
    """Remove metadata from the index systems."""

    def test_mongo(
        self,
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

    def test_solr(
        self,
        solr_server: str,
        metadata: List[Dict[str, Any]],
    ) -> None:
        """Test deleting metadata from solr."""
        for core in ("latest", "files"):
            url = f"{solr_server}/solr/{core}/update/json?commit=true"
            res = requests.post(url, json=metadata)
            res.raise_for_status()
        delete("solr", facets=[("project", "*")], server=solr_server)
        num = _solr_num_q(solr_server, "latest")
        assert num == 0
        delete("solr", facets=[("file", "/foo/*")], server=solr_server)
        delete("solr", facets=[("file", "/foo/*")], server=solr_server)


# --------------------------------------------------------------------------- #
# blue/green rotation
# --------------------------------------------------------------------------- #
class TestBlueGreenSolr:
    """Blue/green core rotation for Solr (index --rotate)."""

    def test_rotate_swaps_live(self, bg_solr, mock_dir: Path) -> None:
        base, configset, suffix = bg_solr
        index(
            "solr",
            str(mock_dir / "fs-cat.yml"),
            server=base,
            batch_size=1,
            rotate=True,
            index_suffix=suffix,
            configset=configset,
            min_docs=1,
        )
        assert _solr_num_q(base, "latest") > 0
        assert _solr_num_q(base, "files") > 0
        cores = _solr_cores(base)
        assert f"files{suffix}" not in cores
        assert f"latest{suffix}" not in cores

    def test_rotate_replaces_not_merges(self, bg_solr, mock_dir: Path) -> None:
        base, configset, suffix = bg_solr
        sentinel = {"file": "/__sentinel__.nc", "uri": "file:///__sentinel__.nc"}
        _solr_seed(base, "files", [sentinel])
        assert _solr_num_q(base, "files", 'file:"/__sentinel__.nc"') == 1
        index(
            "solr",
            str(mock_dir / "fs-cat.yml"),
            server=base,
            batch_size=1,
            rotate=True,
            index_suffix=suffix,
            configset=configset,
            min_docs=1,
        )
        assert _solr_num_q(base, "files", 'file:"/__sentinel__.nc"') == 0
        assert _solr_num_q(base, "files") > 0

    def test_rotate_aborts_below_min_docs(self, bg_solr, mock_dir: Path) -> None:
        base, configset, suffix = bg_solr
        seed = [
            {"file": f"/seed{i}.nc", "uri": f"file:///seed{i}.nc"} for i in range(3)
        ]
        for core in ("files", "latest"):
            _solr_seed(base, core, seed)
        before = {c: _solr_num_q(base, c) for c in ("files", "latest")}
        with pytest.raises(SystemExit):
            index(
                "solr",
                str(mock_dir / "fs-cat.yml"),
                server=base,
                batch_size=1,
                rotate=True,
                index_suffix=suffix,
                configset=configset,
                min_docs=10_000,
            )
        assert {c: _solr_num_q(base, c) for c in ("files", "latest")} == before
        cores = _solr_cores(base)
        assert f"files{suffix}" not in cores
        assert f"latest{suffix}" not in cores


class TestBlueGreenMongo:
    """Blue/green collection rotation for Mongo (index --rotate).

    The target database starts empty, so ``test_rotate_promotes`` also covers
    the first-deployment case (rename with nothing to drop).
    """

    def test_rotate_promotes(self, bg_mongo_db: Dict[str, str], mock_dir: Path) -> None:
        """A rotation populates the live collections and drops the temp ones."""
        url, dbname = bg_mongo_db["url"], bg_mongo_db["database"]
        index(
            "mongo",
            str(mock_dir / "fs-cat.yml"),
            url=url,
            database=dbname,
            rotate=True,
            index_suffix="_bg",
            min_docs=1,
        )
        with MongoClient(url) as client:
            db = client[dbname]
            assert db["latest"].count_documents({}) > 0
            assert db["files"].count_documents({}) > 0
            names = set(db.list_collection_names())
        assert "files_bg" not in names
        assert "latest_bg" not in names

    def test_rotate_replaces_not_merges(
        self, bg_mongo_db: Dict[str, str], mock_dir: Path
    ) -> None:
        """rename(dropTarget=True) replaces the live collection wholesale."""
        url, dbname = bg_mongo_db["url"], bg_mongo_db["database"]
        with MongoClient(url) as client:
            client[dbname]["files"].insert_one({"file": "/__sentinel__.nc"})

        index(
            "mongo",
            str(mock_dir / "fs-cat.yml"),
            url=url,
            database=dbname,
            rotate=True,
            index_suffix="_bg",
            min_docs=1,
        )
        with MongoClient(url) as client:
            db = client[dbname]
            assert db["files"].count_documents({"file": "/__sentinel__.nc"}) == 0
            assert db["files"].count_documents({}) > 0

    def test_rotate_aborts_below_min_docs(
        self, bg_mongo_db: Dict[str, str], mock_dir: Path
    ) -> None:
        """Too few docs -> abort, drop temp collections, keep live ones."""
        url, dbname = bg_mongo_db["url"], bg_mongo_db["database"]
        seed = [{"file": f"/seed{i}.nc"} for i in range(3)]
        with MongoClient(url) as client:
            db = client[dbname]
            db["files"].insert_many(list(seed))
            db["latest"].insert_many(list(seed))
            before = {c: db[c].count_documents({}) for c in ("files", "latest")}

        with pytest.raises(SystemExit):
            index(
                "mongo",
                str(mock_dir / "fs-cat.yml"),
                url=url,
                database=dbname,
                rotate=True,
                index_suffix="_bg",
                min_docs=10_000,
            )

        with MongoClient(url) as client:
            db = client[dbname]
            after = {c: db[c].count_documents({}) for c in ("files", "latest")}
            names = set(db.list_collection_names())
        assert after == before  # live collections untouched
        assert "files_bg" not in names  # temp collections dropped
        assert "latest_bg" not in names
