"""test ingesting."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

import intake
import mock
import requests
from pymongo import MongoClient

from metadata_crawler import add, delete, index


def test_ingest_solr(
    drs_config_path: Path, storage_options: Dict[str, str], solr_server: str
) -> None:
    """Test ingesting metadata to solr."""

    cat_file = "s3://test/metadata_crawler/tests/solr.yml"
    cat_files = []
    lens = []
    for ds in ("fs", "s3", "swift"):
        cat_file = f"s3://test/metadata_crawler/tests/solr-{ds}.yml"
        cat_files.append(cat_file)
        add(
            cat_file,
            drs_config_path,
            data_store_prefix=f"s3://test/metadata_crawler/tests/solrdata-{ds}",
            data_set=[f"obs-{ds}"],
            storage_options=storage_options,
        )
        cat = intake.open_catalog(cat_file, storage_options=storage_options)
        lens.append(len(cat.latest.read()))
    with mock.patch.dict(os.environ, {"MDC_INTERACTIVE": "1"}, clear=True):
        index(
            "solr",
            *cat_files,
            server=solr_server,
            storage_options=storage_options,
            batch_size=1,
        )
    res = requests.get(
        f"{solr_server}/solr/latest/select", params={"q": "*:*", "rows": 0}
    )
    num = res.json().get("response", {}).get("numFound", 0)
    assert sum(lens) == num


def test_ingest_mongo(
    drs_config_path: Path,
    mongo_server: Dict[str, str],
) -> None:
    """Test ingesting metadata to mongo."""

    cat_files = []
    lens = []
    with TemporaryDirectory() as temp_dir:
        for ds in ("fs", "s3", "swift"):
            cat_file = Path(temp_dir) / f"mongo-{ds}.yml"
            cat_files.append(cat_file)
            add(
                cat_file,
                drs_config_path,
                data_store_prefix=f"{ds}-mongodata",
                data_set=[f"obs-{ds}"],
                catalogue_backend="jsonlines",
            )
            cat = intake.open_catalog(cat_file)
            lens.append(len(cat.latest.read()))
        index("mongo", *cat_files, **mongo_server)
    with MongoClient(mongo_server["url"]) as client:
        col = client[mongo_server["database"]]["latest"]
        _f = list(col.find({}))
        print(len(_f))
        assert len(_f) == sum(lens)


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
