"""Unit tests for ``SolrIndex._convert`` (no external services required)."""

from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict


class TestSchemaDrift:
    """``_convert`` must tolerate documents whose fields outrun the schema.

    A field the store's schema does not describe (e.g. ``data_type`` present in
    the data but missing from a backend's reconstructed schema) must pass
    through untouched instead of raising ``KeyError`` and killing the whole
    batch. Pure unit tests -- no Solr/Mongo/DB required.
    """

    @staticmethod
    def _solr(schema: Dict[str, Any]) -> Any:
        from metadata_crawler.ingester.solr import SolrIndex

        # bypass __init__; _convert only reads self.index_schema, which is
        # served by the first entry of ``_stores``
        inst = SolrIndex.__new__(SolrIndex)
        inst._stores = [SimpleNamespace(schema=schema)]
        return inst

    def test_unknown_fields_pass_through(self) -> None:
        solr = self._solr({"file": SimpleNamespace(type="string")})
        out = solr._convert({"file": "/a.nc", "data_type": ["obs"], "extra": 1})
        assert out["file"] == "/a.nc"
        assert out["data_type"] == ["obs"]  # untouched, no KeyError
        assert out["extra"] == 1

    def test_empty_schema_passes_everything(self) -> None:
        solr = self._solr({})  # worst-case drift: store exposes no schema
        doc = {"a": 1, "b": "x"}
        assert solr._convert(dict(doc)) == doc

    def test_known_special_types_still_convert(self) -> None:
        schema = {
            "bbox": SimpleNamespace(type="bbox"),
            "time": SimpleNamespace(type="daterange"),
            "creation_time": SimpleNamespace(type="datetime"),
            "file": SimpleNamespace(type="string"),
        }
        solr = self._solr(schema)
        t0, t1 = datetime(2020, 1, 1), datetime(2020, 12, 31)
        out = solr._convert(
            {
                "bbox": [1, 2, 3, 4],
                "time": [t0, t1],
                "creation_time": [t0],
                "file": "/a.nc",
                "unknown": "keep",
            }
        )
        assert out["bbox"] == "ENVELOPE(1, 2, 4, 3)"
        assert out["time"] == "[2020-01-01T00:00:00Z TO 2020-12-31T00:00:00Z]"
        assert out["creation_time"] == "2020-01-01T00:00:00Z"
        assert out["unknown"] == "keep"  # unknown alongside known fields
