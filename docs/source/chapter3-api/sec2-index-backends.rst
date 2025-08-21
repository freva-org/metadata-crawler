.. _add_backends:

Adding index backends
---------------------

An *index backend* stores the final, translated metadata records.
Built‑in backends include a **DuckDB** database (either on disk,
in memory or on S3) and **MongoDB** via Motor.  You can implement
additional index backends to suit your needs.

Base classes and helpers
^^^^^^^^^^^^^^^^^^^^^^^^

The ``metadata_stores.py`` module defines two key abstractions:

* ``IndexStore`` – An abstract base class representing an index
  backend.  Concrete implementations must implement methods to
  ``add`` batches of records, ``read`` chunks from an index, and
  ``delete`` based on facet filters.  A convenience ``close`` method
  cleans up resources.
* ``StorageIndex`` – A simple data class grouping together the index
  name and any configuration needed by the backend.

DuckDBIndexStore
^^^^^^^^^^^^^^^^

``DuckDBIndexStore`` indexes metadata into a DuckDB database.  When
initialised you specify the database path and the table names to
create (``latest``, ``time_aggregation``, etc.).  The schema is
derived from the configuration.  The store supports two modes:

* ``w`` – Write mode: drop any existing tables and create fresh
  tables based on the schema.  Use this when building an index from
  scratch.
* ``r`` – Read‑only mode: tables must already exist; data can be
  appended but not structural changes.

DuckDB supports writing to local files, to ``:memory:``, or to
remote S3/MinIO using the httpfs extension.  To write to S3 you must
install ``httpfs`` inside your DuckDB connection and set PRAGMA
properties to configure endpoint, credentials and region (see
``sec4-storage-options`` for details).

MongoIndexStore
^^^^^^^^^^^^^^^

``MongoIndexStore`` stores records in MongoDB collections.  Each
index name corresponds to a collection.  Records are upserted based
on the ``file`` facet: if a document with the same ``file`` exists
it will be replaced; otherwise it is inserted.  Deletion uses
``$regex`` queries for glob patterns and ``$eq`` for exact values.

Provide the MongoDB connection URL and database name via the
``url`` and ``database`` parameters.  You may specify additional
options (e.g. TLS settings) in ``storage_options``.

Implementing a custom backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add a new index backend:

1. **Subclass** ``IndexStore`` and implement the abstract methods
   ``add(self, metadata_batch)`` (accepts a list of tuples of
   ``index_name`` and record dicts), ``read`` (yield records in
   chunks), and ``delete`` (remove matching records).
2. **Register** your implementation under the entry point
   ``metadata_crawler.index_backends`` so it can be discovered via
   the ``index_backend`` CLI option.
3. The ``schema`` argument passed to your constructor contains
   ``SchemaField`` objects that describe the canonical facets (see
   :doc:`../chapter2-config/index`).  Use this information to
   construct tables or documents with appropriate types.

Example skeleton
^^^^^^^^^^^^^^^^

.. code-block:: python

   from metadata_crawler.metadata_stores import IndexStore
   from typing import List, Tuple, Dict, Any, Iterator

   class MyIndexStore(IndexStore):
       def __init__(self, path: str, index_names: Tuple[str, ...], schema, **opts):
           super().__init__(path, index_names, schema)
           # setup connection...

       async def add(self, metadata_batch: List[Tuple[str, Dict[str, Any]]]) -> None:
           # insert or upsert records
           ...

       async def read(self, index_name: str) -> Iterator[List[Dict[str, Any]]]:
           # yield records in batches
           ...

       async def delete(self, index_name: str, facets):
           # remove matching records
           ...

   # register in pyproject.toml
   # [project.entry-points."metadata_crawler.index_backends"]
   # mybackend = "my_package.my_index:MyIndexStore"
