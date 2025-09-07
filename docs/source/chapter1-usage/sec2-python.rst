Using the Python API
---------------------
.. _python_lib:

The Python API exposes high‑level functions to perform crawling and
indexing tasks.  These functions accept the same parameters as the
CLI but give you full control over the event loop and thread pool.

Two styles of APIs are provided:

* **Synchronous** wrappers that block until completion.
* **Asynchronous** coroutines that can be integrated into your own
  asyncio event loop and combined with other tasks.


Synchronous usage
^^^^^^^^^^^^^^^^^^

The synchronous API functions return when the operation is finished
and raise exceptions on error.  A typical workflow consists of

1. **Crawling**: collect metadata from one or more files or datasets
   into a temporary catalog (e.g. JSON lines).
2. **Indexing**: read entries from the catalog and write them to the
   configured index backend (e.g. Apache Solr or MongoDB).
3. **Deleting**: remove previously indexed entries matching a set
   of search facets (optional).

Below is a minimal example that crawls data from a local directory,
stores it in a JSON lines catalog, and indexes it to Apache Solr:

.. code-block:: python

   from metadata_crawler import add, index, delete

   # 1) collect metadata into a catalog
   add(
       store="/tmp/catalog.jsonl",
       config_fle="/path/to/drs_config.toml",
       data_object=["/path/to/data"],
       catalogue_backend="jsonlines",
       threads=8,
       batch_size=50,
   )

   # 2) index the catalog into a Apache Solr core named 'latest'
   index(
       "solr",
       "/tmp/catalog-1.yml",
       "/tmp/catalog-2.yml",
       batch_size=50,
   )

   # 3) optionally delete entries from the index
   delete(
       "mongo",
       url="mongodb://mongo:secret@localhost:27017",
       database="metadata",
       latest_version="latest",
       facets=[("project", "CMIP6"), ("institute", "MPI-M")],
   )

Asynchronous usage
^^^^^^^^^^^^^^^^^^^

For applications that already run an event loop, metadata‑crawler
provides async counterparts to the functions above.  They are named
``async_add``, ``async_index`` and ``async_delete``.  These
coroutines can be awaited directly or scheduled concurrently with
other tasks:

.. code-block:: python

   import asyncio
   from metadata_crawler import async_add, async_index, async_delete


   async def main():
       # crawl metadata from one or more data objects or datasets
       await async_add(
           store="/tmp/catalog.yaml",
           config_file="/path/to/",
           data_set=["cmip6-fs", "obs-fs"],
           threads=8,
           batch_size=50,
       )

       # index into a MongoDB backend named 'latest'
       await async_index(
           "mongo" "/tmp/catalog-1.yml",
           "/tmp/catalog-2.yml",
           config_file="/path/to/drs_config.toml",
           url="mongodb://localhost:27017",
           database="metadata",
           threads=8,
           batch_size=50,
       )

       # delete entries matching a wildcard pattern (glob translated to regex)
       await async_delete(
           "solr",
           server="localhost:8983",
           latest_version="latest",
           facets=[("file", "*.nc"), ("project", "OBS")],
       )


   asyncio.run(main())

Library Reference
-----------------

.. automodule:: metadata_crawler
   :exclude-members: DataCollector
   :member-order: bysource
