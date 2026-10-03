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
   into a permanent source of truth (intake catalogue, database).
2. **Indexing**: read entries from the source of truth and write them to the
   configured index backend (e.g. Apache Solr or MongoDB).
3. **Deleting**: remove previously indexed entries matching a set
   of search facets (optional).

Below is a minimal example that crawls data from a local directory,
stores it in a metadata store, and indexes it to Apache Solr:

.. code-block:: python

   from metadata_crawler import add, index, delete, remove

   # 1) collect metadata into a catalog
   add(
       "/path/to/drs_config.toml",
       "/path/to/second/drs_config.toml",
       store="/tmp/catalog.yml",
       data_object=["/path/to/data"],
       backend="jsonlines",
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

   # 4) optionally remove data from the source of truth so it is not indexed
   #    anymore
   #    (same key: OR, different keys: AND; dry_run=True only counts)
   removed_items = remove(
       "mongodb://mongo:secret@localhost:27017/metadata",
       facets=[("project", "CMIP6"), ("institute", "MPI-M")],
   )


.. versionchanged:: 2511.0.0

   The catalogue argument ``store`` of :func:`~metadata_crawler.add`
   has been rearranged and is now a keyword argument:
   ``add("data.yaml", "drs-config.toml")`` becomes
   ``add("drs-config.toml", store="data.yaml")``. If the ``store`` keyword
   is omitted the output catalogue will be interpreted as config file.

.. versionadded:: 2605.0.0

    Instead of writing to file-based ``intake`` catalogues, metadata can be
    crawled directly into a **MongoDB** or **PostgreSQL** database. Database
    backends store catalogue metadata internally, so no YAML catalogue file
    is needed. The backend is detected automatically from the URL scheme.


.. versionadded:: 2610.0.0

    Search facets can be used to create subset of the metadata added to the source
    of truth (databases or intake catalogues). Sometimes it might be necessary to
    delete certain data from source of truth. To select datasets that should be
    deleted from an intake catalogue or database the ``remove`` method can
    be used.



**MongoDB as data store:**

.. code-block:: python

    add(
       "/path/to/drs_config.toml",
       "/path/to/second/drs_config.toml",
       store="mongodb://server:27017/databasename",
       storage_options={"username": "user", "password": "secret"},
       data_object=["/path/to/data"],
       batch_size=50,
    )

**PostgreSQL as data store:**

.. code-block:: python

    add(
       "/path/to/drs_config.toml",
       "/path/to/second/drs_config.toml",
       store="postgresql://server:5432/databasename",
       storage_options={"username": "user", "password": "secret"},
       data_object=["/path/to/data"],
       batch_size=50,
    )

The backend is derived from the URL scheme; ``backend=...`` is only needed
when it can't be.

**Named connections:**

.. versionadded:: 2610.0.0

Every function that takes a store also accepts a connection object, for
example one defined in ``connections.toml`` (see :ref:`connections`):

.. code-block:: python

    from metadata_crawler.connections import read_configfiles

    with read_configfiles() as config:
        add("/path/to/drs_config.toml", store=config["prod"], data_set="cmip6-fs")
        index("solr", config["prod"], server="localhost:8983")




Asynchronous usage
^^^^^^^^^^^^^^^^^^^

For applications that already run an event loop, metadata‑crawler
provides async counterparts to the functions above.  They are named
``async_add``, ``async_index``, ``async_remove`` and ``async_delete``.  These
coroutines can be awaited directly or scheduled concurrently with
other tasks:

.. code-block:: python

   import asyncio
   from metadata_crawler import async_add, async_index, async_delete


   async def main():
       # crawl metadata from one or more data objects or datasets
       await async_add(
           "/path/to/drs_config.toml",
           store="/tmp/catalog.yaml",
           data_set=["cmip6-fs", "obs-fs"],
           batch_size=50,
       )

       # index into a MongoDB backend named 'latest'
       await async_index(
           "mongo",
           "/tmp/catalog-1.yml",
           "/tmp/catalog-2.yml",
           config_file="/path/to/drs_config.toml",
           url="mongodb://localhost:27017",
           database="metadata",
           batch_size=50,
       )

       # delete entries matching a wildcard pattern (glob translated to regex)
       await async_delete(
           "solr",
           server="localhost:8983",
           latest_version="latest",
           facets=[("file", "*.nc"), ("project", "OBS")],
       )
       # optionally remove data from the source of truth so it is not indexed
       #    anymore
       removed_items = await async_remove(
           "mongodb://mongo:secret@localhost:27017/metadata",
           facets=[("project", "CMIP6"), ("institute", "MPI-M")],
       )



   asyncio.run(main())

.. versionchanged:: 2511.0.0

   The catalogue argument ``store`` of :func:`~metadata_crawler.async_add`
   has been rearranged and is now a keyword argument:
   ``async_add("data.yaml", "drs-config.toml")`` becomes
   ``async_add("drs-config.toml", store="data.yaml")``. If the ``store`` keyword
   is omitted the output catalogue will be interpreted as config file.

.. versionadded:: 2605.0.0

    Instead of writing to file-based ``intake`` catalogues, metadata can be
    crawled directly into a **MongoDB** or **PostgreSQL** database. Database
    backends store catalogue metadata internally, so no YAML catalogue file
    is needed. The backend is detected automatically from the URL scheme.

.. versionadded:: 2610.0.0

    Search facets can be used to create subset of the metadata added to the source
    of truth (databases or intake catalogues). Sometimes it might be necessary to
    delete certain data from source of truth. To select datasets that should be
    deleted from an intake catalogue or database the ``remove`` method can
    be used.



Library Reference
-----------------

.. automodule:: metadata_crawler
   :exclude-members: DataCollector
   :member-order: bysource
