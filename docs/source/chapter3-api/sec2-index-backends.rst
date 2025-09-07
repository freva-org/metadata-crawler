.. _add_backends:

Custom index backends
---------------------

An *index backend* stores the final, translated metadata records.
Built‑in backends include a **JSONLines** data structure (either on disk,
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

SolrIndex
^^^^^^^^^

``SolrIndex`` indexes metadata into a Apache Solr.  When
initialised you specify the solr server and the core names to
create (``latest``, ``files``, etc.).  The schema is
derived from the configuration.  The store supports two modes:

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

Recipe: Implementing a custom index
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add a new index backend:

1. **Subclass** ``IndexStore`` and implement the abstract methods
   ``index`` to add and ``delete`` records.
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

    import os
    from typing import Any, Dict, Iterator, List, Optional, Tuple

    from metadata_crawler.metadata_stores import IndexStore


    class MySQLIndex(IndexStore):
        def __post_init__(self):
            """Any additional attributes can be set in this method."""

            self.password = os.getenv("MYSQL_PASSWD") or ""

        async def index(
            self, server: Optional[str] = None, user: Optional[str] = None, pw: bool = True
        ) -> None:
            """insert or upsert records."""
            if pw and not self.password:
                self.password = getpass("Give DB password: ")
            with self.db_connection(server, user, self.password) as con:
                for table in self.index_names:
                    async for chnunk in self.get_metadata(index):
                        con.add(chunk)

        async def delete(
            self,
            facets: Optional[List[Tuple[str, str]]] = None,
            server: Optional[str] = None,
            user: Optional[str] = None,
            pw: bool = True,
        ) -> None:
            """remove matching records."""
            if pw and not self.password:
                self.password = getpass("Give DB password: ")
            with self.db_connection(server, user, self.password) as con:
                for table in self.index_names:
                    con.delete(**dir(facets))

.. admonition:: pyproject.toml

    .. code-block:: toml

        # register in pyproject.toml
        [project.entry-points."metadata_crawler.index_backends"]
        mysql = "my_package.my_index:MySQLIndex"

Extending the CLI
^^^^^^^^^^^^^^^^^^

The CLI entry point ``metadata-crawler`` registers its commands in ``cli.py``.
You can extend the CLI by defining new commands or options and registering
them. This registration is inspired by the `Typer <https://typer.tiangolo.com/>`_
library.

CLI API
********


``cli.py`` defines decorators ``@cli_function`` and the ``cli_parameter`` method
to annotate functions with help messages and parameter metadata.  The
actual CLI commands are defined in your :ref:`add_backends`  via the
``@cli_function`` decorator.  To add a new command:

1. **Decorate** the ``index`` and ``delete`` functions in our :ref:`add_backends`
   Use the ``@cli_function`` decorator to register it.
2. **Annotate** the function parameters with ``Annotated`` and
   ``cli_parameter`` to supply CLI options (see ``SolrIndex`` for
   examples).
3. **Registering** Once decorated the registering will happen automatically.

Example: adding a cli for the ``MySQL`` Index
**********************************************

The  MySQL index backend from above can be turned to a CLI as follows:

.. code-block:: python

   from typing import Optional
   from typing_extensions import Annotated
   from .metadata_stores import IndexStore


   @cli_function(help="Index data in MySQL")
   def index(
       self,
       server: Annotated[str, cli_parameter("--server", help="Server name")],
       user: Annotate[Optional[str], cli_parameter("--user", help="User name")] = None,
       db: Annotated[str, cli_parameter("--database", help="Database name")] = "foo",
       pw: Annotate[
           bool,
           cli_parmeter("--password", "-p", action="store_true", help="Ask for password"),
       ] = False,
   ) -> None:
       """Your index implementation here."""

.. note::

    The arguments and keyword arguments of th e``cli_parameter`` method
    follow the logic of `argparse.ArgumentParser.add_argument <https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.add_argument>`_.

When you run ``metadata-crawler mysql --server localhost -p``
the function executes your custom logic.

.. automodule:: metadata_crawler.api.cli
   :exclude-members: Parameter

API Reference
-------------

.. autoclass:: metadata_crawler.api.index.BaseIndex
