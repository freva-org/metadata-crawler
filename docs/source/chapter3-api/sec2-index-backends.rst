.. _add_backends:

Custom index backends
---------------------

An *index system* is where the metadata ends up for searching, for example
Apache Solr or MongoDB. It reads the records from one or more metadata stores
(the source of truth) and writes them into its own indexes. You can add your
own index system as a plugin.

Base class
^^^^^^^^^^

Index systems subclass :class:`~metadata_crawler.api.index.BaseIndex`. The base
class opens the metadata stores, so an implementation only deals with its own
system:

* ``index(**options)`` reads the records with
  ``async for batch in self.get_metadata(name)`` for each name in
  ``self.index_names`` (``latest`` and ``files``) and writes them.
* ``delete(**options)`` removes the records matching some facets.
* ``__post_init__()`` sets up anything the instance needs. It runs at the end
  of ``__init__``.
* ``self.index_schema`` describes the facets and their types (see
  :doc:`../chapter2-config/index`), for creating tables or documents.
* ``self.target`` is the connection to the index system, if one was given
  (see below).

The keyword-only parameters of ``index`` and ``delete`` become the options of
``mdc <name> index`` and ``mdc <name> delete`` (see `Extending the CLI`_).

The built-in index systems are ``SolrIndex`` (``mdc solr ...``) and
``MongoIndex`` (``mdc mongo ...``).

Connections for your index system
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 2610.0.0

Users can describe where your index system is, including credentials, in
``connections.toml``/``secrets.toml`` (see :ref:`connections`) and pass the
name instead of an address: ``mdc solr index prod --server solr-prod``. Three
things make an index system support this:

1. **A connection model.** Subclass
   :class:`~metadata_crawler.api.stores.base.BaseConnection` and give it a
   unique ``backend`` name. Every field becomes a key users can set in
   ``connections.toml``; credentials (``username``, ``password``) come from
   :class:`~metadata_crawler.api.stores.base.Credentials` and are masked.
   Defining the class registers it. Leave ``schemes`` empty if your URLs look
   like other backends' (plain ``http(s)://``, say); users then set
   ``backend = "<name>"`` in their entry.
2. **The link from the index system.** Set the class attribute
   ``connection = YourConnection``.
3. **The option that takes it.** Mark the option of ``index`` and ``delete``
   that says where the system is with ``cli_parameter(..., connection=True)``.
   It then accepts a connection name as well as an address. A configured name
   is turned into the connection, which reaches your instance as
   ``self.target``; the option itself is then ``None``. Fall back to
   ``self.target`` for every option the user didn't give, so that explicit
   options win.

The connection is checked to be an instance of ``connection`` before your
``__post_init__`` runs, so there ``self.target`` is either ``None`` or your
model.

Because users list connections for every backend in one file, your models
must be importable whenever the file is read. metadata-crawler therefore
imports the modules of all ``metadata_crawler.ingester`` entry points when
it validates connections, and skips those whose dependencies are missing.
Define the connection model in the module your entry point names (or one it
imports).

Example
^^^^^^^

A MySQL index system with named connections:

.. code-block:: python

    from typing import Annotated, Any, ClassVar, Dict, List, Optional, Tuple, cast

    from metadata_crawler.api.cli import cli_function, cli_parameter
    from metadata_crawler.api.index import BaseIndex
    from metadata_crawler.api.stores.base import BaseConnection, Credentials


    class MySQLConnection(BaseConnection, Credentials):
        """A MySQL server (``mysql://host:port/database``)."""

        backend: ClassVar[str] = "mysql"
        """Name of the backend type used for validation."""

        schemes: ClassVar = frozenset({"mysql"})
        """Valid schemas, used to deduce the backend ``myslq://host:port/dev``"""

        database: str = "metadata"
        """Default database name."""

        @property
        def store_uri(self) -> str:
            return self.url


    class MySQLIndex(BaseIndex):
        """Index metadata into MySQL."""

        connection = MySQLConnection

        def _where(self, server: Optional[str]) -> Tuple[str, Dict[str, Any]]:
            """Explicit options win; the rest comes from the target."""
            target = cast(Optional[MySQLConnection], self.target)
            if server or target is None:
                return server or "mysql://localhost/metadata", {}
            password = target.password.get_secret_value() if target.password else None
            return target.store_uri, {"user": target.username, "password": password}

        @cli_function(help="Index metadata into MySQL.")
        async def index(
            self,
            *,
            server: Annotated[
                Optional[str],
                cli_parameter(
                    "--server",
                    help="URL of the server, or the name of a mysql connection",
                    type=str,
                    connection=True,
                ),
            ] = None,
        ) -> None:
            url, credentials = self._where(server)
            async with connect(url, **credentials) as con:  # your driver
                for table in self.index_names:
                    async for batch in self.get_metadata(table):
                        await con.upsert(table, batch)

        @cli_function(help="Remove metadata from MySQL.")
        async def delete(
            self,
            *,
            server: Annotated[
                Optional[str],
                cli_parameter(
                    "--server",
                    help="URL of the server, or the name of a mysql connection",
                    type=str,
                    connection=True,
                ),
            ] = None,
            facets: Annotated[
                Optional[List[Tuple[str, str]]],
                cli_parameter(
                    "-f", "--facets", nargs=2, action="append", help="Facets."
                ),
            ] = None,
        ) -> None:
            url, credentials = self._where(server)
            ...

.. admonition:: pyproject.toml

    .. code-block:: toml

        [project.entry-points."metadata_crawler.ingester"]
        mysql = "my_package.my_index:MySQLIndex"

Users can then write

.. code-block:: toml

    # connections.toml
    [search-db]
    url = "mysql://db.example.org:3306/search"

.. code-block:: toml

    # secrets.toml
    [search-db]
    username = "indexer"
    password = "..."

and run ``mdc mysql index prod --server search-db``, or in Python
``index("mysql", store, server=config["search-db"])``.

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

1. **Decorate** the ``index`` and ``delete`` methods of your :ref:`index system <add_backends>`.
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
   from metadata_crawler.api.cli import cli_function, cli_parameter


   @cli_function(help="Index data in MySQL")
   def index(
       self,
       server: Annotated[str, cli_parameter("--server", help="Server name")],
       user: Annotated[Optional[str], cli_parameter("--user", help="User name")] = None,
       db: Annotated[str, cli_parameter("--database", help="Database name")] = "foo",
       pw: Annotated[
           bool,
           cli_parameter("--password", "-p", action="store_true", help="Ask for password"),
       ] = False,
   ) -> None:
       """Your index implementation here."""

.. note::

    The arguments and keyword arguments of the ``cli_parameter`` method
    follow the logic of `argparse.ArgumentParser.add_argument <https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.add_argument>`_.
    The only addition is ``connection=True``, which marks the option that also
    accepts a connection name (see `Connections for your index system`_).

When you run ``metadata-crawler mysql index --server localhost -p``
the function executes your custom logic.

.. automodule:: metadata_crawler.api.cli
   :exclude-members: Parameter

API Reference
-------------

.. autoclass:: metadata_crawler.api.index.BaseIndex
