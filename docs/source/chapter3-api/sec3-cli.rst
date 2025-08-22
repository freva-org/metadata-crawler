Extending the CLI
-----------------

The CLI entry point ``metadata-crawler`` registers its commands in ``cli.py``.
You can extend the CLI by defining new commands or options and registering
them. This registration is inspired by the `Typer <https://typer.tiangolo.com/>`_
library.

Structure of ``cli.py``
^^^^^^^^^^^^^^^^^^^^^^^

``cli.py`` defines decorators ``@cli_function`` and the ``cli_parameter`` method
to annotate functions with help messages and parameter metadata.  The
actual CLI commands are defined in your :ref:`add_backends`  via the
``@cli_function`` decorator.  To add a new command:

1. **Decorate** the ``index`` and ``delete`` functions in our :ref:`add_backends`
   Use the ``@cli_function`` decorator to register it.
2. **Annotate** the function parameters with ``Annotated`` and
   ``cli_parameter`` to supply CLI options (see ``SolrIndex`` for
   examples).
3. **Registering** Once decorated the registering will happend automatically.

Example: adding a ``MySQL`` database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Suppose you've definde a MySQL backend and want to add a CLI method for indexing.
You could implement it as follows:

.. code-block:: python

   from typing import Optinal
   from typing_extensions import Annotated
   from .metadata_stores import IndexStore

   @cli_function(help="Index data in MySQL")
   def index(
       server: Annotated[str, cli_parameter("--server", help="Server name")],
       user: Annotate[Optional[str], cli_parameter("--user", help="User name")] = None,
       db: Annotated[str, cli_parameter("--database", help="Database name")] = "foo",
       pw: Annotate[bool, cli_parmeter("--password", "-p", action="store_true", help="Ask for password")] = False,
   ) -> None:
       """Index."""

.. note::

    The arguments and keyword arguments of th e``cli_parameter`` method
    follow the logic of `argparse.ArgumentParser.add_argument <https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.add_argument>`_.

When you run ``metadata-crawler mysql --server localhost -p``
the function executes your custom logic.

.. automodule:: metadata_crawler.api.cli
   :exclude-members: Parameter
