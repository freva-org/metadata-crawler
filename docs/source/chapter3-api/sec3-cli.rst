Extending the CLI
-----------------

metadata‑crawler’s command–line interface is built using the
`Typer <https://typer.tiangolo.com/>`_ library.  The CLI entry point
``metadata-crawler`` registers its commands in ``cli.py``.  You can
extend the CLI by defining new commands or options and registering
them with Typer.

Structure of ``cli.py``
^^^^^^^^^^^^^^^^^^^^^^^

``cli.py`` defines decorators ``@cli_parameter`` and ``@cli_function``
to annotate functions with help messages and parameter metadata.  The
actual CLI commands are defined in ``run.py`` using Typer groups and
are imported into ``cli.py``.  To add a new command:

1. **Define** a function in ``run.py`` (or another module) that
   performs your desired operation.  Use Typer’s ``@app.command``
   decorator to register it.
2. **Annotate** the function parameters with ``Annotated`` and
   ``cli_parameter`` to supply CLI options (see ``run.py`` for
   examples).
3. **Register** the command in the Typer app defined in ``cli.py``.

Example: adding a ``stats`` command
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Suppose you want a command that prints the number of records in an
index.  You could implement it as follows:

.. code-block:: python

   # in run.py
   import typer
   from typing_extensions import Annotated
   from .metadata_stores import IndexStore

   app = typer.Typer()

   @app.command()
   def stats(
       index_name: Annotated[str, cli_parameter("--index-name", help="Index to inspect")],
       index_backend: Annotated[str, cli_parameter("--index-backend", help="Backend type")],
       **common_kwargs,
   ) -> None:
       """Show record counts for a given index."""
       store = load_index_store(index_backend, index_name, **common_kwargs)
       count = sum(1 for _ in store.read(index_name))
       print(f"Index {index_name} contains {count} records")

   # import the command in cli.py
   from .run import app as run_app
   app = typer.Typer()
   app.add_typer(run_app, name="run")

When you run ``metadata-crawler stats --index-name latest --index-backend duckdb``
the function executes your custom logic.

For more details on Typer, refer to its official documentation.
