Adding storage backends
-----------------------

Storage backends provide the abstraction for traversing files and
directories, reading datasets and extracting metadata.  The crawler
ships with built‑in backends for Posix, S3/MinIO, Swift and Intake.
You can add your own backend by subclassing
``PathTemplate`` defined in ``storage_backend.py``.

Base classes
^^^^^^^^^^^^

The core classes used for storage backends are:

* ``TemplateMixin`` – Provides a ``storage_template`` method to
  render strings using Jinja2.  Useful when the backend requires
  templated URLs or credentials.
* ``PathMixin`` – Provides convenience methods ``suffix`` and
  ``name`` to extract parts of a path using ``anyio.Path``.
* ``PathTemplate`` – Abstract base class combining
  ``TemplateMixin`` , ``LookupMixin`` and ``PathMixin``.  Concrete backends must
  implement asynchronous methods ``is_dir``, ``is_file``, ``iterdir``,
  ``rglob`` and synchronous methods ``path`` and ``uri``.  Optional
  overrides include ``open_dataset`` (open an xarray dataset given
  a URI) and ``read_attr`` (read a metadata attribute).

Recipe: Implement a backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To implement a new backend:

1. **Subclass** ``PathTemplate`` and set the class variable ``_fs_type`` to a
   short name identifying your backend.
2. **Implement** the abstract methods:
    * ``is_dir(path)`` should return True if the given URI is a directory/prefix.
    * ``is_file(path)`` should return True if the given URI is a file/object containing data.
    * ``iterdir(path)`` should asynchronously iterate over immediate children (directories and files) of the given path.
    * ``rglob(path, glob_pattern="*")`` should asynchronously yield ``Metadata`` objects for all files matching the glob pattern.
    * ``path(path)`` should return a URI with scheme/authority as required by your backend.
    * ``uri(path)`` should return the raw URI (including bucket or container names as appropriate).
3. **Register** your backend by adding it to the entry point group
    * ``metadata_crawler.storage_backends`` in your ``setup.cfg`` or
    * ``pyproject.toml``.  This allows the ``fs_type`` string in the configuration to resolve to your backend class.

    .. admonition:: pyproject.toml

        .. code-block:: toml

            [project.entry-points."metadata_crawler.storage_backends"]
            foo = "my_package.foo_backend:FooBackend"


Example skeleton
^^^^^^^^^^^^^^^^

Here is a minimal example of a custom storage backend for a
hypothetical ``foo`` protocol:

.. code-block:: python

   from metadata_crawler.storage_backend import PathTemplate, Metadata
   from anyio import Path


   class FooBackend(PathTemplate):
       _fs_type = "foo"

       async def is_dir(self, path: str) -> bool:
           # implement logic to check for a directory
           ...

       async def is_file(self, path: str) -> bool:
           # implement logic to check for a file
           ...

       async def iterdir(self, path: str):
           # yield child names
           ...

       async def rglob(self, path: str, glob_pattern: str = "*"):
           # recursively yield Metadata objects
           for child in await self.iterdir(path):
               if await self.is_dir(child):
                   async for item in self.rglob(child, glob_pattern):
                       yield item
               elif await self.is_file(child) and fnmatch(child, glob_pattern):
                   yield Metadata(path=child)

       def path(self, path: str) -> str:
           return f"foo://{path}"

       def uri(self, path: str) -> str:
           return self.path(path)


.. code-block:: toml

   # Then register in your packaging config:
   [project.entry-points."metadata_crawler.storage_backends"]
   foo = "my_package.foo_backend:FooBackend"

Once registered, you can set ``fs_type = "foo"`` in a dataset
definition and optionally provide ``storage_options`` that will be
passed into your backend’s constructor.

API Reference:
--------------

.. autoclass:: metadata_crawler.api.storage_backend.PathTemplate
   :inherited-members: False

.. autoclass:: metadata_crawler.api.mixin.PathMixin
   :inherited-members: False

.. autoclass:: metadata_crawler.api.mixin.TemplateMixin
   :inherited-members: False

.. autoclass:: metadata_crawler.api.mixin.LookupMixin
