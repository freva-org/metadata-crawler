API Reference
=============

This chapter documents the extension points of metadata‑crawler.  It
describes how to implement custom storage backends, how to add new
index systems, and how to provide CLI extensions.  The public API
consists of abstract base classes and helper functions defined in
``storage_backend.py``, ``metadata_stores.py``, and ``index.py``.


To make any custom implementations available inside ``metadata-crawler`` you
need to `create entry points <https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-package-metadata>`_
in your ``pyproject.toml`` file:

.. admonition:: pyproject.toml

    .. code-block:: toml

        # register a new index system (mdc mybackend index ...)
        [project.entry-points."metadata_crawler.ingester"]
        mybackend = "my_package.my_index:MyIndex"

        # register your storage backend
        [project.entry-points."metadata_crawler.storage"]
        foo = "my_package.foo_backend:FooBackend"




.. toctree::
   :maxdepth: 2

   sec1-storage-backends
   sec2-index-backends
