Command‑line interface
----------------------

The software installs a console entry point named
``metadata-crawler`` or ``mdc`` that exposes the high‑level subcommands:

* ``add``  – Collect metadata into a temporary catalog.
* ``config`` – Display general configuration
* ``glance`` – Get an overview over the crawled metadata in a metadata store.
* ``solr``   - Index and delete metadata to/from Apache solr.
* ``mongo``  – Index and deleta metadata to/from MongoDB.
* ``walk-intake`` – Convenience module to traverse and check intake catalogues.

Use ``--help`` on any command to see available options.  Below are
some examples.

Basic crawling
^^^^^^^^^^^^^^

To harvest a directory of files into a meta data store (multiple config
files are supported since `v2511.0.0`):

.. code-block:: console

   mdc add \
        /tmp/cat.yml \
       -c /path/to/drs_config-1.toml \
       -c /path/to/drs_config-1.toml \
       --catalogue-backend jsonlines \
       --threads 4 \
       --batch-size 100 \
       --data-object /path/to/data

Alternatively you can provide one or more dataset names defined in
your DRS configuration instead of explicit file paths
(glob pattern for config files are also supported since `v2511.0.0`):

.. code-block:: console

   metadata-crawler add \
       /tmp/catalog.yaml \
       -c /path/to/drs_*.toml \
       --data-set cmip6-fs obs-fs


.. versionchanged:: 2511.0.0

   The ``metadata-crawler add`` sub commands support multiple config files
   and glob pattern of config files.

Crawling into databases
^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 2605.0.0

    Instead of writing to file-based ``intake`` catalogues, metadata can be
    crawled directly into a **MongoDB** or **PostgreSQL** database. Database
    backends store catalogue metadata internally, so no YAML catalogue file
    is needed. The backend is detected automatically from the URL scheme.

**MongoDB:**

.. code-block:: console

   mdc add \
       mongodb://localhost:27017 \
       -c /path/to/drs_config.toml \
       --data-object /path/to/data \
       -s username metadata \
       -s password secret \
       -s database metadata

**PostgreSQL:**

.. code-block:: console

   mdc add \
       postgresql://localhost:5432/metadata \
       -c /path/to/drs_config.toml \
       --data-object /path/to/data \
       -s username metadata \
       -s password secret

Credentials can also be provided via the ``MDC_STORAGE_OPTIONS``
environment variable to keep them out of the command line and shell
history:

.. code-block:: console

   export MDC_STORAGE_OPTIONS="username:metadata,password:secret"
   mdc add mongodb://localhost:27017 -c /path/to/drs_config.toml --data-object /path/to/data

The ``--table`` / ``--collection`` / ``--prefix`` flag controls the
table or collection name prefix (defaults to ``metadata``).

.. note::

   Database backends require optional dependencies:
   ``pymongo`` for MongoDB, ``sqlalchemy`` and ``psycopg`` for PostgreSQL.


Indexing
^^^^^^^^

Once a catalog has been generated you can index it into a backend.
Apache Slor and MongoDB backends are supported out of the box.  The
following example writes to a json.gz file and index named ``latest``:

.. code-block:: console

   metadata-crawler solr index \
       /tmp/catalog.yml \
       --server localhost:8983

For MongoDB, supply the database URL and name:

.. code-block:: console

   metadata-crawler mongo index \
       /tmp/catalog.yml /tmp/catalog-2.yml \
       --url mongodb://localhost:27017 \
       --database metadata


Blue/green index rotation
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 2607.0.0

   The ``index`` command can rotate its target atomically, so queries never
   see a half-built index during a re-index.

Passing ``--rotate`` (alias ``--blue-green``) indexes into a fresh, empty
core/collection and only promotes it into production once indexing has
finished and passed a sanity check. The previously live data is dropped in
the same atomic step, giving a zero-downtime re-index:

.. code-block:: console

   # Apache Solr
   metadata-crawler solr index \
       /tmp/catalog.yml \
       --server localhost:8983 \
       --rotate \
       --configset freva \
       --min-docs 1

   # MongoDB
   metadata-crawler mongo index \
       /tmp/catalog.yml \
       --url mongodb://localhost:27017 \
       --database metadata \
       --rotate \
       --min-docs 1

How it works:

* A uniquely named temporary index (the ``latest``/``files`` names with a
  timestamp suffix) is created and populated.
* After a commit the new index is validated. If any target holds fewer than
  ``--min-docs`` documents the rotation is **aborted**, the temporary index is
  dropped, and the live index is left untouched.
* Otherwise the temporary index is promoted atomically — for Solr a ``SWAP``
  followed by ``UNLOAD`` of the old core, for MongoDB a ``renameCollection``
  with ``dropTarget`` — and the previous data is removed. On a first
  deployment (no live index yet) the new index is simply renamed into place.

Options:

``--rotate`` / ``--blue-green``
    Enable the rotation. Without it, ``index`` writes into the live
    ``latest``/``files`` targets directly.
``--configset`` *(Solr only, default* ``freva`` *)*
    The Solr configset used to create the temporary cores. It must already
    exist on the Solr server.
``--min-docs`` *(default* ``1`` *)*
    Abort the rotation if a freshly built index holds fewer than this many
    documents. Guards against promoting an empty or half-crawled index over
    good production data.
``--index-suffix``
    Override the auto-generated temporary-index suffix. Rarely needed; the
    default timestamp keeps back-to-back rotations from colliding.

.. note::

   For Solr the ``--configset`` must be available on the server or core
   creation fails. MongoDB needs no configset.

Deleting
^^^^^^^^

The ``delete`` command removes documents from the index using one or
more facet filters.  Facet values may contain shell wild cards
(``*`` and ``?``) which are translated to MongoDB regular expressions
(Apache Solr deletion uses filters internally).  For example:

.. code-block:: console

   metadata-crawler mongo delete \
       --url mongodb://localhost:27017 \
       --database metadata \
       -f project CMIP6 -f file "*.nc"

See ``metadata-crawler --help`` for a complete list of options.
