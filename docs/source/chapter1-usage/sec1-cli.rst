Command‑line interface
----------------------

The software installs a console entry point named
``metadata-crawler`` or ``mdc`` that exposes the high‑level subcommands:

* ``add``  – Collect metadata into a permanent source of truth.
* ``remove`` – Remove a subset of the metadata from the source of truth.
* ``config`` – Display the DRS configuration.
* ``glance`` – Get an overview over the crawled metadata in a metadata store.
* ``init-config`` – Create templates for named connections and secrets
  (see :ref:`connections`).
* ``solr``   – Index and delete metadata to/from Apache Solr.
* ``mongo``  – Index and delete metadata to/from MongoDB.
* ``walk-intake`` – Convenience module to traverse and check intake catalogues.

Use ``--help`` on any command to see available options.  Below are
some examples.

Wherever a command expects a metadata store (``add``, ``remove``, ``glance``
and ``<index system> index``) you can pass a path, a URL or the name of a
connection defined in ``connections.toml``. Two options, accepted before or
after the sub-command, select other connection files:

``--mdc-config``
    Path to the connections file (default: ``MDC_CONFIG_PATH`` or
    ``~/.config/metadata-crawler/connections.toml``).
``--mdc-secrets``
    Path to the secrets file (default: ``MDC_SECRETS_PATH`` or
    ``~/.config/metadata-crawler/secrets.toml``).

Basic crawling
^^^^^^^^^^^^^^

To harvest a directory of files into a meta data store (multiple config
files are supported since `v2511.0.0`):

.. code-block:: console

   mdc add \
        /tmp/cat.yml \
       -c /path/to/drs_config-1.toml \
       -c /path/to/drs_config-2.toml \
       --catalogue-backend jsonlines \
       --n-procs 4 \
       --batch-size 100 \
       --data-object /path/to/data

Alternatively you can provide one or more dataset names defined in
your DRS configuration instead of explicit file paths
(glob pattern for config files are also supported since `v2511.0.0`):

.. code-block:: console

   metadata-crawler add \
       /tmp/catalog.yaml \
       -c '/path/to/drs_*.toml' \
       --data-set cmip6-fs --data-set obs-fs

Dataset names may contain wildcards, e.g. ``-ds 'cmip6-*'``.


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

The most convenient way is a named connection with its credentials in
``secrets.toml`` (see :ref:`connections`):

.. code-block:: console

   mdc add mongo -c /path/to/drs_config.toml --data-object /path/to/data

The ``--table`` / ``--collection`` / ``--prefix`` flag controls the
table or collection name prefix (defaults to ``metadata``).

.. note::

   Database backends require optional dependencies:
   ``pymongo`` for MongoDB, ``sqlalchemy`` and ``psycopg`` for PostgreSQL.
   Encrypted secrets files need ``pyrage`` (``metadata-crawler[vault]``).


Removing metadata from a source of truth
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 2610.0.0

    Search facets can be used to create subset of the metadata added to the source
    of truth (databases or intake catalogues). Sometimes it might be necessary to
    delete certain data from source of truth. To select datasets that should be
    deleted from an intake catalogue or database the ``remove`` sub command can
    be used.


.. code-block:: console

   export MDC_STORAGE_OPTIONS="username:metadata,password:secret"
   mdc remove mongodb://localhost:27017 -f variable tas -f time_frequency 1hr -f project cmip6

   # count what would be removed, using a named connection
   mdc remove prod -f variable tas -f variable pr --dry-run

To select the subsets the ``-f/--facets`` flags can be used. Each flag takes
two values, the facet *key* and the target *value*:

* values given for the **same key** are combined with logical **OR**
  (``-f variable tas -f variable pr``: tas or pr),
* **different keys** are combined with logical **AND**
  (``-f variable tas -f project cmip6``: tas in cmip6).

Keys and string values are compared in lower case, and string values may
contain the wildcards ``*`` and ``?``. Keys that are not part of the metadata
schema, as well as time facets, are ignored with a warning. Without any
usable facet nothing is removed. ``--dry-run`` only counts the matching
entries.

The command works for databases and intake catalogues alike.


Indexing
^^^^^^^^

Once a catalog has been generated you can index it into a backend.
Apache Solr and MongoDB backends are supported out of the box.  The
following example indexes a catalogue into the Solr cores ``latest`` and
``files``:

.. code-block:: console

   metadata-crawler solr index \
       /tmp/catalog.yml \
       --server localhost:8983

The stores to index can be paths, URLs, glob patterns or names of
connections (``mdc solr index prod --server localhost:8983``).

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
