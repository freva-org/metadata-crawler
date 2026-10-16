Harvest your climate metadata
=============================

.. image:: https://img.shields.io/badge/License-BSD-purple.svg
   :target: LICENSE

.. image:: https://img.shields.io/pypi/pyversions/freva-client.svg
   :target: https://pypi.org/project/freva-client

.. image:: https://img.shields.io/badge/ViewOn-GitHub-purple
   :target: https://github.com/freva-org/metadata-crawler

.. image:: https://github.com/freva-org/metadata-crawler/actions/workflows/ci_job.yml/badge.svg
   :target: https://github.com/freva-org/metadata-crawler/actions

.. image:: https://codecov.io/gh/freva-org/metadata-crawler/graph/badge.svg?token=W2YziDnh2N
   :target: https://codecov.io/gh/freva-org/metadata-crawler


Overview
--------

**Metadata Crawler** is a tool for harvesting, normalising, and indexing
metadata from climate and earth‑system datasets stored on POSIX file
systems, S3/MinIO object stores, or OpenStack Swift. The software is highly
configurable: dataset definitions, directory and filename patterns, and
metadata extraction are controlled via TOML configuration files.
You can use the asynchronous and synchronous Python APIs directly or drive
everything through a command‑line interface (CLI).

Installation & Quick Start
--------------------------

Install via `pip <https://pypi.org>`_ or `conda-forge <https://conda-forge.org>`_:

.. code-block:: console

    python -m pip install metadata-crawler
    conda install -c conda-forge metadata-crawler

After installation, use the CLI immediately (see TL;DR below) or import
the modules in your own code.

Too long; didn't read (TL;DR)
------------------------------

.. code-block:: console

    mdc add s3://freva/metadata-crawler/data.yml -c drs-config.toml -ds 'xces-*'
    mdc solr index s3://freva/metadata-crawler/data.yml --server localhost:8983

    # or give your stores a name, with credentials kept in a separate file
    mdc init-config
    mdc add prod -c drs-config.toml -ds 'xces-*'
    mdc solr index prod --server localhost:8983


- **Multi-backend discovery**: POSIX, S3/MinIO, Swift (async REST), Intake
- **Two-stage pipeline**: *crawl → source of truth* then *source of truth → index*
- **Schema driven**: strong types (e.g. ``string``, ``datetime[2]``,
  ``float[4]``, ``string[]``)
- **DRS dialects**: packaged CMIP6/CMIP5/CORDEX; build your own via inheritance
- **Path specs & data specs**: parse directory/filename parts and/or read
  dataset attributes/vars
- **Special rules**: conditionals and method/function calls (e.g. CMIP6 realm,
  time aggregation)
- **Sources of truth**: intake catalogues (local or S3), MongoDB, PostgreSQL
- **Index backends**: Apache Solr, MongoDB
- **Named connections**: refer to stores by name; credentials live in a
  separate, optionally age encrypted secrets file
- **Support of dataset versions**: Dataset versions are stored separately.
  Data containing *all* dataset versions and the *latest* versions only.

The CLI uses a **custom framework** inspired by `Typer <https://typer.tiangolo.com>`_
but is **not** Typer. The main commands are ``config``, ``add``, ``remove``,
``glance`` and ``init-config``, plus ``index`` and ``delete`` for every index
system (``mdc solr index``, ``mdc mongo delete``, ...).

Check also ``mdc --help``

Check the configuration
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

    mdc config --config drs_config.toml --json |jq  .drs_settings

Without the ``--json`` flag the merged toml config (pre defined config + user
defined config) will be displayed and can be piped into a file for later usage
and adjusted.

.. tip::

    Use the ``--json`` flag with ``jq`` command line json parser to inspect
    the configuration by ``<key>-<value>`` pair queries.


Check the last metadata crawl
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can get a quick overview over the metadata store by inspecting it's
content with the ``glance`` sub command:

.. code-block:: console

    mdc glance mongodb://localhost -s username mongo -s password secret

    # the same with a named connection, see below
    mdc glance mongo



Harvest metadata into a source of truth
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Metadata-crawler distinguishes between index or search systems such as
apache solr or elastic search and so called *source of truth* stores. These
stores can be static catalogues or databases that hold *all* metadata entries.

A source of truth is considered a permanent store of crawled metadata, where
as data in an index can change.

.. versionadded:: 2605.0.0

    Instead of writing to file-based ``intake`` catalogues, metadata can be
    crawled directly into a **MongoDB** or **PostgreSQL** database. Database
    backends store catalogue metadata internally, so no YAML catalogue file
    is needed. The backend is detected automatically from the URL scheme.


.. code-block:: console

   # Intake
   mdc add cat.yaml -c drs_config.toml -ds cmip6-fs -ds obs-fs --batch-size 100

   # MongoDB
   mdc add mongodb://username:password@server:27017/database \
       -c drs_config.toml -ds cmip6-fs -ds obs-fs --batch-size 100

   # PostgreSQL
   mdc add postgresql://username:password@server:5432/database \
       -c drs_config.toml -ds cmip6-fs -ds obs-fs --batch-size 100

   # A named connection from connections.toml
   mdc add prod -c drs_config.toml -ds cmip6-fs -ds obs-fs



This reads dataset definitions from ``drs_config.toml`` and writes harvested
metadata into a **metadata store**. You can specify one or
more dataset names via ``-ds/--data-set`` or explicit paths via ``-d/--data-object``.
Meta data store formats include **intake** (via gzipped JSONLines) **MongoDB**
and **PostgreSQL**.



Index entries from a source of truth
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

   mdc <backend> index cat-1.yaml cat-2.yaml

This reads entries from a catalogue and inserts/updates them in the chosen
index backend. Supported backends include **Solr**
and **MongoDB** (see :doc:`chapter3-api/index`).

Remove entries from the source of truth
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 2610.0.0

    Entries from the source of truth (catalogue or database) can be deleted
    with the ``remove`` sub-command.


.. code-block:: console

   # Intake
   mdc remove cat.yaml -f dataset cmip6-fs -f variable tas

   # MongoDB
   mdc remove mongodb://username:password@server:27017/database -f dataset cmip6-fs -f variable tas

   # PostgreSQL, or any named connection; --dry-run only counts the matches
   mdc remove prod -f dataset cmip6-fs -f dataset obs-fs --dry-run

Facets with the same key are combined with OR, different keys with AND, so
the last example removes everything from ``cmip6-fs`` *or* ``obs-fs``.

Named connections
^^^^^^^^^^^^^^^^^

.. versionadded:: 2610.0.0

Instead of repeating URLs and credentials, describe your stores once in
``~/.config/metadata-crawler/connections.toml`` and keep the credentials in
``secrets.toml`` (which can be encrypted with age). ``mdc init-config``
creates commented templates of both files:

.. code-block:: toml

   # ~/.config/metadata-crawler/connections.toml
   [prod]
   url = "postgresql://db.example.org/metadata"

.. code-block:: toml

   # ~/.config/metadata-crawler/secrets.toml (chmod 600)
   [prod]
   username = "ab1234"
   password = "..."

.. code-block:: console

   mdc glance prod

See :ref:`connections` for all options.


Delete entries from an index
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

   mdc <backend> delete --facets file /path/to/*.nc

Deletes entries matching facet/value pairs.
Wild cards in the value are supported (e.g., ``"file *.nc"``).

For detailed options and examples, see the usage
chapter and :doc:`chapter3-api/index`.

Contents
--------

.. toctree::
   :maxdepth: 1

   chapter1-usage/index
   chapter2-config/index
   chapter3-api/index
   whatsnew
   code-of-conduct

.. seealso::

   `Freva <https://pypi.org/project/freva-client/>`_
        The freva evaluation system.
   `Freva admin docs <https://freva-deployment.readthedocs.io>`_
        Installation and configuration of the freva services.



Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
