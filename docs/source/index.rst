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

    mdc add s3://freva/metadata-crawler/data.yml -c drs-config.toml -ds xces-*
    mdc solr index s3://feva/metadata-crawler/data.yml --server localhost:8983


- **Multi-backend discovery**: POSIX, S3/MinIO, Swift (async REST), Intake
- **Two-stage pipeline**: *crawl → catalogue* then *catalogue → index*
- **Schema driven**: strong types (e.g. ``string``, ``datetime[2]``,
  ``float[4]``, ``string[]``)
- **DRS dialects**: packaged CMIP6/CMIP5/CORDEX; build your own via inheritance
- **Path specs & data specs**: parse directory/filename parts and/or read
  dataset attributes/vars
- **Special rules**: conditionals and method/function calls (e.g. CMIP6 realm,
  time aggregation)
- **Index backends**: JSONLines (intake), Apache Solr, MongoDB
- **Support of dataset versions**: Dataset versions are stored separately.
  Data containing *all* dataset versions and the *latest* versions only.

The CLI uses a **custom framework** inspired by `Typer <https://typer.tiangolo.com>`_
but is **not** Typer. The Main commands are grouped under four verbs:
``config``, ``crawl``, ``index`` and ``delete``.

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


Harvest metadata into a catalogue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

   mdc crawl cat.yaml -c drs_config.toml --dataset cmip6-fs --dataset obs-fs \
             --threads 4 --batch-size 100

This reads dataset definitions from ``drs_config.toml`` and writes harvested
metadata into a temporary **catalogue** file. You can specify one or
more dataset names via ``--dataset`` or explicit paths via ``--data-object``.
Catalogue formats include JSONLines (gzipped).

Index catalogue entries
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

   mdc <backend> index cat-1.yaml cat2.yaml

This reads entries from a catalogue and inserts/updates them in the chosen
index backend. Supported backends include **Solr**
and **MongoDB** (see :doc:`chapter3-api/index`).

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
