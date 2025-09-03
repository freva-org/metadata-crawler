Command‑line interface
----------------------

The software installs a console entry point named
``metadata-crawler`` or ``mdc`` that exposes the high‑level subcommands:

* ``add``  – Collect metadata into a temporary catalog.
* ``config`` – Display general configuration
* ``solr``   - Index and delete metadata to/from Apache solr.
* ``mongo``  – Index and deleta metadata to/from MongoDB.
* ``walk-intake`` – Convenience module to traverse and check intake catalogues.

Use ``--help`` on any command to see available options.  Below are
some examples.

Basic crawling
^^^^^^^^^^^^^^

To harvest a directory of files into a JSON lines catalog:

.. code-block:: console

   mdc add \
        /tmp/cat.yml \
       -c /path/to/drs_config.toml \
       --catalogue-backend jsonlines \
       --threads 4 \
       --batch-size 100 \
       --data-object /path/to/data

Alternatively you can provide one or more dataset names defined in
your DRS configuration instead of explicit file paths:

.. code-block:: console

   metadata-crawler add \
       /tmp/catalog.yaml \
       -c /path/to/drs_config.toml \
       --data-set cmip6-fs obs-fs


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
