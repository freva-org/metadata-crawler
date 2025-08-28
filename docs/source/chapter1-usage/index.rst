Usage
======

This chapter introduces how to use **metadata‑crawler** to collect
metadata from files stored in various backends and index the results
into an index system. You can drive the crawler either from the
Python API or via the provided command‐line interface. The library
supports both synchronous and asynchronous workflows.

The general workflow of collecting metadata is separated into *two* steps:

1. Harvesting metadata and storing the crawled data to a **temporary intake**
   catalogue. This step should de-couples the crawling from the indexing procedure -
   but if favourable it can also be used to only create **intake** catalogues.
2. Indexing the metadata to the index backend.


The harvesting supports versioned datasets. Dataset versions are stored in two
different collection. One that defines *all* dataset versions and one that only
stores data from the *latest* dataset versions. This discrimination allows users
to quickly access relevant datasets without having to take dataset versions into
account (*latest* versions only).

.. toctree::
   :maxdepth: 1

   sec1-cli
   sec2-python
