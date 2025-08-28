Configuration
=============

This chapter describes how to configure metadata‑crawler via a
``drs_config.toml`` file.  The configuration is divided into different
layers:

* :ref:`schema`: definitions for canonical facets and their data types.
* :ref:`dialects`: definitions for each DRS standard (e.g. CMIP6, CORDEX)
  describing how to parse directory and file names and which sources
  of metadata to use.
* :ref:`datasets`: entries that specify where to find data (root paths,
  storage backends, authentication) and how to inherit from a dialect.
* :ref:`specs`: instructions on how to extract metadata.
* :ref:`special`: instructions on how to apply additional retrieval rules.

.. toctree::
   :maxdepth: 1

   sec1-datasets
   sec2-dialects
   sec3-specs
   sec4-special
   sec5-schema
   sec6-templates
   sec7-inheritance
