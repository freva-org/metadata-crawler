.. _specs:

Path and data specs
-------------------

Metadata can be extracted from three primary sources: the file system
path and the dataset contents and the filesystem itself.  Dialects declare
how to interpret each via ``path_specs`` and ``data_specs``.

.. _path_specs:

Path specs (dir_parts and file_parts)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``dir_parts`` lists facet names corresponding to directory
  components.  The crawler takes the relative path (between
  ``file_parts`` and the file name), splits it on ``/``, and assigns
  each segment in order.  If the number of segments differs from the
  length of ``dir_parts``, the extra segments are ignored.

* ``file_parts`` lists facet names corresponding to parts of the
  file name.  The file name (without extension) is split on the
  ``file_sep`` character (``_`` by default).  Each part is mapped
  to a facet.  When fewer parts are present than entries in
  ``file_parts`` the remainder is ignored; conversely extra parts are
  discarded.

This mechanism makes it easy to support DRS naming conventions like:


.. code-block:: console

    mip_era/activity_id/institution_id/source_id/experiment_id/member_id\
        /table_id/variable_id/grid_label/version/\
        variable_id_table_id_source_id_experiment_id_member_id_grid_label_time.nc

.. _data_specs:

Data specs (data_specs)
^^^^^^^^^^^^^^^^^^^^^^^

``data_specs`` defines how to read attributes and variables from the
dataset file itself (e.g. netCDF, Zarr).  A ``data_specs`` table
contains three subsections:

* ``globals`` - A mapping from facet names to dataset global
  attribute names.  For example ``project = "mip_era"`` means the
  global attribute ``mip_era`` populates the ``project`` facet.

* ``var_attrs`` - Rules to extract attributes from specific
  variables.  Each entry describes the target facet, the variable
  name (can be a placeholder like ``__variable__`` meaning all
  variables, or a format string like ``{{variable}}`` that refers to
  the parsed ``variable`` facet), the attribute name

.. admonition:: TOML CONFIG

  .. code-block:: toml

     [drs_settings.dialect.cmip6.data_specs.var_attrs]
     # attach standard_name attribute of each variable to the facet 'variable'
     variable = { var="__variables__", attr="standard_name"}

* ``stats`` - Extract numeric statistics from variables or coordinate
  arrays.  Each entry specifies a ``stat`` type (``min``, ``max``,
  ``minmax``, ``range``, or ``bbox``), the target facet, and
  variables or coordinate names.  ``range`` returns start and end
  values (useful for time coordinates); ``bbox`` computes the
  bounding box from latitude and longitude variables.

.. admonition:: TOML CONFIG

  .. code-block:: toml

     [drs_settings.dialect.cmip6.data_specs.stats.time]
     stat   = "range"
     coord  = "time"
     default = ["1970-01-01", "1970-01-01"]

When ``sources`` includes ``data`` the crawler opens the dataset with
``xarray`` and applies these rules after path parsing.  A cache
mechanism ensures that repeated attribute lookups across many files
are efficient.

* ``read_kws`` - Keyword arguments that are passed to ``xarray`` to open datasets.
  Such as ``engine=netcdf``.
