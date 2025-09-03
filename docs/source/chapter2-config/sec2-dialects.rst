.. _dialects:

Dialects
--------

A *dialect* encapsulates the rules for interpreting directory
structures and file names, mapping raw keys from the metadata into
canonical facets, and computing derived fields.  Dialects are
declared under ``[drs_settings.dialect.<name>]``.  The name (e.g.
``cmip6``, ``cmip5``, ``cordex``) is referenced by datasets via the
``drs_format`` attribute.

The dialect definition typically consists of several components:

* **sources** – A list describing where to obtain metadata.  Valid
  values include ``path`` (parse directory/file names), ``data``
  (read attributes from the dataset), and ``storage`` (obtain from
  the storage backend).  Order matters: if the first source yields a
  value it will not be overridden by later sources.
* **path_specs** – The set of rules indicating how to derive metadata from
  paths. See :doc:`sec3-specs` for more information.
* **facets** – A mapping that overrides the raw key associated with
  a canonical facet.  For example, CMIP6 uses ``source_id`` to
  populate the ``model`` facet.  If omitted, the facet name itself is
  used.
* **defaults** – Provide values for missing facets across this
  dialect (see also dataset defaults).  For CMIP6 one may set
  ``grid_label = "gn"`` and ``version = -1``.
* **special** – A set of computed rules.  Each entry describes how
  to derive a value not directly available from the path or
  attributes.  Three kinds of special rules exist:

  - ``conditional`` rules evaluate a Python expression on the
    temporary ``data`` dictionary and choose ``true`` or ``false``
    values accordingly.
  - ``lookup`` rules call a registered lookup method on the config class
     with arguments referencing other facets and data attributes for lookup.
  - ``call`` rules evaluate an arbitrary expression using the
    ``drs_config`` entries and the already retrieved ``metadata``.
  See :doc:`sec4-special` for more information.
* **domains** – For CORDEX, a table mapping domain codes (e.g.
  ``EUR-11``) to bounding boxes; used by the ``bbox`` special rule.
* **data_specs** – (see :doc:`sec3-specs`) define rules for reading
  metadata from the dataset itself.

Example: CMIP6 dialect
^^^^^^^^^^^^^^^^^^^^^^

Below is a simplified CMIP6 dialect definition illustrating the
various sections.  The ``sources`` list indicates that metadata
should first be parsed from the path and then, if necessary, from
data attributes.  Directory components encode facets like ``mip_era``
and ``activity_id``; the file name encodes the variable, table, model
and time period.  Special rules call helper methods to look up the
realm and time aggregation.

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.dialect.cmip6]
       sources = ["path", "data"]
       # map canonical facet names to raw keys
       facets = { model = "source_id", ensemble = "member_id" }
       defaults = { grid_label = "gn", version = -1 }
       [drs_settings.dialect.cmip6.path_specs]
       dir_parts = [
         "mip_era", "activity_id", "institution_id", "source_id",
         "experiment_id", "member_id", "table_id", "variable_id",
         "grid_label", "version",
       ]
       file_parts = [
         "variable_id", "table_id", "source_id", "experiment_id",
         "member_id", "grid_label", "time",
       ]
      [drs_settings.dialect.cmip6.special.realm]
       type = "lookup"
       tree = ["{{ table_id }}", "{{ variable_id }}", "realm"]
       standard = "cmip6"
       [drs_settings.dialect.cmip6.special.time_aggregation]
       type = "conditional"
       condition = "'pt' in '{{ time_frequency | lower}}'"
       true = "inst"
       false = "mean"

Example: CORDEX dialect with domain lookup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

CORDEX defines model names as a combination of several fields, and
uses a domain table to derive bounding boxes.  The ``special``
section defines a function rule that concatenates three facets, and
``bbox`` looks up the domain in the ``domains`` table.

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.dialect.cordex]
       sources = ["path"]
       [drs_settings.dialect.cordex.defaults]
       realm = "atmos"
       [drs_settings.dialect.cordex.path_specs]
       dir_parts = [
         "project", "product", "domain", "institution", "driving_model",
         "experiment", "ensemble", "rcm_name", "rcm_version",
         "time_frequency", "variable", "version",
       ]
       file_parts = [
         "variable", "domain", "driving_model", "experiment",
         "ensemble", "rcm_name", "rcm_version", "time_frequency",
         "time",
       ]
       [drs_settings.dialect.cordex.special.model]
       type = "function"
       call = "'{{driving_model}}-{{rcm_name}}-{rcm_version}}'"
       [drs_settings.dialect.cordex.special.bbox]
       type = "function"
       call = "dialect['cordex']['domains'].get('{{domain | upper}}', [0,360,-90,90])"
       [drs_settings.dialect.cordex.domains]
       EUR-11 = [-44.14, 64.40, 22.20, 72.42]
       AFR-44 = [-24.64, 60.28, -45.76, 42.24]
       # ... further domain definitions ...


.. tip::

    Check the ``mdc config`` sub command for full
    definitions of the built‑in dialects (CMIP6, CMIP5, CORDEX,
    NextGEMS, Observations, etc.).
