Inheritance
-----------

Dialect and dataset definitions support inheritance to reduce
duplication.  Use the ``inherits_from`` key to base a new definition
on an existing one and override only the properties that differ.

Dialect inheritance
^^^^^^^^^^^^^^^^^^^

If you have a dialect that is nearly identical to another, you can
create a new dialect inheriting from it.  For instance, suppose
``nextgems`` is derived from ``cmip6`` but uses a different
``specs_file`` and some overrides:

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.dialect.nextgems]
       inherits_from = "cmip6"
       specs_file = ["variable", "time", "level"]
       defaults = { project = "NEXTGEMS" }

When the config is loaded, the parent dialect (``cmip6``) is copied
and then the child’s fields are merged on top.  Empty tables in the
child override the parent’s tables; nested dictionaries are merged
recursively.  Multiple levels of inheritance are supported.

Dataset inheritance
^^^^^^^^^^^^^^^^^^^

Datasets can also inherit from other datasets.  Use this when two
datasets share the same root or storage options but differ in
defaults or dialect choice.  For example:

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [cmip6-cycle3]
       inherits_from = "cmip6-fs"
       root_path = "/data/cmip6/cycle3"
       defaults = { version = -2 }

In this case all fields from ``cmip6-fs`` (root_path, fs_type,
storage_options, defaults, etc.) are copied; only ``root_path`` and
``defaults.version`` are overridden.  Using inheritance reduces
duplication in complex configuration files.

Note that cyclic inheritance is not allowed.  If a parent dialect or
dataset cannot be found the loader will raise an error.
