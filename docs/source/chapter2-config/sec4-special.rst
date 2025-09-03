.. _special:

Special rules
--------------

Special rules let you **compute or override** facet values after path/data extraction.
They run with access to the already‐collected metadata, the file path/URI, and the full
loaded configuration model. A rule only assigns a value if the facet is **not already**
present in the record.

Execution model
^^^^^^^^^^^^^^^^

- **Precedence**: specials never clobber values already set by path/data extraction
  (the early ``continue`` means “first win” for any facet).
- **Templating**: before evaluation, strings are rendered with **Jinja2** using
  ``data`` (see “Available variables” below).
- **Evaluation scope**: Available variables are define by the ``drs_config``:
        * ``datasets: dict``: The dictionary of the defined datasets
        * ``suffixes: list``: The valid path suffixes (``drs_settings.suffixes``)
        * ``index_schema: dict``: The defined data schema as defined in ``drs_settings.index_schema``
        * ``special: dict``: Global special rules as defined in ``drs_settings.special``
        * ``storage_options: dict``: Global storage options as defined in ``drs_settings.storage_options``
        * ``dialect: dict``: Dialect settings defined by ``drs_settings.dialect``
- **Truthiness**: assignment happens only if ``result`` is truthy. Empty strings,
  empty lists, ``0`` or ``False`` will **not** be assigned. If you need to set such
  values, return a non-empty representation or adjust the code to check ``is not None``.

Available variables
^^^^^^^^^^^^^^^^^^^^

Two namespaces are available depending on the step:

- **Jinja2 template context**:

  - All current record keys from the ``metadata`` (e.g. ``{{ variable }}``,
    ``{{ table_id }}``, ``{{ time_frequency }}``).
  - The file path and URI as ``{{ file }}`` and ``{{ uri }}``.

- **Python eval context**:

  - The entire parsed model (your TOML) exposed as a nested dictionary:
    ``datasets``, ``suffixes``, ``index_schema``, ``special``, ``storage_options``,
    ``dialect``. For example:
    - ``dialect['cordex']['domains']['EUR-11']``
    - ``datasets['cmip6-fs']['root_path']``

Rule types
^^^^^^^^^^^

Conditional
~~~~~~~~~~~

Evaluate a boolean **Python** expression (after Jinja rendering) and choose
between two literal values.

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.special.time_aggregation]
       type      = "conditional"
       condition = "'pt' in '{{ time_frequency | default(\"day\") | lower }}'"
       true      = "inst"
       false     = "mean"

Flow:

1. Jinja renders the condition using current metadata (``{{ time_frequency }}``).
2. The rendered string is **eval**’d against the model dict (no builtins).
3. If truthy → assign ``true``, else ``false`` (only if facet not already set).

Lookup
~~~~~~
Lookup is a special type of data attribute lookup that stores the results of the
attribute lookup in a nested cache. This allows for efficient retrieval of
attributes that have already been retrieved from datasets.

Internally this call the dataset storage backend’s ``lookup(path, attribute, *tree, **read_kws)``
method to fetch values from a **cached tree** (e.g., mapping CMIP6 ``table_id`` + ``variable_id`` to
``realm`` or ``frequency``). Items are first rendered via Jinja.

Below you can find the signature of the method that gets involved when applying
the lookup rule:

.. automethod:: metadata_crawler.api.storage_backend::PathTemplate.lookup
   :no-index:

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.dialect.cmip6.special.realm]
       type            = "lookup"
       attribute       = "realm"
       tree            = ["{{ table_id }}", "{{ variable_id }}"]
       standard        = "cmip6"

       [drs_settings.dialect.cmip6.special.time_frequency]
       type   = "lookup"
       attribute = "frequency"
       tree  = ["{{ table_id }}", "{{ variable_id }}"]


.. note::

    - Backends should **memoize** lookups in an in-memory, tree-shaped cache so
      repeated queries across millions of files are O(1) after the first read.

    - ``read_kws`` are taken from ``dialect[standard].data_specs.read_kws`` (e.g.,
      the xarray engine) and passed through to the dataset reader.

    - The ``standard`` key in the lookup-table configuration selects the top-level
      namespace (branch) where data are stored. If ``standard`` is omitted or empty,
      the lookup falls back to the DRS-type name (the dialect).


Call
~~~~

Render a string with Jinja and **eval** it as a Python expression within the model
dict scope. Useful for string composition or referencing config data structures.

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.dialect.cordex.special.model]
       type = "call"
       call = "'{{ driving_model }}-{{ rcm_name }}-{{ rcm_version }}'"

You may also reference config structures as nested dicts in the expression,
for example:

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.dialect.cordex.special.default_bbox]
       type = "call"
       call = "dialect['cordex']['domains'].get('{{ domain | upper }}', [0,360,-90,90])"

Order and scoping
^^^^^^^^^^^^^^^^^^

- **Where to define rules**:
  - Global: ``[drs_settings.special.<facet>]`` (applies to all dialects)
  - Per-dialect: ``[drs_settings.dialect.<name>.special.<facet>]``

- **Which wins**:
  - Specials never overwrite a facet already set by earlier steps.
  - If you apply **global** specials first and **dialect** specials second, the
  dialect can fill remaining gaps specific to that standard.
  - If you need a dialect rule to take precedence for a facet that a global rule
  might also set, ensure the dialect rule runs **first** (so the global pass
  will skip, seeing the value already present). Choose your pass order
  accordingly in your pipeline.

Examples recap
^^^^^^^^^^^^^^^

Global conditional (time aggregation)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.special.time_aggregation]
       type      = "conditional"
       condition = "'pt' in '{{ time_frequency | default(\"mean\") | lower }}'"
       true      = "inst"
       false     = "mean"

CORDEX composite model (call)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.dialect.cordex.special.model]
       type = "call"
       call = "'{{ driving_model }}-{{ rcm_name }}-{{ rcm_version }}'"

CMIP6 lookups (realm / frequency)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.dialect.cmip6.special]
       realm.type            = "lookup"
       realm.tree            = ["{{ table_id }}", "{{ variable_id }}"]
       realm.attribute       = "realm"

       time_frequency.type   = "lookup"
       time_frequency.tree   = ["{{ table_id }}", "{{ variable_id }}"]
       realm.attribute       = "frequency"

Performance notes
^^^^^^^^^^^^^^^^^^

- The **lookup** rule is designed for high repetition: even if filenames are unique,
  the ``(table_id, variable_id)`` pairs repeat, so cached results eliminate costly I/O.
- Use **lookup** instead of **call** or conditional.
- Keep **conditional** and **call** expressions simple; they run per file.
- Avoid using complex *jinja2* templates. Although the templates are pre-compiled
  and cache. Evaluating them on a per file basis is costly.
- Changing the **batch-size** can influence the overall performance of the process.

.. warning::

    - Both ``conditional`` and ``call`` use **eval** with your model dict as the only
      scope (no Python builtins). Treat configuration as **trusted input**.
    - Prefer Jinja templating (``{{ ... }}``) for string assembly and limit Python
      expressions to straightforward logic.
    - When using Jinja templating variable quoting is important.
    - Don't use this method if you can't expect consistency of attributes across
      many files.

Troubleshooting
^^^^^^^^^^^^^^^

- Nothing gets assigned:
  - Ensure the facet isn’t already present from path/data extraction.
  - Remember: falsy results (``""``, ``[]``, ``0``, ``False``) are not assigned.
- Name errors in expressions:
  - In ``conditional``/``call`` expressions, only names from the **model dict**
  are available; use Jinja to substitute metadata values first (``{{ variable }}``).
- Name errors:
    - Check quotes in Jinja templates.
