.. _schema:

Schema and field options
========================

The metadata crawler uses a **schema** to define how raw metadata keys are mapped into canonical index facets. Each facet in the schema is described by a ``SchemaField`` entry in the DRS configuration. A schema entry specifies the raw key to read, the data type, whether the field is required or multi‑valued, and other options.

Field attributes
----------------

Every schema entry is a table keyed by the facet name. For example:

.. code-block:: toml

   [drs_settings.schema.product]
   key         = "domain"
   type        = "string"
   required    = true

The following attributes are available:

``key`` (required)
    The raw key or placeholder that the crawler uses to read the value.
    A raw key must match a field in the collected metadata, If the field is
    derived from a special rule in the dialect (see :ref:`templates`),
    the ``key`` corresponds to that rule’s output.

``type`` (required)
    Specifies the logical data type.  Allowed base types are:

    * ``string``   - arbitrary text.
    * ``integer``  - whole numbers.
    * ``float``    - decimal numbers.
    * ``datetime`` - ISO 8601 timestamps.
    * ``path``     - path to the dataset.
    * ``uri``      - uri (``<fs_type>://<path>``) to dataset.
    * ``dataset``  - name of the dataset.
    * ``fmt``      - dataset format.
    * ``storage``  - name of the storage backend.
    * ``daterange``- Alias for ``datetime[2]``

    You can declare array types with brackets.  For
    example ``float[4]`` means a list of four floats (e.g. for bounding boxes),
    ``integer[3]`` means three integers, and ``string[]`` means a
    variable‑length list of strings.  Synonyms such as ``bbox``
    (alias of ``float[4]``) are supported.

``required`` (boolean)
    Marks the facet as mandatory.  If a required value is missing from a
    record, it will raise a validation error.

``default`` (any)
    Default value to assign when the raw key is missing or empty.
    Defaults should match the declared type (e.g. an empty string
    for ``string`` fields or a list of four floats for a ``float[4]`` field).

``multi_valued`` (boolean)
    Indicates that the value may be a list.  When ``multi_valued = true``, the
    crawler will store multiple values for that facet.  You can also use
    ``string[]`` or ``integer[]`` in ``type`` to indicate a variable‑length
    list.

``indexed`` (boolean)
    Whether this facet should be indexed for searching.  Most fields should
    set ``indexed = true`` to support filtering in the index store.

``unique`` (boolean)
    Indicates that the facet value is unique across all records.
    This is mainly useful for identifiers such as ``"file"`` or ``"uri"`` and
    is enforced by some index backends (e.g. MongoDB upsert).

``name`` (string)
    If specified, this overrides the canonical facet name in the index.
    Otherwise, the facet name is used.

Examples
--------

The following TOML snippet defines several facets with various options:

.. admonition:: TOML CONFIG

    .. code-block:: toml

        [drs_settings.schema.file]
        key       = "file"
        type      = "path"
        required  = true
        indexed   = true
        unique    = true

        [drs_settings.schema.time]
        key         = "time"
        type        = "datetime[2]"
        indexed     = true
        multi_valued= false
        default     = []

        [drs_settings.schema.bbox]
        key       = "bbox"
        type      = "float[4]"  # west, east, south, north
        default   = [0.0, 360.0, -90.0, 90.0]
        indexed   = false

        [drs_settings.schema.ensemble]
        key         = "member"
        type        = "string[]"
        indexed     = true
        multi_valued= true

        [drs_settings.schema.level_type]
        key     = "level_type"
        type    = "string"
        default = "2d"
        indexed = true

In this example:

* ``file`` is a required string and is marked as unique and indexed.
* ``time`` is a pair of timestamps (start and end) and defaults to an empty list if missing.
* ``bbox`` is a 4‑element array of floats representing the bounding box and is not indexed.
* ``ensemble`` can hold multiple ensemble member identifiers.
* ``level_type`` defaults to ``"2d"`` if not provided.

When designing your schema, choose the simplest type that fits the data and set sensible defaults.  For more complex computed fields, you can use special rules or Jinja2 templates in the dialects (see the Configuration chapter) to populate the values.
