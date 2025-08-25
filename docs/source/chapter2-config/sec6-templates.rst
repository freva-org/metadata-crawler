.. _templates:

Jinja2 templating
-----------------

``metadata‑crawler`` allows you to use Jinja2 templating within
configuration values.  This is especially useful when you need to
construct dynamic paths or default values based on other facets.  The
templating is applied during the translation phase when metadata
records are assembled.

For example, suppose you want the ``uri`` facet to include both the
``fs_type`` and the file name.  In your schema you could set

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.schema.uri]
       key = "__uri__"
       type = "string"
       # Provide a Jinja2 expression for the default
       default = "{{ fs_type }}://{{ file }}"

Here ``fs_type`` and ``file`` are existing facets (populated from the
path or defaults).  The expression will be rendered into a final
string at runtime.  You can use any Jinja2 syntax including filters
and conditionals.  The context available during rendering contains
all canonical facets plus the current dataset entry.  See the
``jinja2`` documentation for full syntax.

.. note::

   Quoting matters: Remember that when defning templates that should serve as
   code and have to be evaluated the quoting of variables becomes important:

   ``'{{ test }}'`` is not ``{{ test }}``.

Another example uses templating to compute the ``dataset`` name from
the dataset entry itself:

.. admonition:: TOML CONFIG

    .. code-block:: toml

       [drs_settings.schema.dataset]
       key = "__dataset__"
       type = "string"
       default = "{{ dataset }}"

Here ``dataset`` refers to the section name (e.g. ``cmip6-fs``)
under which the dataset is defined.

Templating is also supported in ``defaults`` and in special rule
arguments.  Remember to escape braces if you need literal ``{{`` or
``}}`` characters.

.. tip::

   You can also resolve environment variables to pass sensitive information
   such as passwords or keys:

   [foo.storage_settings]
   secret_key = "{{ env['MY_KEY'] }}"
