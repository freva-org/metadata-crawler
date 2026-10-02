.. _connections:

Named connections and secrets
-----------------------------

.. versionadded:: 2610.0.0

    Stores can be given a name in ``connections.toml``, with their
    credentials kept apart in ``secrets.toml``, which can be encrypted with
    `age <https://age-encryption.org>`_.

Typing database URLs with passwords, or S3 endpoints with keys, on every
command is tedious and leaves credentials in your shell history. Instead you
can describe each store once and refer to it by name:

.. code-block:: console

   mdc init-config                       # write commented templates
   $EDITOR ~/.config/metadata-crawler/connections.toml
   $EDITOR ~/.config/metadata-crawler/secrets.toml

   mdc glance prod
   mdc add prod -c drs_config.toml -ds cmip6-fs
   mdc remove prod -f variable pr --dry-run
   mdc solr index prod waterpark --server solr-prod

The configuration lives in two files:

``connections.toml``
    Where the stores and index systems are and how to reach them: URLs,
    hosts, database schemas, S3 endpoints. Nothing secret, so it can be
    shared with your team.
``secrets.toml``
    Usernames, passwords, keys and tokens. Readable only by you, and
    optionally encrypted.

Both are optional. Without them everything works as before with plain paths
and URLs.

Creating the files
^^^^^^^^^^^^^^^^^^

.. code-block:: console

   mdc init-config           # skips files that already exist
   mdc init-config --force   # replace existing files

This writes ``connections.toml`` (mode ``0640``) and ``secrets.toml`` (mode
``0600``) to ``~/.config/metadata-crawler/`` (or ``$XDG_CONFIG_HOME``). Both
templates are fully commented out, so a fresh copy has no effect; they
document every option and contain examples for each backend. To write them
somewhere else, use ``--mdc-config``/``--mdc-secrets`` or set
``MDC_CONFIG_PATH``/``MDC_SECRETS_PATH``:

.. code-block:: console

   mdc init-config --mdc-config ./team-connections.toml

The same is available in Python as :func:`metadata_crawler.init_config`.

Defining connections
^^^^^^^^^^^^^^^^^^^^

Every table in ``connections.toml`` is one named store:

.. code-block:: toml

   # ~/.config/metadata-crawler/connections.toml

   [scratch]                      # intake catalogue on disk
   url = "/work/ab1234/catalogues/scratch.yml"
   description = "My test crawls"

   [waterpark]                    # intake catalogue on S3
   url = "s3://metadata/catalogues/waterpark.yml"
   endpoint_url = "https://s3.example.org"

   [prod]                         # PostgreSQL
   url = "postgresql://db.example.org/metadata"
   db_schema = "metadata_crawler"

   [prod-staging]                 # same server, other schema, same credentials
   url = "postgresql://db.example.org/metadata"
   db_schema = "metadata_crawler_staging"
   secrets = "prod"

   [mongo]                        # MongoDB
   url = "mongodb://mongo.example.org:27017/metadata"
   tls = true

Keys every connection understands:

``url``
    Required. Path or URL of the store. ``uri`` and ``path`` are accepted as
    synonyms. Relative paths are relative to ``connections.toml``, not to the
    directory you run the crawler from.
``backend``
    Optional. Derived from the URL if not given: ``postgresql://`` and
    ``postgres://`` select PostgreSQL, ``mongodb://`` and ``mongodb+srv://``
    select MongoDB, anything else (paths, ``s3://``, ...) is an intake
    catalogue.
``secrets``
    Optional. Take the credentials from this table of ``secrets.toml`` instead
    of the table with the connection's own name.
``description``
    Optional free text.

The remaining keys depend on the backend:

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Backend
     - Keys
   * - intake
     - Any `s3fs <https://s3fs.readthedocs.io>`_ option such as
       ``endpoint_url``, ``anon`` or ``client_kwargs``. They only apply to
       remote URLs.
   * - PostgreSQL
     - ``host``, ``port`` (default 5432), ``database`` (default ``metadata``)
       and ``db_schema`` (default ``metadata_crawler``). Host, port and
       database can also be part of the URL; explicit keys win. Unknown keys
       are an error.
   * - MongoDB
     - ``host``, ``port`` and ``database`` (default ``metadata``) as for
       PostgreSQL. Every other key becomes a MongoDB URI option, for example
       ``tls``, ``replicaSet`` or ``authSource`` (default ``admin``).

Storing credentials
^^^^^^^^^^^^^^^^^^^

A table in ``secrets.toml`` is merged into the connection of the same name,
or into every connection that names it with ``secrets = "<table>"``:

.. code-block:: toml

   # ~/.config/metadata-crawler/secrets.toml

   [prod]                  # used by [prod] and [prod-staging]
   username = "ab1234"
   password = "change-me"

   [waterpark]             # S3: key and secret must be given together
   key = "AKIAXXXXXXXXXXXXXXXX"
   secret = "change-me"

``user`` and ``passwd`` are accepted as aliases of ``username`` and
``password``. Tables that no connection refers to are ignored; a ``secrets``
reference to a missing table is an error.

Credentials are held as masked values: they do not show up in logs, error
messages or ``repr()`` output, and they are never written into catalogue
metadata. A password embedded in a connection URL
(``postgresql://user:pw@host/db``) is moved into the masked field as well,
but keeping it in ``secrets.toml`` is the better habit.

metadata-crawler refuses to read a secrets file that group or others can
access, encrypted or not:

.. code-block:: console

   chmod 600 ~/.config/metadata-crawler/secrets.toml

Encrypting the secrets
^^^^^^^^^^^^^^^^^^^^^^

The secrets file can be encrypted with `age <https://age-encryption.org>`_.
Save it as ``secrets.toml.age`` next to where ``secrets.toml`` would be and
delete the plaintext. Decryption needs the ``vault`` extra:

.. code-block:: console

   python -m pip install "metadata-crawler[vault]"

   # to your SSH key (the key must not be passphrase protected)
   age -R ~/.ssh/id_ed25519.pub -o secrets.toml.age secrets.toml

   # to a dedicated age key, created with: age-keygen -o key.txt
   age -r <public key printed by age-keygen> -o secrets.toml.age secrets.toml

   # with a passphrase
   age -p -o secrets.toml.age secrets.toml

If both ``secrets.toml.age`` and ``secrets.toml`` exist, the encrypted file
is used; a plaintext copy next to it is most likely left over from editing.
The file is decrypted in memory only, once per command (or per
``read_configfiles`` block in Python). Keys and passphrases are found as
follows:

* key encrypted files: the identity in ``MDC_AGE_IDENTITY`` (an SSH private
  key or an age key file), otherwise ``~/.ssh/id_ed25519`` and
  ``~/.ssh/id_rsa``;
* passphrase encrypted files: ``MDC_SECRETS_PASSPHRASE``, otherwise an
  interactive prompt when a terminal is attached.

To edit the file, decrypt it to a private location, change it and encrypt it
again:

.. code-block:: console

   age -d -i ~/.ssh/id_ed25519 secrets.toml.age > "$XDG_RUNTIME_DIR/secrets.toml"

.. note::

   Encrypting to a key that lies unprotected on the same machine protects
   copies of the file (backups, accidental commits), not access to your
   account.

Which settings win
^^^^^^^^^^^^^^^^^^

Settings are combined per key, later sources win:

1. ``connections.toml``
2. ``secrets.toml`` (or ``secrets.toml.age``)
3. ``-s/--storage-option`` on the command line (and ``MDC_STORAGE_OPTIONS``)

So ``mdc glance prod -s db_schema other`` inspects another schema of the
``prod`` database with the stored credentials.

The files are looked up in this order:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - File
     - Selected by
     - Default
   * - connections
     - ``--mdc-config``, then ``MDC_CONFIG_PATH``
     - ``~/.config/metadata-crawler/connections.toml``
   * - secrets
     - ``--mdc-secrets``, then ``MDC_SECRETS_PATH``
     - ``~/.config/metadata-crawler/secrets.toml(.age)``

A file given explicitly must exist; a missing default file simply counts as
empty. ``XDG_CONFIG_HOME`` moves ``~/.config``.

Index systems
^^^^^^^^^^^^^

.. versionadded:: 2610.0.0

The server of an index system can be a named connection, too. The option
that says where the index system is accepts a name as well as an address:

.. code-block:: console

   mdc solr index prod --server solr-prod
   mdc solr delete --server solr-prod -f project cmip6
   mdc mongo index prod --url mongo-search

.. code-block:: toml

   # connections.toml

   [solr-prod]                    # Apache Solr
   backend = "solr"
   url = "https://solr.example.org:8983"

   [mongo-search]                 # MongoDB as index system
   url = "mongodb://mongo.example.org:27017/search"

.. code-block:: toml

   # secrets.toml

   [solr-prod]
   username = "indexer"
   password = "change-me"     # or: token = "..."

   [mongo-search]
   username = "indexer"
   password = "change-me"

``solr`` (``--server``)
    ``backend = "solr"`` is required: a Solr URL is a plain ``http(s)://``
    URL, which on its own means an intake catalogue.

    .. list-table::
       :widths: 22 78

       * - ``url``
         - ``http(s)://host:port``; a trailing ``/solr`` is optional.
       * - ``username``, ``password``
         - In ``secrets.toml``. Sent with HTTP Basic auth (Solr's
           ``BasicAuthPlugin``, or a proxy in front of Solr).
       * - ``token``
         - In ``secrets.toml``, instead of username and password. Sent as
           ``Authorization: Bearer <token>`` (``JWTAuthPlugin``, or a proxy
           that checks tokens).
       * - ``ca_file``
         - CA bundle (PEM) to verify the server certificate with, e.g. for an
           internal CA. Relative to ``connections.toml``.
       * - ``verify_ssl``
         - ``true`` by default. ``false`` skips the certificate check; only for
           testing, since the credentials then go to whoever answers.
``mongodb`` (``--url``)
    The same kind of connection as a MongoDB catalogue, with the same keys
    (see `Defining connections`_). Its database is used unless
    ``--database`` is given.

Options you give on the command line win over the connection, e.g.
``--database`` for MongoDB. The connection to the index system is separate
from the stores being indexed: ``-s/--storage-option`` still applies to the
stores, not to the index system. Index system plugins can support named
connections as well, see :ref:`add_backends`.

How names are resolved on the command line
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Names are accepted wherever the CLI expects a store: the ``store`` argument of
``add``, ``remove`` and ``glance``, and the stores passed to
``<index system> index``. They are also accepted by the option that names
the server of an index system (``--server`` for Solr, ``--url`` for
MongoDB). An argument counts as a name if it has no
``scheme://`` and does not point to an existing file or directory. Names that
are not configured are passed on unchanged, so

* ``mdc add new-catalogue.yml ...`` still creates a new catalogue,
* ``mdc glance localhost -cb mongodb`` still means the host ``localhost``, and
* ``mdc solr index prod --server localhost:8983`` still means that server.

The configuration files are only read when at least one argument looks like
a name, so plain paths and URLs work without any configuration.

.. note::

   The ``storage_options`` of datasets in a ``drs_config.toml`` are not taken
   from these files. Credentials for datasets can be read from environment
   variables with :ref:`templating <templates>`.

Using connections in Python
^^^^^^^^^^^^^^^^^^^^^^^^^^^

In Python, names are not resolved automatically. Open the configuration with
:func:`~metadata_crawler.connections.read_configfiles` and pass the
connection objects to the API functions, which accept them wherever they
accept a path or URL:

.. code-block:: python

   import metadata_crawler as mdc
   from metadata_crawler.api.stores.postgresql import PostgresConnection
   from metadata_crawler.connections import read_configfiles

   with read_configfiles() as config:
       prod = config.get("prod", PostgresConnection)   # type checked
       scratch = config["scratch"]                       # any backend

       mdc.add("drs_config.toml", store=prod, data_set=["cmip6-fs"])
       mdc.remove(scratch, facets=[("variable", "pr")], dry_run=True)
       print(mdc.glance_metadata(prod))

       # index systems take their connection where the CLI takes the name
       mdc.index("solr", prod, server=config["solr-prod"])

       # a configured name or an ad-hoc URL, with overrides
       other = config.resolve("prod", db_schema="metadata_crawler_staging")

The secrets are dropped when the ``with`` block ends; afterwards the
configuration can no longer be used. ``read_configfiles`` takes optional
``store_path`` and ``secrets_path`` arguments that work like ``--mdc-config``
and ``--mdc-secrets``.

Connections can also be created without any files:

.. code-block:: python

   from metadata_crawler.api.stores.mongodb import MongoConnection

   conn = MongoConnection.from_url(
       "mongodb://mongo.example.org/metadata", username="ab1234", password="..."
   )
   mdc.glance_metadata(conn)

Reference
^^^^^^^^^

.. autofunction:: metadata_crawler.connections.read_configfiles

.. autoclass:: metadata_crawler.connections.ConfigFiles
   :members: get, resolve, close
   :no-inherited-members:

.. autoclass:: metadata_crawler.ingester.solr.SolrConnection
   :members: store_uri, auth_headers, ssl
   :no-inherited-members:

.. autoclass:: metadata_crawler.api.stores.base.BaseConnection
   :members: from_url, store_uri, storage_options, catalogue_options
   :no-inherited-members:
