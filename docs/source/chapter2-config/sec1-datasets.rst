.. _datasets:

Datasets
---------


A *dataset* entry identifies a logical collection of files or objects
to crawl.  In a ``drs_config.toml`` file each dataset appears under
its own top‑level table.  At minimum you must specify a
``root_path`` (the top directory or bucket prefix) and a
``drs_format`` (the dialect name).  Additional keys control which
storage backend is used and allow per‑dataset defaults.

Example
^^^^^^^^

The following snippet defines three datasets.  ``cmip6-fs`` is a
CMIP6 dataset stored on a POSIX filesystem; ``obs-s3`` lives in a
MinIO/S3 bucket; and ``ngm-swift`` is hosted on a Swift object
store.  Defaults override or add facet values when records are
constructed.
.. admonition:: TOML CONFIG

    .. code-block:: toml

       # CMIP6 data on a local filesystem
       [cmip6-fs]
       root_path = "/data/model/global/cmip6"
       drs_format = "cmip6"
       # optional: defaults
       [cmip6-fs.defaults]
       project = "CMIP6"

       # NextGems stored on s3
       [ngm-s3]
       root_path = "/freva/nexgems"
       inherits_from = "obs-s3" # (anything else like for obs-s3)
       glob_pattern = "*.zarr" # (get only zarr stores)


       # Observational data on S3/MinIO
       [obs-s3]
       root_path = "s3://freva/observations"
       drs_format = "obs"
       fs_type = "s3"
       [obs-s3.storage_options]
       endpoint_url = "https://play.min.io"
       aws_access_key_id = "<ACCESS_KEY>"
       aws_secret_access_key = "<SECRET_KEY>"
       region = "us-east-1"
       [obs-s3.defaults]
       project = "observations"

       # NextGEMS data on Swift
       [ngm-swift]
       root_path = "nextgems/era5"
       drs_format = "nextgems"
       fs_type = "swift"
       [ngm-swift.storage_options]
       os_auth_url = "https://swift.example.com/auth/v3"
       os_storage_url = "https://swift.example.com/v1"
       os_username = "{{ env['USER'] | default('myuser') }}"
       os_password = "{{ env['PASS'] }}"
       os_project_name = "<PROJECT>"
       os_user_domain_name = "Default"
       os_project_domain_name = "Default"
       [ngm-swift.defaults]
       project = "nextgems"

Keys
^^^^

* **root_path** – Path or prefix to search for files.  For S3 and
  Swift backends it is the prefix inside the bucket/container.  For
  POSIX it is an absolute directory.  When using the intake backend
  this can be a intake or intake-esm catalog file.
* **drs_format** – Name of the DRS dialect to use.  Must match one of
  the dialects defined under ``[drs_settings.dialect]``.
* **fs_type** – Optional override of the storage backend for this
  dataset.  Supported values include ``posix`` (default), ``s3``,
  ``swift`` and ``intake``.  Backends determine how the
  crawler traverses paths and reads data.
* **storage_options** – A table of backend specific options.  For
  S3 this may include ``endpoint_url``, ``aws_access_key_id``,
  ``aws_secret_access_key``, ``region`` and ``endpoint_url``.
  For Swift use keys beginning with ``os_`` (see the Swift
  example).  Additional fields can be added for custom backends.
* **defaults** – Facet values that should be filled in when the
  corresponding key is absent in the parsed metadata.  These
  defaults override the dialect defaults if both are specified.
* **inherits_from** – Onther dataset name to take defaults from.
* **glob_pattern** – Apply this glob pattern for file object discovery.


Storage options
---------------

The ``fs_type`` key determines which storage backend the crawler uses
to traverse files.  Each backend accepts a set of backend specific parameters
under ``storage_options``.  This section summarises supported backends and
their options.

POSIX (default)
^^^^^^^^^^^^^^^

If ``fs_type`` is omitted or set to ``posix``, the crawler walks a
local filesystem.  No special options are required.

S3/MinIO
^^^^^^^^

Set ``fs_type = "s3"`` to crawl an object store exposing the S3
API.  Required options include:

* ``endpoint_url`` – The full URL of the S3 endpoint (e.g.
  ``https://s3.amazonaws.com`` or a MinIO server).
* ``key``, ``secret`` – Credentials.
* ``region`` – Region name (may be optional for MinIO).
  ``root_path`` (``s3://my-bucket/path``) or supply it via
  ``storage_options.bucket``.

Swift
^^^^^

Set ``fs_type = "swift"`` to crawl an OpenStack Swift object store.
Swift uses Keystone v3 credentials.  Required options include:

* ``os_auth_url`` – Keystone authentication URL.
* ``os_storage_url`` – Storage URL for object endpoints; typically
  ends in ``/v1``.
* ``os_username``, ``os_password``,
  ``os_project_id`` – Identity
  credentials.
* ``container`` – The container name may be specified either in the
  ``storage_options`` or as part of the ``root_path`` (first path
  component).

Intake
^^^^^^

When ``fs_type = "intake"`` the crawler reads from a intake-esm or other
Intake catalog rather than walking a directory.  The ``root_path``
points to the CSV file and ``storage_options`` are not required.


Custom backends
^^^^^^^^^^^^^^^

The API can be extended with new storage backends (see
:doc:`../chapter3-api/sec1-storage-backends`).  Provide the backend
class via an entry point or plugin and specify its name in
``fs_type``.  Your backend may define arbitrary ``storage_options``.
