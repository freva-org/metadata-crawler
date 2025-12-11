What's new
==========

This document highlights major changes and additions across releases.

v2511.1.2
---------
* do not install uvloop on windows


v2511.1.1
---------
* windows bug fixing

v2511.1.0
---------
* cli ``add`` call, python ``add`` and ``async_add`` support multiple config
  input files. Glob pattern are also supported
* The catalogue argument has been rearanged and is now a keyword
  argument: of the python funcdions ``add("data.yaml", "drs-config.toml")``
  and ``async_add("data.yaml", "drs-config.toml")`` become
  ``add("drs-config.toml", store="data.yaml")`` and
  ``async_add("drs-config.toml", store="data.yaml")``.
* Add rust based posix backend for faster posix fs crawling.

.. warning::

    If the ``store`` keywords in :mod:`metadata_crawler.add` and
    :mod:`metadata_crawler.async_add` are omitted the
    output catalogue will be interpreted as config file.



v2510.1.1
---------
* Fix bug for determining latest versions.

v2510.1.0
---------
* Do not set default port for solr indexing.
* Add inferring CMOR style time-frequency from dataspecs.

v2510.0.2
---------
* Allow any type of additional keys.


v2510.0.1
---------
* Bug fixing

v2510.0.0
---------
* Restructure solr ingest.
* Make directory and filename parts in path specs optional.

v2509.0.2
----------
* Display progressbar for ingestion.
* Improved logging.
* Fix S3 "flat directory bug".
* Drop dataset versioning if crawl path if past version position.
* Fix "time" bug.
* Add an optional index suffix for solr cores and mongo collections.
* Add Multi threaded solr indexing.

v2509.0.1
----------

* Initial release of the documentation.
* Added support for multiple storage backends (POSIX, S3, Swift,
  Intake, FDB5) and index backends (Apache Solr, MongoDB).
* Introduced a Jinja2 templating engine for configuration defaults.
* Implemented dialect inheritance and dataset overrides.
* Provided a CLI based on Typer with ``crawl``, ``index`` and
  ``delete`` commands.
* Added asynchronous API alongside synchronous wrappers.

Future changes will be documented in this file.
