What's new
==========

This document highlights major changes and additions across releases.

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
