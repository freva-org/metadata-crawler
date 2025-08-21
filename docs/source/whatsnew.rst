What's new
==========

This document highlights major changes and additions across releases.

v2509.0.0
----------

* Initial release of the documentation.
* Added support for multiple storage backends (POSIX, S3, Swift,
  Intake, FDB5) and index backends (DuckDB, MongoDB).
* Introduced a Jinja2 templating engine for configuration defaults.
* Implemented dialect inheritance and dataset overrides.
* Provided a CLI based on Typer with ``crawl``, ``index`` and
  ``delete`` commands.
* Added asynchronous API alongside synchronous wrappers.

Future changes will be documented in this file.
