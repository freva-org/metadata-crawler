# metadata-crawler

[![License](https://img.shields.io/badge/License-BSD-purple.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/pyversions/metadata-crawler.svg)](https://pypi.org/project/metadata-crawler/)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/metadata-crawler.svg)](https://anaconda.org/conda-forge/metadata-crawler)
[![Docs](https://readthedocs.org/projects/metadata-crawler/badge/?version=latest)](https://metadata-crawler.readthedocs.io/en/latest/?badge=latest)
[![Tests](https://github.com/freva-org/metadata-crawler/actions/workflows/ci_job.yml/badge.svg)](https://github.com/freva-org/metadata-crawler/actions)
[![Test-Coverage](https://codecov.io/gh/freva-org/metadata-crawler/graph/badge.svg?token=W2YziDnh2N)](https://codecov.io/gh/freva-org/metadata-crawler)


Harvest, normalise, and index climate / earth-system metadata from **POSIX**,
**S3/MinIO**, and **OpenStack Swift** using configurable **DRS dialects**
(CMIP6, CMIP5, CORDEX, …). Output to a temporary **catalogue** (JSONLines)
and then **index** into systems such as **Solr** or **MongoDB**.
Configuration is **TOML** with inheritance, templating, and computed rules.

## TL;DR

- Define datasets + dialects in ``drs_config.toml``
- ``mdc add`` → write a temporary catalogue (``jsonl.gz``)
- ``mdc config`` → inspect a the (merged) crawler config.
- ``mdc walk-intake`` → inspect the content of an intake catalogue.
- ``mdc <backend> index`` → push records from catalogue into your index backend
- ``mdc <backend> delete`` → remove records by facet match

## Features

- **Multi-backend discovery**: POSIX, S3/MinIO, Swift (async REST), Intake
- **Two-stage pipeline**: *crawl → catalogue* then *catalogue → index*
- **Schema driven**: strong types (e.g. ``string``, ``datetime[2]``,
  ``float[4]``, ``string[]``)
- **DRS dialects**: packaged CMIP6/CMIP5/CORDEX; build your own via inheritance
- **Path specs & data specs**: parse directory/filename parts and/or read
  dataset attributes/vars
- **Special rules**: conditionals, cache lookups and function calls (e.g. CMIP6 realm,
  time aggregation)
- **Index backends**: MongoDB (Motor), Solr
- **Sync + Async APIs** and a clean CLI
- **Docs**: Sphinx with ``pydata_sphinx_theme``

## Install

```console

   pip install metadata-crawler
   conda install -c conda-forge metadata-crawler
```

## Quickstart (CLI)

```console

   # 1) Crawl → write catalogue
   mdc add \
     cat.yaml \
     --config-file drs_config.toml \
     --dataset cmip6-fs,obs-fs \
     --threads 4 --batch-size 100

   # 2) Index from catalogue → Solr (or Mongo)
   mdc solr index \
     cat.yaml \
     --server localhot:8983

   # 3) Delete by facets (supports globs on values)
   mdc delete \
     --server localhost:8983 \
     --facets "file *.nc" --facets "project CMIP6"
```

> [!NOTE]
>   The CLI is a **custom framework** inspired by Typer (not Typer itself).
>   Use ``--help`` on any subcommand to see all options.

## Minimal config (``drs_config.toml``)

```toml

   # === Canonical schema ===
   [drs_settings.schema.file]
   key      = "file"
   type     = "path"
   required = true
   indexed  = true
   unique   = true

   [drs_settings.schema.uri]
   key      = "uri"
   type     = "uri"
   required = true
   indexed  = true

   [drs_settings.schema.variable]
   key          = "variable"
   type         = "string[]"
   multi_valued = true
   indexed      = true

   [drs_settings.schema.time]
   key     = "time"
   type    = "datetime[2]"     # [start, end]
   indexed = true
   default = []

   [drs_settings.schema.bbox]
   key     = "bbox"
   type    = "float[4]"        # [W,E,S,N]
   default = [0, 360, -90, 90]

   # === Dialect: CMIP6 (example) ===
   [drs_settings.dialect.cmip6]
   sources   = ["path","data"]         # path | data | storage
   defaults.grid_label = "gn"
   specs_dir  = ["mip_era","activity_id","institution_id","source_id","experiment_id","member_id","table_id","variable_id","grid_label","version"]
   specs_file = ["variable_id","table_id","source_id","experiment_id","member_id","grid_label","time"]

   [drs_settings.dialect.cmip6.special.realm]
   type   = "method"
   method = "_get_realm"
   args   = ["table_id","variable_id","__file_name__"]

   [drs_settings.dialect.cmip6.special.time_aggregation]
   type   = "method"
   method = "_get_aggregation"
   args   = ["table_id","variable_id","__file_name__"]

   # === Dialect: CORDEX (bbox by domain) ===
   [drs_settings.dialect.cordex]
   sources   = ["path","data"]
   specs_dir = ["project","product","domain","institution","driving_model","experiment","ensemble","rcm_name","rcm_version","time_frequency","variable","version"]
   specs_file= ["variable","domain","driving_model","experiment","ensemble","rcm_name","rcm_version","time_frequency","time"]

   [drs_settings.dialect.cordex.special.bbox]
   type   = "call"
   method = "dialect['cordex']['domains'].get('{{domain | upper }}', [0,360,-90,90])"

   [drs_settings.dialect.cordex.domains]
   EUR-11 = [-44.14, 64.40, 22.20, 72.42]
   AFR-44 = [-24.64, 60.28, -45.76, 42.24]

   # === Datasets ===
   [cmip6-fs]
   root_path  = "/data/model/global/cmip6"
   drs_format = "cmip6"             # dialect name
   fs_type    = "posix"

   [cmip6-s3]
   root_path        = "s3://test-bucket/data/model/global/cmip6"
   drs_format       = "cmip6"
   fs_type          = "s3"
   storage_options.endpoint_url = "http://127.0.0.1:9000"
   storage_options.aws_access_key_id = "minioadmin"
   storage_options.aws_secret_access_key = "minioadmin"
   storage_options.region_name = "us-east-1"
   storage_options.url_style   = "path"
   storage_options.use_ssl     = false

   [obs-fs]
   root_path  = "/arch/observations"
   drs_format = "custom"
   # define your specs_dir/specs_file or inherit from another dialect
```

## Concepts

### Schema (facet definitions)

Each canonical facet describes:

- ``key``: where to read value (``"project"``, ``"variable"``,)
- ``type``: ``string``, ``integer``, ``float``, ``datetime``, with arrays like
  ``float[4]``, ``string[]``, ``datetime[2]``, or special types like ``file``,
  ``uri``, ``fs_type``, ``dataset``, ``fmt``
- ``required``, ``default``, ``indexed``, ``unique``, ``multi_valued``

### Dialects

A dialect tells the crawler how to **interpret paths** and **read data**:

- ``sources``: which sources to consult (``path``, ``data``, ``storage``) in priority
- ``specs_dir`` / ``specs_file``: ordered facet names encoded in directory and file names
- ``data_specs``: pull values from dataset content (attrs/variables); supports
  ``__variable__`` and templated specs
- ``special``: computed fields (``conditional`` | ``method`` | ``function``)
- Optional lookups (e.g., CORDEX ``domains`` for bbox)

### Path specs vs data specs

- **Path specs** parse segments from the path, e.g.:
  ``/project/product/institute/model/experiment/.../variable_time.nc``
- **Data specs** read from the dataset itself (e.g., xarray/global attribute, variable
  attributes, per-var stats). Example: gather all variables ``__variable__``, then
  their units with a templated selector.

### Inheritance

Create new dialects/datasets by inheriting:

```toml

   [drs_settings.dialect.reana]
   inherits_from = "cmip5"
   sources       = ["path","data"]
   [drs_settings.dialect.reana.data_specs.read_kws]
   engine = "h5netcdf"
```

## Python API

### Async

```python

   import asyncio
   from metadata_crawler.run import async_add, async_index, async_delete

   async def main():
       # crawl → catalogue
       await async_add(
           "cat.yaml",
           config_file="drs_config.toml",
           dataset_names=["cmip6-fs"],
           threads=4,
           batch_size=100,
       )
       # index → backend
       await async_index(
           "solr",
           "cat.yaml",
           config_file="drs_config.toml",
           server="localhost:8983",
       )
       # delete by facets
       await async_delete(
           config_path="drs_config.toml",
           index_store="solr",
           facets=[("file", "*.nc")],
       )

   asyncio.run(main())
```


### Sync (simple wrapper)

```python

   import asyncio
   from metadata_crawler import add

   add(
       store="cat.yaml",
       config_file="drs_config.toml",
       dataset_names=["cmip6-fs"],
   )
```

## Index backends

- **MongoDB** (Motor): upserts by unique facet (e.g., ``file``), bulk deletes (glob → regex)
- **Solr**: fields align with managed schema; supports multi-valued facets


## Contributing

Development install:

```console

   git clone https://github.com/freva-org/metadata-crawler.git
   cd metadata-crawler
   pip install -e .

```

PRs and issues welcome. Please add tests and keep examples minimal & reproducible
(use the MinIO compose stack). Run:


```console
   python -m pip install tox
   tox -e test lint types
```

### Benchmarks
For benchmarking you can create a directory tree with roughly 1.5 M files by
calling the ``create-cordex.sh`` script in the ``dev-env`` folder:

```console
./dev-env/create-cordex.sh
python dev-env/benchmark.py --max-files 20000
```


See ``code-of-conduct.rst`` and ``whatsnew.rst`` for guidelines and changelog.

Use MinIO or LocalStack via ``docker-compose`` and seed a bucket (e.g., ``test-bucket``).
Then point a dataset’s ``fs_type = "s3"`` and set ``storage_options``.

### Documentation

Built with Sphinx + ``pydata_sphinx_theme``. Build locally:

```console
   tox -e docs
```
