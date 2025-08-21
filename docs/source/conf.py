"""
Sphinx configuration for metadata‑crawler documentation.

This configuration uses the PyData Sphinx theme and enables a number of
extensions to provide a modern look and feel and automatic API
documentation.  It is intentionally minimal; you can extend it as
needed.
"""

from __future__ import annotations

import io
import os
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime

# Include the project source on the Python path so sphinx can find it.
sys.path.insert(0, os.path.abspath("."))
sys.path.insert(0, os.path.abspath("../src"))

from metadata_crawler import __version__
from metadata_crawler.cli import cli


def get_cli_output(*args: str) -> str:
    if args:
        cmd = list(args) + ["--help"]
    else:
        cmd = ["--help"]
    command = f"metadata-crawler {' '.join(args)} --help"
    buf = io.StringIO()
    try:
        with redirect_stderr(buf), redirect_stdout(buf):
            cli(cmd)
    except SystemExit:
        pass
    output = buf.getvalue()
    return f"{command}\n{output}\n"


# -- General configuration ------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "myst_parser",  # support for Markdown files if desired
]

ogp_site_url = "https://freva-org.github.io/freva-admin"
opg_image = (
    "https://freva-org.github.io/freva-admin/_images/freva_flowchart-new.png",
)
ogp_type = "website"
ogp_custom_meta_tags = [
    '<meta name="twitter:card" content="summary_large_image">',
    '<meta name="keywords" content="metadata, climate, data, freva, science, reproducibility">',
]
html_meta = {
    "description": "Index climate metadata.",
    "keywords": "freva, climate, data analysis, freva, metadata, climate science",
    "author": "Freva Team",
    "og:title": "Metadata Crawler",
    "og:description": "Index climate metadata.",
    "og:type": "website",
    "og:url": "https://freva-org.github.io/freva-admin/",
    "og:image": "https://freva-org.github.io/freva-admin/_images/freva_flowchart-new.png",
    "twitter:card": "summary_large_image",
    "twitter:title": "Freva – Evaluation System Framework",
    "twitter:description": "Search, analyse and evaluate climate model data.",
    "twitter:image": "https://freva-org.github.io/freva-admin/_images/freva_flowchart-new.png",
}

# -- MyST options ------------------------------------------------------------

# This allows us to use ::: to denote directives, useful for admonitions
myst_enable_extensions = ["colon_fence", "linkify", "substitution"]
myst_heading_anchors = 2
myst_substitutions = {
    "rtd": "[Read the Docs](https://readthedocs.org/)",
    "version": __version__,
    "cli_main": get_cli_output(),
    "cli_crawl": get_cli_output("crawl"),
    "cli_config": get_cli_output("config"),
    "cli_walk": get_cli_output("walk-intake"),
    "cli_mongo": get_cli_output("mongo"),
    "cli_mongo_index": get_cli_output("mongo", "index"),
    "cli_mongo_delete": get_cli_output("mongo", "delete"),
    "cli_solr": get_cli_output("solr"),
    "cli_solr_index": get_cli_output("solr", "index"),
    "cli_solr_delete": get_cli_output("solr", "delete"),
}
myst_url_schemes = {
    "http": None,
    "https": None,
}
# Substitutions
rst_prolog = """
.. version replace:: {version}
""".format(
    version=__version__,
)


autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "private-members": False,
    "special-members": "__init__",
    "inherited-members": True,
    "show-inheritance": True,
}

# Set the default role so that bare text becomes code when appropriate.
# default_role = "py"

templates_path = ["_templates"]

exclude_patterns: list[str] = []

# -- Options for HTML output ----------------------------------------------
html_theme = "pydata_sphinx_theme"
html_logo = "_static/freva_owl.svg"
html_favicon = "_static/freva_owl.svg"
html_theme_options = {
    "github_url": "https://github.com/freva-org/metadata-crawler",
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
}
html_context = {
    "github_user": "freva-org",
    "github_repo": "metadata-crawler",
    "github_version": "main",
    "doc_path": "docs",
}

html_static_path = ["_static"]

# Add any paths that contain custom static files (such as style sheets)
# relative to this directory. They are copied after the builtin static
# files, so a file named ``default.css`` will overwrite the builtin
# theme’s CSS.

# -- Intersphinx configuration --------------------------------------------
# intersphinx_mapping = {
#    "python": ("https://docs.python.org/3", None),
# }

# The master toctree document.
master_doc = "index"

project = "metadata‑crawler"

# The full version, including alpha/beta/rc tags.
release = __version__
version = release

copyright = f"{datetime.now().year}, freva.org"

# ReadTheDocs has its own way of generating sitemaps, etc.
if not os.environ.get("READTHEDOCS"):
    extensions += ["sphinx_sitemap"]

    html_baseurl = os.environ.get("SITEMAP_URL_BASE", "http://127.0.0.1:8000/")
    sitemap_locales = [None]
    sitemap_url_scheme = "{link}"

# specifying the natural language populates some key tags
language = "en"
