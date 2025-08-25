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
from urllib.parse import urljoin

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
    "sphinxext.opengraph",
    "myst_parser",  # support for Markdown files if desired
]
# Public base URL where the built docs are served.
# Prefer an ENV var; fall back to GitHub Pages for this repo.
SITE_BASEURL = os.environ.get(
    "SITE_BASEURL", "https://freva-org.github.io/metadata-crawler/"
)

# Auto docs
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = True

# Paths to your social preview image (ideal size: 1200x630).
# Put a copy in docs/_static/social_card.png (wide) for best results.
OG_IMAGE = urljoin(SITE_BASEURL, "_static/final_logo.png")
ogp_site_url = SITE_BASEURL
ogp_image = OG_IMAGE
ogp_image_alt = "Metadata Crawler – pastel node web logo"
ogp_type = "website"
ogp_description_length = 300
ogp_use_first_image = False  # keep explicit ogp_image
ogp_custom_meta_tags = (
    '<meta property="og:site_name" content="metadata-crawler">',
    '<meta property="og:locale" content="en_US">',
    '<meta name="twitter:card" content="summary_large_image">',
    '<meta name="twitter:site" content="@freva_org">',  # adjust or remove
    '<meta name="twitter:creator" content="@freva_org">',  # adjust or remove
    '<meta name="theme-color" content="#5B6C8F">',  # matches logo line colour
)

html_meta = {
    "description": "Index climate metadata.",
    "keywords": "freva, climate, data analysis, freva, metadata, climate science",
    "author": "Freva Team",
    "og:title": "Metadata Crawler",
    "og:description": "Index climate metadata.",
    "og:type": "website",
    "og:url": SITE_BASEURL,
    "og:image": OG_IMAGE,
    "twitter:card": "summary_large_image",
    "twitter:title": "Metadata Crawler",
    "twitter:description": "Search, analyse and evaluate climate model data.",
    "twitter:image": OG_IMAGE,
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

exclude_patterns = ["_build", "build", ".DS_Store", "Thumbs.db"]
# -- Options for HTML output ----------------------------------------------
html_theme = "pydata_sphinx_theme"
html_logo = None
html_favicon = "_static/favicon.png"

html_theme_options = {
    # "github_url": "https://github.com/freva-org/metadata-crawler",
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
    "logo": {
        "image_light": "_static/final_logo-light.png",
        "image_dark": "_static/final_logo-dark.png",
        "text": "Metadata Crawler",
    },
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/freva-org/metadata-crawler",
            "icon": "fab fa-github",
            "type": "fontawesome",
        },
        # {"name": "PyPI", "url": "https://pypi.org/project/metadata-crawler/",
        #  "icon": "fas fa-box", "type": "fontawesome"}
    ],
}


html_context = {
    "github_user": "freva-org",
    "github_repo": "metadata-crawler",
    "github_version": "main",
    "doc_path": "docs",
    "default_mode": "dark",
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

    html_baseurl = SITE_BASEURL
    sitemap_locales = [None]
    sitemap_url_scheme = "{link}"

# specifying the natural language populates some key tags
language = "en"
