"""Call the Command line interface of the metadata-crawler."""

import sys

from metadata_crawler.cli import cli

if __name__ == "__main__":
    cli(sys.argv[1:])
