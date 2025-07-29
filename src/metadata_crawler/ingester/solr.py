"""Collection of aync data ingest classes."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Annotated, List

import aiohttp

from ..api.cli import cli_function, cli_parameter
from ..logger import logger
from .base import BaseIngest


class SolrIngest(BaseIngest):
    """Ingest metadata into an apache solr server."""

    def __init__(
        self,
        catalogue_file: str,
        batch_size: int = 2500,
    ) -> None:
        super().__init__(uri=catalogue_file, batch_size=batch_size)
        self.timeout = aiohttp.ClientTimeout(total=50)
        self._uri: str = ""

    async def solr_url(self, server: str, core: str) -> str:
        """Construct the solr url from a given solr core."""
        if not self._uri:
            scheme, _, server = server.rpartition("://")
            scheme = scheme or "http"
            solr_server, _, solr_port = server.partition(":")
            solr_port = solr_port or "8983"
            solr_server = solr_server or "localhost"
            self._uri = f"{scheme}://{solr_server}:{solr_port}/solr"
        return f"{self._uri}/{core}/update/json?commit=true"

    @staticmethod
    async def convert_str_to_timestamp(
        time_str: str, alternative: str = "0"
    ) -> str:
        """Convert a string representation of a time step to an iso timestamp

        Parameters
        ----------
        time_str: str
            Representation of the time step in formats:
            - %Y%m%d%H%M%S%f (year, month, day, hour, minute, second, millisecond)
            - %Y%m%d%H%M (year, month, day, hour, minute)
            - %Y%m (year, month)
            - %Y%m%dT%H%M (year, month, day, hour, minute with T separator)
            - %Y%j (year and day of year, e.g. 2022203 for 22nd July 2022)
            - %Y (year only)
        alternative: str, default: 0
            If conversion fails, the alternative/default value the time step
            gets assign to

        Returns
        -------
        str: ISO time string representation of the input time step, such as
            %Y %Y-%m-%d or %Y-%m-%dT%H%M%S
        """
        has_t_separator = "T" in time_str
        position_t = time_str.find("T") if has_t_separator else -1
        # Strip anything that's not a number from the string
        if not time_str:
            return alternative
        # Not valid if time repr empty or starts with a letter, such as 'fx'
        digits = "".join(filter(str.isdigit, time_str))
        l_times = len(digits)

        if not l_times:
            return alternative
        try:
            if l_times <= 4:
                # Suppose this is a year only
                return digits.zfill(4)
            if l_times <= 6:
                # Suppose this is %Y%m or %Y%e
                return f"{digits[:4]}-{digits[4:].zfill(2)}"
            # Year and day of year
            if l_times == 7:
                # Suppose this is %Y%j
                year = int(digits[:4])
                day_of_year = int(digits[4:])
                date = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
                return date.strftime("%Y-%m-%d")

            if l_times <= 8:
                # Suppose this is %Y%m%d
                return f"{digits[:4]}-{digits[4:6]}-{digits[6:].zfill(2)}"

            date_str = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
            time = digits[8:]
            if len(time) <= 2:
                time = time.zfill(2)
            else:
                # Alaways drop seconds
                time = time[:2] + ":" + time[2 : min(4, len(time))].zfill(2)
            return f"{date_str}T{time}"

        except ValueError:
            if has_t_separator and position_t > 0:
                date_part = time_str[:position_t]
                time_part = time_str[position_t + 1 :]

                date_digits = "".join(filter(str.isdigit, date_part))
                if len(date_digits) >= 8:
                    return f"{date_digits[:4]}-{date_digits[4:6]}-{date_digits[6:8]}T{time_part[:2].zfill(2)}"

            return alternative

    async def get_solr_time_range(self, time: str) -> str:
        """Create a solr time range stamp for ingestion.

        Parameters
        ----------
        time: str
            string representation of the time range
        sep: str, default: -
            separator for start and end time

        Returns
        -------
        str: solr time range string representation
        """
        start, _, end = time.replace(":", "").replace("_", "-").partition("-")
        start_str = await self.convert_str_to_timestamp(start, alternative="0")
        end_str = await self.convert_str_to_timestamp(end, alternative="9999")
        return f"[{start_str} TO {end_str}]"

    @cli_function(
        help="Remove metadata from the apache solr server.",
    )
    async def delete(
        self,
        *,
        server: Annotated[
            str,
            cli_parameter(
                "-s",
                "--server",
                help="The <host>:<port> to the solr server",
                type=str,
            ),
        ] = None,
        facets: Annotated[
            List[tuple[str, str]],
            cli_parameter(
                "-f",
                "--facets",
                type=str,
                nargs=2,
                action="append",
                help="Search facets matching the delete query.",
            ),
        ] = None,
        latest_version: Annotated[
            str,
            cli_parameter(
                "--latest-version",
                type=str,
                help="Name of the core holding 'latest' metadata.",
            ),
        ] = "latest",
        all_versions: Annotated[
            str,
            cli_parameter(
                "--all-versions",
                type=str,
                help="Name of the core holding 'all' metadata versions.",
            ),
        ] = "files",
    ) -> None:
        query = []
        for key, value in facets.items():
            if key.lower() == "file":
                if value[0] in (os.sep, "/"):
                    value = f"\\{value}"
                value = value.replace(":", "\\:")
            else:
                value = value.lower()
            query.append(f"{key.lower()}:{value}")
        query_str = " AND ".join(query)
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            logger.debug("Deleting entries matching %s", query_str)
            for core in (all_versions, latest_version):
                url = await self.solr_url(core)
                async with session.post(
                    url, json={"delete": {"query": query_str}}
                ) as resp:
                    if resp.status not in (200, 201):
                        logger.debug(await resp.text())

                        logger.error(
                            "Status failed with: %i",
                            resp.status,
                        )

    @cli_function(
        help="Add metadata to the apache solr metadata server.",
    )
    async def index(
        self,
        *,
        server: Annotated[
            str,
            cli_parameter(
                "-s",
                "--server",
                help="The <host>:<port> to the solr server",
                type=str,
            ),
        ] = None,
    ) -> None:
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            for core in self.cores:
                async for chunk in self.get_metadata(core):
                    url = await self.solr_url(server, core)
                    # adopt the bbox field for Solr
                    values = []
                    for entry in chunk:
                        items = {}
                        for k, v in entry.items():
                            print(k, v)
                            if k.lower() == self.facets.get("bbox", "bbox"):
                                val = f"ENVELOPE({v[0]}, {v[1]}, {v[3]}, {v[2]})"
                            elif k.lower() == self.facets.get("time", "time"):
                                val = await self.get_solr_time_range(v or "fx")
                            else:
                                val = v
                            items[k] = val
                        values.append(items.copy())
                        del items
                    logger.debug("Sending %i entries to %s", len(values), url)
                    async with session.post(url, json=values) as resp:
                        if resp.status not in (200, 201):
                            logger.error(await resp.text())
                            raise ValueError(
                                f"Solr Ingest failed with: {resp.status} in {core}"
                            )
