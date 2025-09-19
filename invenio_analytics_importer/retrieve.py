# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 Northwestern University.
#
# invenio-analytics-importer is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Retrieve aggregate analytics from provider."""

import asyncio
import dataclasses
from typing import Any

import httpx

from invenio_analytics_importer.write import write_json


@dataclasses.dataclass
class MatomoAnalytics:
    """Matomo API client."""

    client: Any
    base_url: str
    site_id: int
    token_auth: str

    async def get_analytics_for_day(self, method, day):
        """Get downloads for given YYYY-MM-DD in json format.

        Response format:

        {
            "label": "example.org/records/0dfv3-cmw61/files/f.pdf?download=1",
            "nb_hits": 1,
            "nb_uniq_visitors": 1,
            "nb_visits": 1,
            "sum_time_spent": 0,
        },
        """
        params = {
            "module": "API",
            "format": "json",
            "idSite": self.site_id,
            "period": "day",
            "date": day,
            "method": method,
            "flat": 1,
            "showMetadata": 0,
        }

        try:
            response = await self.client.post(
                self.base_url,
                params=params,
                data={"token_auth": self.token_auth}
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            print(f"Error : {exc.request.url} : {exc}")
            return []

        if response.text == "No data available":
            return []

        results = response.json()

        return results


async def get_analytics_per_day(api_client, action, days):
    """Batch retrieve an action analytics per day as dict."""
    analytics = await asyncio.gather(
        *[
            api_client.get_analytics_for_day(action, day)
            for day in days
        ]
    )
    return dict(zip(days, analytics))


async def get_downloads_per_day(api_client, days):
    """Batch retrieve downloads per day as dict."""
    return await get_analytics_per_day(api_client, "Actions.getDownloads", days)  # noqa


async def get_views_per_day(api_client, days):
    """Batch retrieve downloads per day as dict."""
    return await get_analytics_per_day(api_client, "Actions.getPageUrls", days)


async def retrieve_analytics(
    base_url, site_id, token, days_by_year_month, output_dir
):
    """Retrieve analytics from provider."""
    async with httpx.AsyncClient() as client:
        api_client = MatomoAnalytics(client, base_url, site_id, token)

        for year_month, days in days_by_year_month.items():
            year, _, month = year_month.partition("-")

            downloads = await get_downloads_per_day(api_client, days)
            write_json(
                output_dir / f"downloads_{year}_{month}.json",
                downloads,
            )

            views = await get_views_per_day(api_client, days)
            write_json(
                output_dir / f"views_{year}_{month}.json",
                views,
            )
