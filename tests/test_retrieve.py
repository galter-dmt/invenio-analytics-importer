# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 Northwestern University.
#
# invenio-analytics-importer is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

import dataclasses
import json

import pytest

from invenio_analytics_importer.retrieve import (
    MatomoAnalytics,
    get_downloads_per_day,
    get_views_per_day,
)


@dataclasses.dataclass
class FakeResponse:
    """Fake response."""

    json_body: str

    def raise_for_status(self):
        """Faked raise_for_status."""
        return False

    @property
    def text(self):
        """Faked text."""
        return self.json_body

    def json(self):
        """Faked json."""
        return json.loads(self.json_body)


class FakeClient:
    """Fake httpx client."""

    def __init__(self):
        """Constructor."""
        self._response_data = {}

    def set_response(self, criteria, json_body=""):
        """Register a response."""
        self._response_data[criteria] = {
            "json_body": json_body,
        }

    def get_response(self, criteria):
        """Get response."""
        data = self._response_data[criteria]
        return FakeResponse(**data)

    async def post(self, url, params=None, data=None):
        """Post."""
        return self.get_response((url, params["date"]))


@pytest.mark.asyncio
async def test_get_downloads_per_day(running_app, record_factory, tmp_path):
    days = ["2024-08-01", "2024-08-02"]
    analytics_2024_08_01 = [
        {
            "label": f"prism.northwestern.edu/records/paah4-s0w35/files/PNB-7-75.txt?download=1",  # noqa
            "nb_hits": 1,
            "nb_uniq_visitors": 1,
            "nb_visits": 1,
            "sum_time_spent": 0,
        },
        {
            "label": f"prism.northwestern.edu/records/t8k1h-p8435/files/PNB 7 76.txt?download=1",  # noqa
            "nb_hits": 1,
            "nb_uniq_visitors": 1,
            "nb_visits": 1,
            "sum_time_spent": 0,
        },
    ]
    fake_client = FakeClient()
    base_url = "https://matomo.example.org/"
    fake_client.set_response(
        (base_url, days[0]), json.dumps(analytics_2024_08_01)
    )
    fake_client.set_response((base_url, days[1]), "No data available")
    site_id = 3
    token = "token"
    api_client = MatomoAnalytics(fake_client, base_url, site_id, token)

    result = await get_downloads_per_day(api_client, days)

    assert analytics_2024_08_01 == result["2024-08-01"]
    assert [] == result["2024-08-02"]
