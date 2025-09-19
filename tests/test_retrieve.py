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


@pytest.mark.asyncio
async def test_get_views_per_day(running_app, record_factory, tmp_path):
    days = ["2024-08-01", "2024-08-02"]
    # These are examples of actual responses
    analytics_2024_08_01 = [
        {
            "avg_page_load_time": 1.904,
            "avg_time_dom_completion": 0.629,
            "avg_time_dom_processing": 0.412,
            "avg_time_network": 0.473,
            "avg_time_on_page": 454,
            "avg_time_server": 0.294,
            "avg_time_transfer": 0.096,
            "bounce_rate": "100%",
            "entry_bounce_count": "1",
            "entry_nb_actions": "1",
            "entry_nb_uniq_visitors": 1,
            "entry_nb_visits": "1",
            "entry_sum_visit_length": "0",
            "exit_nb_uniq_visitors": 1,
            "exit_nb_visits": "2",
            "exit_rate": "67%",
            "label": "/records/3s45v-k5m55",
            "max_time_dom_completion": "0.6440",
            "max_time_dom_processing": "0.7940",
            "max_time_network": "1.3810",
            "max_time_server": "0.4350",
            "max_time_transfer": "0.1760",
            "min_time_dom_completion": "0.6130",
            "min_time_dom_processing": "0.2180",
            "min_time_network": "0.0000",
            "min_time_server": "0.0020",
            "min_time_transfer": "0.0020",
            "nb_hits": 4,
            "nb_hits_following_search": "3",
            "nb_hits_with_time_dom_completion": "2",
            "nb_hits_with_time_dom_processing": "4",
            "nb_hits_with_time_network": "4",
            "nb_hits_with_time_server": "4",
            "nb_hits_with_time_transfer": "4",
            "nb_uniq_visitors": 1,
            "nb_visits": 3,
            "sum_time_spent": 1817,
        },
        {
            "avg_page_load_time": 1.307,
            "avg_time_dom_completion": 0,
            "avg_time_dom_processing": 0.485,
            "avg_time_network": 0.726,
            "avg_time_on_page": 18,
            "avg_time_server": 0.091,
            "avg_time_transfer": 0.005,
            "bounce_rate": "29%",
            "entry_bounce_count": "2",
            "entry_nb_actions": "47",
            "entry_nb_uniq_visitors": 6,
            "entry_nb_visits": "7",
            "entry_sum_visit_length": "3236",
            "exit_nb_uniq_visitors": 2,
            "exit_nb_visits": "2",
            "exit_rate": "22%",
            "label": "/",
            "max_time_dom_completion": None,
            "max_time_dom_processing": "1.2450",
            "max_time_network": "5.7480",
            "max_time_server": "0.2580",
            "max_time_transfer": "0.0250",
            "min_time_dom_completion": None,
            "min_time_dom_processing": "0.1360",
            "min_time_network": "0.0000",
            "min_time_server": "0.0110",
            "min_time_transfer": "0.0000",
            "nb_hits": 12,
            "nb_hits_following_search": "2",
            "nb_hits_with_time_dom_completion": "0",
            "nb_hits_with_time_dom_processing": "10",
            "nb_hits_with_time_network": "12",
            "nb_hits_with_time_server": "12",
            "nb_hits_with_time_transfer": "12",
            "nb_uniq_visitors": 7,
            "nb_visits": 9,
            "sum_time_spent": 220,
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

    result = await get_views_per_day(api_client, days)

    assert analytics_2024_08_01 == result["2024-08-01"]
    assert [] == result["2024-08-02"]
