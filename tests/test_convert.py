# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 Northwestern University.
#
# invenio-analytics-importer is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

from invenio_analytics_importer.convert import EntryOfDownloadFromAnalytics
import pytest


@pytest.fixture
def entry_analytics_raw():
    """Raw Matamo analytics entry."""
    return (
        "2024-08-30",
        {
            "label": "prism.northwestern.edu/records/3s45v-k5m55/files/coffee.assess.bmi.gz?download=1",
            "nb_hits": 5,
            "nb_uniq_visitors": 2,
            "nb_visits": 3,
            "sum_time_spent": 0,
        },
    )


def test_entry_create(entry_analytics_raw):
    entry = EntryOfDownloadFromAnalytics.create(*entry_analytics_raw)

    assert entry.pid == "3s45v-k5m55"
    assert entry.file_key == "coffee.assess.bmi.gz"
    assert entry.visits == 3
    assert entry.views == 5
    assert entry.year_month_day == "2024-08-30"
