# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 Northwestern University.
#
# invenio-analytics-importer is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

import dataclasses
import re

from .read import iter_day_analytics_from_filepaths


@dataclasses.dataclass
class EntryOfDownloadFromAnalytics:
    """Intermediate representation."""

    year_month_day: str  # keeping it simple for now
    pid: str
    file_key: str
    visits: int
    views: int

    @classmethod
    def create(cls, year_month_day, analytics_raw):
        """Create Entry from raw analytics."""
        label = analytics_raw.get("label", "")

        # extract "3s45v-k5m55" from ".../records/3s45v-k5m55[/...]"
        regex_pid = re.compile(r"/records/([^/]*)(?:/|$)")
        pid = regex_pid.search(label).group(1)

        # extract file key
        regex_key = re.compile(r"/files/([^?]*)\?download=1")
        file_key = regex_key.search(label).group(1)

        return EntryOfDownloadFromAnalytics(
            year_month_day=year_month_day,
            pid=pid,
            file_key=file_key,
            visits=analytics_raw.get("nb_visits", 0),
            views=analytics_raw.get("nb_hits", 0),
        )


def iter_to_entries_of_download_analytics(filepaths):
    """Iterable of download actions as fully formed input to search engine."""
    for year_month_day, raw_analytics in iter_day_analytics_from_filepaths(filepaths):
        yield EntryOfDownloadFromAnalytics.create(year_month_day, raw_analytics)
