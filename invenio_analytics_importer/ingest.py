# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 Northwestern University.
#
# invenio-analytics-importer is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Ingest analytics into InvenioRDM's stats indices."""

from datetime import datetime, timezone

from invenio_search import current_search_client
from invenio_search.engine import search

from invenio_analytics_importer.convert import (
    iter_to_entries_of_download_analytics,
)


def to_download(entry, cache):
    """To download."""
    file_id = cache.get_file_id(entry.pid, entry.file_key)
    bucket_id = cache.get_bucket_id(entry.pid)
    year_month_day = entry.year_month_day
    event_name = "file-download"
    year_month = year_month_day.rsplit("-", 1)[0]
    count = entry.views
    unique_count = entry.visits
    volume = count * cache.get_size(file_id)
    file_key = entry.file_key
    recid = entry.pid
    parent_recid = cache.get_parent_pid(recid)

    return {
        "_id": f"{bucket_id}_{file_id}-{year_month_day}",
        "_index": f"stats-{event_name}-{year_month}",
        "_source": {
            # Since those entries are synthetic anyway, we place them at
            # the start of the day
            "timestamp": f"{year_month_day}T00:00:00",
            "unique_id": f"{bucket_id}_{file_id}",
            "count": count,
            "updated_timestamp": datetime.now(timezone.utc).isoformat(),
            "unique_count": unique_count,
            "volume": volume,
            "file_key": file_key,
            "bucket_id": bucket_id,
            "file_id": file_id,
            "recid": recid,
            "parent_recid": parent_recid,
        },
    }


def iter_to_stats_file_download_actions(filepaths, cache):
    """Iterable of download actions as fully formed input to search engine."""
    for entry in iter_to_entries_of_download_analytics(filepaths):
        yield to_download(entry, cache)


def ingest_downloads(downloads):
    """Ingest downloads."""
    search.helpers.bulk(
        current_search_client,
        downloads,
        stats_only=True,
        chunk_size=50,
    )


def ingest_download_analytics_from_filepaths(filepaths, cache):
    """Ingest."""
    downloads = iter_to_stats_file_download_actions(filepaths, cache)
    ingest_downloads(downloads)
