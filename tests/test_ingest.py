# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 Northwestern University.
#
# invenio-analytics-importer is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

import datetime as dt

import time_machine
from invenio_search import current_search_client
from invenio_search.engine import dsl

from invenio_analytics_importer.cache import Cache
from invenio_analytics_importer.convert import DownloadAnalytics
from invenio_analytics_importer.ingest import (
    generate_download_stats,
    ingest_statistics,
)


# def fill_cache(cache, record, file_key):
#     """Fills the cache for record + file_key."""
#     file_id = str(record.files[file_key].file.file_model.id)
#     cache.set_bucket_id(record.pid.pid_value, record.bucket_id)
#     cache.set_file_id(record.pid.pid_value, file_key, file_id)
#     cache.set_parent_pid(record.pid.pid_value, record.parent.pid.pid_value)
#     # know that each file in test is 9 bytes
#     cache.set_size(file_id, 9)


# def test_ingest_analytics_from_filepaths(
#     running_app, record_factory, tmp_path
# ):
#     # Prepare records
#     r1 = record_factory.create_record(
#         filenames=["PNB-7-75.txt", "PNB 7 76.txt"]
#     )
#     r2 = record_factory.create_record(filenames=["coffee.assess.nobmi.txt"])
#     r2.index.refresh()  # r2 and r1 share same index

#     # Prepare fake analytics files
#     fp1 = write_json(
#         tmp_path / "downloads_2024-08.json",
#         {
#             "2024-08-30": [
#                 {
#                     "label": f"prism.northwestern.edu/records/{r1.pid.pid_value}/files/PNB-7-75.txt?download=1",  # noqa
#                     "nb_hits": 1,
#                     "nb_uniq_visitors": 1,
#                     "nb_visits": 1,
#                     "sum_time_spent": 0,
#                 },
#                 {
#                     "label": f"prism.northwestern.edu/records/{r1.pid.pid_value}/files/PNB 7 76.txt?download=1",  # noqa
#                     "nb_hits": 1,
#                     "nb_uniq_visitors": 1,
#                     "nb_visits": 1,
#                     "sum_time_spent": 0,
#                 },
#             ]
#         },
#     )
#     fp2 = write_json(
#         tmp_path / "downloads_2024-09.json",
#         {
#             "2024-09-01": [
#                 {
#                     "label": f"prism.northwestern.edu/records/{r1.pid.pid_value}/files/PNB 7 76.txt?download=1",  # noqa
#                     "nb_hits": 1,
#                     "nb_uniq_visitors": 1,
#                     "nb_visits": 1,
#                     "sum_time_spent": 0,
#                 },
#                 {
#                     "label": f"prism.northwestern.edu/records/{r2.pid.pid_value}/files/coffee.assess.nobmi.txt?download=1",  # noqa
#                     "nb_hits": 3,
#                     "nb_uniq_visitors": 1,
#                     "nb_visits": 2,
#                     "sum_time_spent": 0,
#                 },
#             ]
#         },
#     )
#     filepaths = [fp1, fp2]

#     # Prepare cache
#     cache = Cache()
#     fill_cache(cache, r1, "PNB-7-75.txt")
#     fill_cache(cache, r1, "PNB 7 76.txt")
#     fill_cache(cache, r2, "coffee.assess.nobmi.txt")

#     # Actual function under test
#     ingest_download_analytics_from_filepaths(filepaths, cache)

#     current_search_client.indices.refresh(index="stats-file-*")

#     # Test stats-file-download-2024-08 - PNB 7 76.txt
#     s = dsl.Search(
#         using=current_search_client,
#         index="stats-file-download-2024-08"
#     )
#     file_id = str(r1.files["PNB 7 76.txt"].file.file_model.id)
#     unique_id = f"{r1.bucket_id}_{file_id}"
#     result = next((h for h in s.scan() if h["unique_id"] == unique_id), None)

#     assert "timestamp" in result
#     assert 1 == result["count"]
#     assert "updated_timestamp" in result
#     assert 1 == result["unique_count"]
#     assert 9 == result["volume"]
#     assert file_id == result["file_id"]
#     assert "PNB 7 76.txt" == result["file_key"]
#     assert str(r1.bucket_id) == result["bucket_id"]
#     assert r1.pid.pid_value == result["recid"]
#     assert r1.parent.pid.pid_value == result["parent_recid"]

#     # Test stats-file-download-2024-09 - coffee.assess.nobmi.txt
#     s = dsl.Search(
#         using=current_search_client,
#         index="stats-file-download-2024-09"
#     )
#     file_id = str(r2.files["coffee.assess.nobmi.txt"].file.file_model.id)
#     unique_id = f"{r2.bucket_id}_{file_id}"
#     result = next((h for h in s.scan() if h["unique_id"] == unique_id), None)

#     assert "timestamp" in result
#     assert 3 == result["count"]
#     assert "updated_timestamp" in result
#     assert 2 == result["unique_count"]
#     assert 3*9 == result["volume"]
#     assert file_id == result["file_id"]
#     assert "coffee.assess.nobmi.txt" == result["file_key"]
#     assert str(r2.bucket_id) == result["bucket_id"]
#     assert r2.pid.pid_value == result["recid"]
#     assert r2.parent.pid.pid_value == result["parent_recid"]

def test_generate_download_stats():
    cache = Cache()
    cache.set_size("cb297587-b25c-4675-832a-d5d2634551c7", 9)
    cache.set_file_id(
        "5ret9-dwz86",
        file_key="coffee.assess.nobmi.txt",
        file_id="cb297587-b25c-4675-832a-d5d2634551c7",
    )
    cache.set_bucket_id("5ret9-dwz86", "1741e723-420a-4023-ad89-7aedebab7bb1")
    cache.set_parent_pid("5ret9-dwz86", "tb2gj-axd97")

    iter_analytics = [
        DownloadAnalytics(
            year_month_day="2024-09-03",
            pid="5ret9-dwz86",
            file_key="coffee.assess.nobmi.txt",
            visits=2,
            views=3,
        ),
    ]

    pit = dt.datetime(2025, 9, 23, 0, 0, 0, tzinfo=dt.timezone.utc)
    with time_machine.travel(pit):
        stats = generate_download_stats(iter_analytics, cache)
        stats = list(stats)

    expected = {
        "_id": "1741e723-420a-4023-ad89-7aedebab7bb1_cb297587-b25c-4675-832a-d5d2634551c7-2024-09-03",  # noqa
        "_index": f"stats-file-download-2024-09",
        "_source": {
            "timestamp": f"2024-09-03T00:00:00",
            "unique_id": "1741e723-420a-4023-ad89-7aedebab7bb1_cb297587-b25c-4675-832a-d5d2634551c7",  # noqa
            "count": 3,
            "updated_timestamp": "2025-09-23T00:00:00+00:00",
            "unique_count": 2,
            "volume": 27,
            "file_id": "cb297587-b25c-4675-832a-d5d2634551c7",
            "file_key": "coffee.assess.nobmi.txt",
            "bucket_id": "1741e723-420a-4023-ad89-7aedebab7bb1",
            "recid": "5ret9-dwz86",
            "parent_recid": "tb2gj-axd97",
        },
    }
    assert 1 == len(stats)
    assert expected == stats[0]


def test_ingest_statistics(running_app):
    stats_for_ingest = [
        {
            "_id": f"ui_0c8rx-zsn76-2024-08-30",
            "_index": f"stats-record-view-2024-08",
            "_source": {
                "timestamp": f"2024-08-30T00:00:00",
                "unique_id": "ui_0c8rx-zsn76",
                "count": 1,
                "updated_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),  # noqa
                "unique_count": 1,
                "recid": "0c8rx-zsn76",
                "parent_recid": "by5n4-x1h80",
                "via_api": False,
            },
        },
        {
            "_id": "1741e723-420a-4023-ad89-7aedebab7bb1_cb297587-b25c-4675-832a-d5d2634551c7-2024-09-03",  # noqa
            "_index": f"stats-file-download-2024-09",
            "_source": {
                "timestamp": f"2024-09-03T00:00:00",
                "unique_id": "1741e723-420a-4023-ad89-7aedebab7bb1_cb297587-b25c-4675-832a-d5d2634551c7",  # noqa
                "count": 3,
                "updated_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                "unique_count": 2,
                "volume": 27,
                "file_id": "cb297587-b25c-4675-832a-d5d2634551c7",
                "file_key": "coffee.assess.nobmi.txt",
                "bucket_id": "1741e723-420a-4023-ad89-7aedebab7bb1",
                "recid": "5ret9-dwz86",
                "parent_recid": "tb2gj-axd97",
            },
        },
    ]

    # Actual function under test
    ingest_statistics(stats_for_ingest)
    current_search_client.indices.refresh(index="stats-file-*")
    current_search_client.indices.refresh(index="stats-record-*")

    s = dsl.Search(
        using=current_search_client,
        index="stats-record-view-2024-08"
    )
    unique_id = "ui_0c8rx-zsn76"
    result = next(
        (h for h in s.scan() if h["unique_id"] == unique_id),
        None
    )
    assert "timestamp" in result
    assert 1 == result["count"]
    assert "updated_timestamp" in result
    assert 1 == result["unique_count"]
    assert "0c8rx-zsn76" == result["recid"]
    assert "by5n4-x1h80" == result["parent_recid"]
    assert False == result["via_api"]

    s = dsl.Search(
        using=current_search_client,
        index="stats-file-download-2024-09"
    )
    unique_id = "1741e723-420a-4023-ad89-7aedebab7bb1_cb297587-b25c-4675-832a-d5d2634551c7"  # noqa
    result = next(
        (
            h for h in s.scan()
            if h["unique_id"] == unique_id
        ),
        None
    )
    assert "timestamp" in result
    assert 3 == result["count"]
    assert "updated_timestamp" in result
    assert 2 == result["unique_count"]
    assert 27 == result["volume"]
    assert "cb297587-b25c-4675-832a-d5d2634551c7" == result["file_id"]
    assert "coffee.assess.nobmi.txt" == result["file_key"]
    assert "1741e723-420a-4023-ad89-7aedebab7bb1" == result["bucket_id"]
    assert "5ret9-dwz86" == result["recid"]
    assert "tb2gj-axd97" == result["parent_recid"]
