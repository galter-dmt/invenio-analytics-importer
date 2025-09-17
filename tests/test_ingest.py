# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 Northwestern University.
#
# invenio-analytics-importer is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

import json

from invenio_search import current_search_client

from invenio_analytics_importer.cache import Cache
from invenio_analytics_importer.ingest import ingest_download_analytics_from_filepaths
from invenio_search.engine import dsl


def write_json(filepath, content):
    """Write json filepath."""
    with open(filepath, "w") as f:
        json.dump(content, f)
    return filepath


def fill_cache(cache, record, file_key):
    """Fills the cache for record + file_key."""
    file_id = str(record.files[file_key].file.file_model.id)
    cache.set_bucket_id(record.pid.pid_value, record.bucket_id)
    cache.set_file_id(record.pid.pid_value, file_key, file_id)
    cache.set_parent_pid(record.pid.pid_value, record.parent.pid.pid_value)
    # know that each file in test is 9 bytes
    cache.set_size(file_id, 9)


def test_ingest_analytics_from_filepaths(running_app, record_factory, tmp_path):
    # Prepare records
    r1 = record_factory.create_record(filenames=["PNB-7-75.txt", "PNB 7 76.txt"])
    r2 = record_factory.create_record(filenames=["coffee.assess.nobmi.txt"])
    r2.index.refresh()  # r2 and r1 share same index

    # Prepare fake analytics files
    fp1 = write_json(
        tmp_path / "downloads_2024-08.json",
        {
            "2024-08-30": [
                {
                    "label": f"prism.northwestern.edu/records/{r1.pid.pid_value}/files/PNB-7-75.txt?download=1",
                    "nb_hits": 1,
                    "nb_uniq_visitors": 1,
                    "nb_visits": 1,
                    "sum_time_spent": 0,
                },
                {
                    "label": f"prism.northwestern.edu/records/{r1.pid.pid_value}/files/PNB 7 76.txt?download=1",
                    "nb_hits": 1,
                    "nb_uniq_visitors": 1,
                    "nb_visits": 1,
                    "sum_time_spent": 0,
                },
            ]
        },
    )
    fp2 = write_json(
        tmp_path / "downloads_2024-09.json",
        {
            "2024-09-01": [
                {
                    "label": f"prism.northwestern.edu/records/{r1.pid.pid_value}/files/PNB 7 76.txt?download=1",
                    "nb_hits": 1,
                    "nb_uniq_visitors": 1,
                    "nb_visits": 1,
                    "sum_time_spent": 0,
                },
                {
                    "label": f"prism.northwestern.edu/records/{r2.pid.pid_value}/files/coffee.assess.nobmi.txt?download=1",
                    "nb_hits": 3,
                    "nb_uniq_visitors": 1,
                    "nb_visits": 2,
                    "sum_time_spent": 0,
                },
            ]
        }
    )
    filepaths = [fp1, fp2]

    # Prepare cache
    cache = Cache()
    fill_cache(cache, r1, "PNB-7-75.txt")
    fill_cache(cache, r1, "PNB 7 76.txt")
    fill_cache(cache, r2, "coffee.assess.nobmi.txt")

    # Actual function under test
    ingest_download_analytics_from_filepaths(filepaths, cache)

    current_search_client.indices.refresh(index="stats-file-*")

    # Test stats-file-download-2024-08 - PNB 7 76.txt
    s = dsl.Search(
        using=current_search_client,
        index="stats-file-download-2024-08"
    )
    file_id = str(r1.files["PNB 7 76.txt"].file.file_model.id)
    unique_id = f"{r1.bucket_id}_{file_id}"
    result = next((h for h in s.scan() if h["unique_id"] == unique_id), None)

    assert "timestamp" in result
    assert 1 == result["count"]
    assert "updated_timestamp" in result
    assert 1 == result["unique_count"]
    assert 9 == result["volume"]
    assert file_id == result["file_id"]
    assert "PNB 7 76.txt" == result["file_key"]
    assert str(r1.bucket_id) == result["bucket_id"]
    assert r1.pid.pid_value == result["recid"]
    assert r1.parent.pid.pid_value == result["parent_recid"]

    # Test stats-file-download-2024-09 - coffee.assess.nobmi.txt
    s = dsl.Search(
        using=current_search_client,
        index="stats-file-download-2024-09"
    )
    file_id = str(r2.files["coffee.assess.nobmi.txt"].file.file_model.id)
    unique_id = f"{r2.bucket_id}_{file_id}"
    result = next((h for h in s.scan() if h["unique_id"] == unique_id), None)

    assert "timestamp" in result
    assert 3 == result["count"]
    assert "updated_timestamp" in result
    assert 2 == result["unique_count"]
    assert 3*9 == result["volume"]
    assert file_id == result["file_id"]
    assert "coffee.assess.nobmi.txt" == result["file_key"]
    assert str(r2.bucket_id) == result["bucket_id"]
    assert r2.pid.pid_value == result["recid"]
    assert r2.parent.pid.pid_value == result["parent_recid"]
