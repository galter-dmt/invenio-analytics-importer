# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 Northwestern University.
#
# invenio-analytics-importer is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

import json

from invenio_analytics_importer.cache import generate_cache


def write_json(filepath, content):
    """Write json filepath."""
    with open(filepath, "w") as f:
        json.dump(content, f)
    return filepath


def test_generate_cache(running_app, record_factory, tmp_path):
    r1 = record_factory.create_record(
        filenames=["PNB-7-75.txt", "PNB 7 76.txt"]
    )
    r2 = record_factory.create_record(filenames=["coffee.assess.nobmi.txt"])
    r2.index.refresh() # r2 and r1 share same index

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

    cache = generate_cache([fp1, fp2])

    file_id = cache.get_file_id(r1.pid.pid_value, "PNB 7 76.txt")
    assert str(r1.files["PNB 7 76.txt"].file.file_model.id) == file_id
    parent_pid = cache.get_parent_pid(r1.pid.pid_value)
    assert str(r1.parent.pid.pid_value) == parent_pid
    size = cache.get_size(file_id)
    assert r1.files['PNB 7 76.txt'].file.file_model.size == size
    bucket_id = cache.get_bucket_id(r1.pid.pid_value)
    assert str(r1.bucket_id) == bucket_id
