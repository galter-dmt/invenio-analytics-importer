# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 Northwestern University.
#
# invenio-analytics-importer is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Conftest."""

import io

import pytest
from invenio_access.permissions import system_identity
from invenio_app.factory import create_app as _create_app
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_records_resources.proxies import current_service_registry
from invenio_users_resources.proxies import current_users_service


@pytest.fixture(scope="module")
def app_config(app_config):
    """Override pytest-invenio app_config fixture.

    For test purposes we need to explicitly set these configuration variables
    above any other module's config.py potential clashes.
    """
    app_config.update(
        {
            # Variable not used. We set it to silent warnings
            "JSONSCHEMAS_HOST": "not-used",
            # Disable DATACITE.
            "DATACITE_ENABLED": False,
            "RECORDS_REFRESOLVER_CLS": ("invenio_records.resolver.InvenioRefResolver"),  # noqa
            "RECORDS_REFRESOLVER_STORE": (
                "invenio_jsonschemas.proxies.current_refresolver_store"
            ),
            "MAIL_DEFAULT_SENDER": ("Prism", "no-reply@localhost"),
            # Uncomment to investigate SQL queries
            # 'SQLALCHEMY_ECHO': True,
        }
    )

    return app_config


@pytest.fixture(scope="module")
def create_app():
    """Create app fixture for UI+API app."""
    return _create_app


def create_vocabulary_type(id_, pid):
    """Create vocabulary type."""
    vocabulary_service = current_service_registry.get("vocabularies")
    return vocabulary_service.create_type(system_identity, id_, pid)


@pytest.fixture(scope="module")
def resourcetypes_type(app):
    """Resource type vocabulary type."""
    return create_vocabulary_type("resourcetypes", "rsrct")


@pytest.fixture(scope="module")
def resourcetypes(app, resourcetypes_type):
    """Resource type vocabulary record."""
    vocabulary_service = current_service_registry.get("vocabularies")
    vocabulary_service.create(
        system_identity,
        {  # create parent resource type
            "id": "text",
            "title": {"en": "Text Resources"},
            "type": "resourcetypes",
        },
    )
    program = vocabulary_service.create(
        system_identity,
        {
            "icon": "file alternate",
            "id": "text-program",
            "props": {
                "coar": "text",
                "csl": "",
                "datacite_general": "Text",
                "datacite_type": "Program",
                "eurepo": "info:eu-repo/semantics/other",
                "openaire_resourceType": "",
                "openaire_type": "Text",
                "schema.org": "https://schema.org/TextDigitalDocument",
                "subtype": "text-program",
                "type": "text",
            },
            "title": {"en": "Program"},
            "tags": ["depositable", "linkable"],
            "type": "resourcetypes",
        },
    )
    return [program]


@pytest.fixture(scope="module")
def running_app(app, location, resourcetypes):
    """Running app."""
    return app


@pytest.fixture()
def minimal_record():
    """Minimal record data as dict coming from the external world."""
    return {
        "pids": {},
        "access": {
            "record": "public",
            "files": "public",
        },
        "files": {
            "enabled": False,  # Most tests don't care about files
        },
        "metadata": {
            "creators": [
                {
                    "person_or_org": {
                        "family_name": "Brown",
                        "given_name": "Troy",
                        "type": "personal",
                    }
                },
                {
                    "person_or_org": {
                        "name": "Troy Inc.",
                        "type": "organizational",
                    },
                },
            ],
            "publication_date": "2020-06-01",
            # because DATACITE_ENABLED is True, this field is required
            "publisher": "Acme Inc",
            "resource_type": {"id": "text-program"},
            "title": "A Romans story",
        },
    }


@pytest.fixture()
def index_users():
    """Index users for an up-to-date user service."""

    def _index():
        current_users_service.indexer.process_bulk_queue()
        current_users_service.record_cls.index.refresh()

    return _index


@pytest.fixture()
def uploader(UserFixture, app, db, index_users):
    """Uploader."""
    u = UserFixture(
        email="uploader@inveniosoftware.org",
        password="uploader",
        preferences={
            "visibility": "public",
            "email_visibility": "restricted",
            "notifications": {
                "enabled": True,
            },
        },
        active=True,
        confirmed=True,
    )
    u.create(app, db)
    index_users()

    return u


@pytest.fixture()
def record_factory(db, uploader, minimal_record, location):
    """Creates a record that belongs to a community."""

    class RecordFactory:
        """Test record class."""

        def create_record(
            self,
            record_dict=minimal_record,
            uploader=uploader,
            # community=None,
            filenames=None,
        ):
            """Creates new record that belongs to the same community."""
            service = current_rdm_records_service
            files_service = service.draft_files
            idty = uploader.identity
            filenames = filenames or []

            # create draft
            if filenames:
                record_dict["files"] = {"enabled": True}

            draft = service.create(idty, record_dict)

            # add file to draft
            if filenames:
                data = [{"key": fn} for fn in filenames]
                files_service.init_files(idty, draft.id, data=data)
            for fn in filenames:
                files_service.set_file_content(
                    idty, draft.id, fn, io.BytesIO(b"test file")
                )
                files_service.commit_file(idty, draft.id, fn)

            # publish and get record
            result_item = service.publish(idty, draft.id)
            record = result_item._record

            return record

    return RecordFactory()
