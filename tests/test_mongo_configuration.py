"""Read-only configuration verification with no external Mongo connection."""

from copy import deepcopy
from unittest.mock import MagicMock

import pytest
from pymongo.errors import OperationFailure

from dhtrader import verify_database_configuration
from dhtrader import dhmongo as dhm
from dhtrader import dhstore as dhs


DATABASE = "DELETEME_configuration_unit"


@pytest.fixture
def configured(monkeypatch):
    """Supply an ordinary compound index through a read-only mocked client."""
    configuration = {
        "schema_version": 1,
        "databases": {
            DATABASE: {
                "collections": {
                    "records": {
                        "indexes": [{
                            "keys": [["owner", 1], ["sequence", -1]],
                            "unique": True,
                        }],
                    },
                },
            },
        },
    }
    client = MagicMock(spec=["list_database_names", "__getitem__"])
    client.list_database_names.return_value = [DATABASE]
    target = MagicMock(spec=["list_collection_names", "__getitem__"])
    client.__getitem__.return_value = target
    target.list_collection_names.return_value = ["records", "unmanaged"]
    collection = MagicMock(spec=["list_indexes"])
    target.__getitem__.return_value = collection
    collection.list_indexes.return_value = [
        {"name": "_id_", "key": {"_id": 1}, "v": 2},
        {
            "name": "arbitrary_server_name", "v": 2,
            "key": {"owner": 1, "sequence": -1}, "unique": True,
        },
        {"name": "extra_index", "key": {"extra": 1}},
    ]
    monkeypatch.setattr(dhm, "mc", client)
    return configuration, client, target, collection


def test_configuration_verification_is_read_only_and_exported(configured):
    """Accept extra indexes without changing primary state."""
    configuration, client, target, collection = configured
    original = deepcopy(configuration)
    primary = dhm.db
    assert verify_database_configuration is dhs.verify_database_configuration
    assert verify_database_configuration(configuration) == {
        "ok": True, "issues": [],
    }
    assert dhm.db is primary
    assert configuration == original
    client.list_database_names.assert_called_once_with()
    client.__getitem__.assert_called_once_with(DATABASE)
    target.list_collection_names.assert_called_once_with()
    target.__getitem__.assert_called_once_with("records")
    collection.list_indexes.assert_called_once_with()


@pytest.mark.parametrize(
    "missing,reason",
    [
        ("database", "missing_database"),
        ("collection", "missing_collection"),
        ("index", "missing_index"),
    ],
)
def test_configuration_reports_missing_storage(configured, missing, reason):
    """Report missing parents once without querying nonexistent children."""
    configuration, client, target, collection = configured
    if missing == "database":
        client.list_database_names.return_value = []
    elif missing == "collection":
        target.list_collection_names.return_value = []
    else:
        collection.list_indexes.return_value = []
    report = dhs.verify_database_configuration(configuration)
    assert report["ok"] is False
    assert report["issues"] == [{
        "database": DATABASE,
        "collection": None if missing == "database" else "records",
        "keys": (
            [["owner", 1], ["sequence", -1]] if missing == "index" else None
        ),
        "reason": reason,
    }]
    if missing == "database":
        client.__getitem__.assert_not_called()
    if missing != "index":
        collection.list_indexes.assert_not_called()


@pytest.mark.parametrize(
    "options",
    [
        {"unique": False}, {"sparse": True}, {"hidden": True},
        {"expireAfterSeconds": 600},
        {"partialFilterExpression": {"owner": {"$exists": True}}},
        {"collation": {"locale": "en"}},
        {"prepareUnique": True},
    ],
)
def test_configuration_rejects_incompatible_index_options(configured, options):
    """An index with matching keys must also enforce the desired semantics."""
    configuration, _, _, collection = configured
    collection.list_indexes.return_value[1].update(options)
    report = dhs.verify_database_configuration(configuration)
    assert report["ok"] is False
    assert report["issues"][0]["reason"] == "index_options_mismatch"


def test_configuration_accepts_one_compatible_index_among_variants(configured):
    """A separate incompatible same-key index does not hide a valid one."""
    configuration, _, _, collection = configured
    indexes = collection.list_indexes.return_value
    incompatible = deepcopy(indexes[1])
    incompatible["unique"] = False
    indexes.insert(0, incompatible)
    indexes[2].update({"sparse": False, "hidden": False})
    assert dhs.verify_database_configuration(configuration)["ok"] is True


def test_configuration_preserves_compound_key_order(configured):
    """Reversed compound keys do not satisfy the configured query index."""
    configuration, _, _, collection = configured
    collection.list_indexes.return_value[1]["key"] = {
        "sequence": -1, "owner": 1,
    }
    report = dhs.verify_database_configuration(configuration)
    assert report["issues"][0]["reason"] == "missing_index"


@pytest.mark.parametrize("explicit_unique", [False, None])
def test_configuration_accepts_nonunique_server_defaults(
        configured, explicit_unique):
    """MongoDB can omit unique:false when reporting a nonunique index."""
    configuration, _, _, collection = configured
    desired = configuration["databases"][DATABASE]["collections"]["records"]
    desired["indexes"][0]["unique"] = False
    actual = collection.list_indexes.return_value[1]
    actual.pop("unique")
    if explicit_unique is not None:
        actual["unique"] = explicit_unique
    assert dhs.verify_database_configuration(configuration)["ok"] is True


def test_configuration_can_select_one_database(configured):
    """Inspecting one database does not query unrelated configured targets."""
    configuration, client, _, _ = configured
    configuration["databases"]["DELETEME_unselected"] = deepcopy(
        configuration["databases"][DATABASE]
    )
    assert dhs.verify_database_configuration(
        configuration, database=DATABASE,
    )["ok"] is True
    client.__getitem__.assert_called_once_with(DATABASE)
    client.reset_mock()
    report = dhs.verify_database_configuration(configuration)
    assert report["issues"][0]["database"] == "DELETEME_unselected"


@pytest.mark.parametrize("database", ["", "unknown", False, 1, []])
def test_configuration_rejects_unconfigured_target(configured, database):
    """Reject a selector typo instead of successfully checking an empty set."""
    configuration, client, _, _ = configured
    with pytest.raises(ValueError, match="database not in configuration"):
        dhs.verify_database_configuration(configuration, database=database)
    client.list_database_names.assert_not_called()


@pytest.mark.parametrize("invalid", [None, {}, [], {"schema_version": 1}])
def test_configuration_rejects_invalid_root(configured, invalid):
    """Invalid JSON structure fails before any Mongo requests."""
    _, client, _, _ = configured
    with pytest.raises(ValueError):
        dhs.verify_database_configuration(invalid)
    client.list_database_names.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", True), ("schema_version", 2),
        ("databases", {}), ("databases", []),
        ("unexpected", True),
    ],
)
def test_configuration_rejects_invalid_schema(configured, field, value):
    """Unsupported versions and misspelled settings cannot be ignored."""
    configuration, client, _, _ = configured
    configuration[field] = value
    with pytest.raises(ValueError):
        dhs.verify_database_configuration(configuration)
    client.list_database_names.assert_not_called()


@pytest.mark.parametrize(
    "index",
    [
        {"keys": [], "unique": True},
        {"keys": [["owner", 1]], "unique": 1},
        {"keys": [["owner", True]], "unique": True},
        {"keys": [["owner", 0]], "unique": True},
        {"keys": [["owner", "text"]], "unique": True},
        {"keys": [["1", 1]], "unique": True},
        {"keys": [["", 1]], "unique": True},
        {"keys": [["owner", 1], ["owner", -1]], "unique": True},
        {"keys": [["owner"]], "unique": True},
        {"keys": [["owner", 1]], "unique": True, "sparse": True},
        {"keys": [["owner", 1]]},
    ],
)
def test_configuration_rejects_invalid_index_definitions(configured, index):
    """Only explicit, ordinary ordered indexes are supported by schema v1."""
    configuration, client, _, _ = configured
    configuration["databases"][DATABASE]["collections"]["records"] = {
        "indexes": [index],
    }
    with pytest.raises(ValueError):
        dhs.verify_database_configuration(configuration)
    client.list_database_names.assert_not_called()


def test_configuration_rejects_duplicate_index_definitions(configured):
    """Reject conflicting definitions for the same index keys."""
    configuration, client, _, _ = configured
    indexes = configuration["databases"][DATABASE]["collections"]["records"]
    indexes["indexes"].append(deepcopy(indexes["indexes"][0]))
    with pytest.raises(ValueError, match="duplicate index keys"):
        dhs.verify_database_configuration(configuration)
    client.list_database_names.assert_not_called()


@pytest.mark.parametrize("name", ["invalid/database", "a" * 64])
def test_configuration_rejects_invalid_database_names(configured, name):
    """Reject invalid deployment names before checking or creating anything."""
    configuration, client, _, _ = configured
    configuration["databases"][name] = configuration["databases"].pop(DATABASE)
    with pytest.raises(ValueError, match="invalid Mongo database name"):
        dhs.verify_database_configuration(configuration)
    client.list_database_names.assert_not_called()


@pytest.mark.parametrize("stage", ["databases", "collections", "indexes"])
def test_configuration_propagates_connection_and_permission_errors(
        configured, stage):
    """An inaccessible server or metadata command is not an empty database."""
    configuration, client, target, collection = configured
    method = {
        "databases": client.list_database_names,
        "collections": target.list_collection_names,
        "indexes": collection.list_indexes,
    }[stage]
    method.side_effect = OperationFailure("metadata access denied")
    with pytest.raises(OperationFailure, match="metadata access denied"):
        dhs.verify_database_configuration(configuration)


def test_configuration_report_does_not_share_mutable_keys(configured):
    """Changing the returned issue cannot change future provisioning inputs."""
    configuration, _, _, collection = configured
    collection.list_indexes.return_value = []
    report = dhs.verify_database_configuration(configuration)
    report["issues"][0]["keys"][0][0] = "mutated"
    desired = configuration["databases"][DATABASE]["collections"]["records"]
    assert desired["indexes"][0]["keys"][0] == ["owner", 1]
