"""Tests for custom document store/retrieve/delete/review functions.

Covers store_custom_documents, get_custom_documents_by_field,
get_all_custom_documents, list_custom_documents,
delete_custom_documents_by_field, and review_custom_documents
in dhstore.py, plus their dhmongo backing functions.
"""
import pytest
from dhtrader import (
    COLLECTIONS,
    COLL_PATTERNS,
    delete_custom_documents_by_field,
    get_all_custom_documents,
    get_custom_documents_by_field,
    list_custom_documents,
    review_custom_documents,
    store_custom_documents,
)
from dhtrader.dhmongo import (
    delete_custom_documents_by_field as dhmongo_delete,
)

# ---------------------------------------------------------------------------
# Sentinel values used across all custom-document tests.
# ---------------------------------------------------------------------------

# Non-managed collection name used for all write tests.
_TEST_COLL = "custom_docs_DELETEME"

# Sentinel stored in every test document's "name" field so the
# cleanup fixture can remove them without scanning the whole collection.
_TEST_SENTINEL = "DELETEME_CUSTOM_DOC_TESTS"


@pytest.fixture
def cleanup_custom_docs():
    """Remove test documents before and after each test.

    Deletes all documents in _TEST_COLL where name==_TEST_SENTINEL
    via dhmongo directly (bypassing dhstore guard) so the fixture
    works even when the guard itself is under test.
    """
    def _clean():
        dhmongo_delete(
            collection=_TEST_COLL,
            field="name",
            value=_TEST_SENTINEL,
        )

    _clean()
    yield
    _clean()


# ---------------------------------------------------------------------------
# Guard: managed COLLECTIONS values raise ValueError
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
@pytest.mark.parametrize("coll", list(COLLECTIONS.values()))
def test_custom_document_guard_rejects_managed_collections(coll):
    """store/delete/list/review/get raise ValueError for managed coll."""
    doc = {"name": "should_not_store", "x": 1}
    with pytest.raises(ValueError):
        store_custom_documents(coll, [doc])
    with pytest.raises(ValueError):
        delete_custom_documents_by_field(coll, "name", "x")
    with pytest.raises(ValueError):
        list_custom_documents(coll)
    with pytest.raises(ValueError):
        review_custom_documents(coll)
    with pytest.raises(ValueError):
        get_custom_documents_by_field(coll, "name", "x")
    with pytest.raises(ValueError):
        get_all_custom_documents(coll)


# ---------------------------------------------------------------------------
# Guard: pattern-matched managed collection names raise ValueError
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
@pytest.mark.parametrize("coll", [
    "candles_ES_1m",
    "candles_NQ_5m",
    "events_ES",
    "images.files",
    "images.chunks",
])
def test_custom_document_guard_rejects_pattern_matched_collections(coll):
    """Pattern-matched collections are rejected by the guard."""
    doc = {"name": "should_not_store", "x": 1}
    with pytest.raises(ValueError):
        store_custom_documents(coll, [doc])
    with pytest.raises(ValueError):
        delete_custom_documents_by_field(coll, "name", "x")
    with pytest.raises(ValueError):
        list_custom_documents(coll)
    with pytest.raises(ValueError):
        review_custom_documents(coll)
    with pytest.raises(ValueError):
        get_custom_documents_by_field(coll, "name", "x")
    with pytest.raises(ValueError):
        get_all_custom_documents(coll)


# ---------------------------------------------------------------------------
# gridfs pattern does not block unrelated collections with 'files'/'chunks'
# ---------------------------------------------------------------------------

@pytest.mark.suppress_stdout
@pytest.mark.parametrize("coll", [
    "my_files_collection",
    "chunks_data",
    "report_files",
])
def test_custom_document_guard_allows_non_images_files_chunks(coll):
    """collections with files/chunks not in images bucket are allowed."""
    gridfs_pattern = COLL_PATTERNS["gridfs"]
    assert not gridfs_pattern.search(coll), (
        f"gridfs pattern unexpectedly matched {coll!r}"
    )


# ---------------------------------------------------------------------------
# COLL_PATTERNS is exported correctly
# ---------------------------------------------------------------------------

def test_custom_document_coll_patterns_are_compiled_regexes():
    """COLL_PATTERNS dict is non-empty and values are compiled regexes."""
    import re
    assert isinstance(COLL_PATTERNS, dict)
    assert len(COLL_PATTERNS) > 0
    for key, pattern in COLL_PATTERNS.items():
        assert hasattr(pattern, "search"), (
            f"COLL_PATTERNS[{key!r}] is not a compiled regex"
        )
        assert isinstance(pattern, re.Pattern)


# ---------------------------------------------------------------------------
# Validation errors on bad inputs
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_empty_list_raises():
    """store_custom_documents raises ValueError on empty list."""
    with pytest.raises(ValueError, match="non-empty list"):
        store_custom_documents(_TEST_COLL, [])


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_unconvertible_element_raises():
    """store_custom_documents raises when element cannot be dict-coerced."""
    # A plain string cannot be converted to dict by vars() or dict().
    with pytest.raises(ValueError, match="cannot be converted to dict"):
        store_custom_documents(
            _TEST_COLL,
            [{"name": _TEST_SENTINEL, "ok": 1}, "not_a_dict"],
        )


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_non_json_serializable_raises():
    """store_custom_documents raises ValueError for non-serializable doc."""
    with pytest.raises(ValueError, match="JSON-serializable"):
        store_custom_documents(
            _TEST_COLL,
            [{"name": _TEST_SENTINEL, "bad": object()}],
        )


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_missing_name_raises():
    """store_custom_documents raises ValueError when name key is absent."""
    with pytest.raises(ValueError, match="non-blank 'name'"):
        store_custom_documents(_TEST_COLL, [{"value": 42}])


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_blank_name_raises():
    """store_custom_documents raises ValueError when name is blank."""
    with pytest.raises(ValueError, match="non-blank 'name'"):
        store_custom_documents(_TEST_COLL, [{"name": "  ", "v": 1}])


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_none_name_raises():
    """store_custom_documents raises ValueError when name is None."""
    with pytest.raises(ValueError, match="non-blank 'name'"):
        store_custom_documents(_TEST_COLL, [{"name": None, "v": 1}])


# ---------------------------------------------------------------------------
# dict coercion
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_object_with_dunder_dict_is_coerced(
        cleanup_custom_docs):
    """An object with __dict__ is coerced to dict and stored successfully.

    Verifies the stored document contains the original attribute values.
    """
    class _Doc:
        def __init__(self):
            self.name = _TEST_SENTINEL
            self.payload = "coerced_value"
            self.score = 77

    obj = _Doc()
    # Should not raise; the object is coerced via vars()
    store_custom_documents(_TEST_COLL, [obj])

    # Verify the document was actually stored with correct field values
    results = get_custom_documents_by_field(
        _TEST_COLL, "name", _TEST_SENTINEL
    )
    assert len(results) == 1
    stored = results[0]
    assert stored["name"] == _TEST_SENTINEL
    assert stored["payload"] == "coerced_value"
    assert stored["score"] == 77


# ---------------------------------------------------------------------------
# doc_id assignment
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_assigns_doc_id_prefixed_with_name(
        cleanup_custom_docs):
    """doc_id is auto-assigned as '{name}_{uuid4}' when not supplied.

    Verifies format, prefix, and that retrieval by doc_id returns the
    full original document.
    """
    doc = {"name": _TEST_SENTINEL, "value": 42}
    assert "doc_id" not in doc
    store_custom_documents(_TEST_COLL, [doc])

    assert "doc_id" in doc
    assert isinstance(doc["doc_id"], str)
    # doc_id format is "{name}_{uuid4}"
    assert doc["doc_id"].startswith(f"{_TEST_SENTINEL}_")
    suffix = doc["doc_id"][len(_TEST_SENTINEL) + 1:]
    # UUID4 canonical form is 36 chars
    assert len(suffix) == 36

    # Retrieve by doc_id and verify full document content
    results = get_custom_documents_by_field(
        _TEST_COLL, "doc_id", doc["doc_id"]
    )
    assert len(results) == 1
    stored = results[0]
    assert stored["doc_id"] == doc["doc_id"]
    assert stored["name"] == _TEST_SENTINEL
    assert stored["value"] == 42


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_no_name_raises(cleanup_custom_docs):
    """store_custom_documents raises when the document has no name field."""
    with pytest.raises(ValueError, match="non-blank 'name'"):
        store_custom_documents(_TEST_COLL, [{"value": 1}])


# ---------------------------------------------------------------------------
# Store + retrieve roundtrip
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_and_get_by_field_roundtrip(
        cleanup_custom_docs):
    """store_custom_documents / get_custom_documents_by_field roundtrip.

    Verifies that every user-supplied field is returned correctly and
    the doc_id is preserved exactly.
    """
    preset_id = "roundtrip-doc-id-001"
    doc = {
        "name": _TEST_SENTINEL,
        "doc_id": preset_id,
        "color": "blue",
        "count": 5,
        "tags": ["a", "b"],
    }
    store_custom_documents(_TEST_COLL, [doc])

    results = get_custom_documents_by_field(
        _TEST_COLL, "doc_id", preset_id
    )
    assert len(results) == 1
    stored = results[0]
    assert stored["doc_id"] == preset_id
    assert stored["name"] == _TEST_SENTINEL
    assert stored["color"] == "blue"
    assert stored["count"] == 5
    assert stored["tags"] == ["a", "b"]


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_preserves_caller_doc_id(
        cleanup_custom_docs):
    """A caller-supplied doc_id is preserved and not overwritten.

    Verifies doc_id and all other fields survive the store unchanged.
    """
    preset_id = "preserve-doc-id-12345"
    doc = {
        "name": _TEST_SENTINEL,
        "doc_id": preset_id,
        "v": 1,
        "note": "original",
    }
    store_custom_documents(_TEST_COLL, [doc])

    assert doc["doc_id"] == preset_id
    results = list_custom_documents(_TEST_COLL, "doc_id", preset_id)
    assert len(results) == 1
    stored = results[0]
    assert stored["doc_id"] == preset_id
    assert stored["name"] == _TEST_SENTINEL
    assert stored["v"] == 1
    assert stored["note"] == "original"


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_store_upserts_on_same_doc_id(cleanup_custom_docs):
    """Storing the same doc_id twice upserts; only one record is kept.

    Verifies updated field value and new field are present, original
    doc_id and name are unchanged.
    """
    preset_id = "upsert-test-doc-id"
    doc = {
        "name": _TEST_SENTINEL,
        "doc_id": preset_id,
        "v": 1,
    }
    store_custom_documents(_TEST_COLL, [doc])

    doc["v"] = 2
    doc["extra"] = "added_on_upsert"
    store_custom_documents(_TEST_COLL, [doc])

    results = list_custom_documents(_TEST_COLL, "doc_id", preset_id)
    assert len(results) == 1
    stored = results[0]
    assert stored["v"] == 2
    assert stored["extra"] == "added_on_upsert"
    assert stored["doc_id"] == preset_id
    assert stored["name"] == _TEST_SENTINEL


# ---------------------------------------------------------------------------
# list_custom_documents
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_list_with_field_filter(cleanup_custom_docs):
    """list_custom_documents with field+value returns only matching docs.

    Verifies count, filter correctness, and that distinct seq values
    from the original documents are present in results.
    """
    docs = [
        {"name": _TEST_SENTINEL, "category": "alpha", "seq": 0},
        {"name": _TEST_SENTINEL, "category": "alpha", "seq": 1},
        {"name": _TEST_SENTINEL, "category": "beta", "seq": 2},
    ]
    store_custom_documents(_TEST_COLL, docs)

    alpha_docs = list_custom_documents(_TEST_COLL, "category", "alpha")
    assert len(alpha_docs) == 2
    seqs_found = set()
    for d in alpha_docs:
        assert d["category"] == "alpha"
        assert d["name"] == _TEST_SENTINEL
        seqs_found.add(d["seq"])
    assert seqs_found == {0, 1}


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_list_without_filter_returns_all(
        cleanup_custom_docs):
    """list_custom_documents with no filter returns all stored documents.

    Verifies that every originally stored seq value is present in results.
    """
    docs = [
        {"name": _TEST_SENTINEL, "seq": i} for i in range(3)
    ]
    store_custom_documents(_TEST_COLL, docs)

    all_docs = list_custom_documents(_TEST_COLL)
    test_docs = [
        d for d in all_docs if d.get("name") == _TEST_SENTINEL
    ]
    assert len(test_docs) >= 3
    seqs_found = {d["seq"] for d in test_docs}
    assert {0, 1, 2}.issubset(seqs_found)


# ---------------------------------------------------------------------------
# get_custom_documents_by_field and get_all_custom_documents
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_get_by_field_returns_matching_with_values(
        cleanup_custom_docs):
    """get_custom_documents_by_field returns only matching documents.

    Verifies count, filter correctness, and that distinct payload values
    from the original store call are present in results.
    """
    docs = [
        {"name": _TEST_SENTINEL, "tag": "find_me", "payload": "x1"},
        {"name": _TEST_SENTINEL, "tag": "find_me", "payload": "x2"},
        {"name": _TEST_SENTINEL, "tag": "skip_me", "payload": "x3"},
    ]
    store_custom_documents(_TEST_COLL, docs)

    results = get_custom_documents_by_field(
        _TEST_COLL, "tag", "find_me"
    )
    assert len(results) == 2
    payloads_found = set()
    for r in results:
        assert r["tag"] == "find_me"
        assert r["name"] == _TEST_SENTINEL
        payloads_found.add(r["payload"])
    assert payloads_found == {"x1", "x2"}


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_get_all_returns_all_with_values(
        cleanup_custom_docs):
    """get_all_custom_documents returns all documents with correct values.

    Verifies that every originally stored seq and label pair is present
    in the returned results.
    """
    docs = [
        {"name": _TEST_SENTINEL, "seq": i, "label": f"item_{i}"}
        for i in range(4)
    ]
    store_custom_documents(_TEST_COLL, docs)

    results = get_all_custom_documents(_TEST_COLL)
    test_results = [
        r for r in results if r.get("name") == _TEST_SENTINEL
    ]
    assert len(test_results) >= 4
    seqs_found = {r["seq"] for r in test_results}
    labels_found = {r["label"] for r in test_results}
    assert {0, 1, 2, 3}.issubset(seqs_found)
    assert {
        "item_0", "item_1", "item_2", "item_3"
    }.issubset(labels_found)


# ---------------------------------------------------------------------------
# delete_custom_documents_by_field
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_delete_removes_only_matching_records(
        cleanup_custom_docs):
    """delete_custom_documents_by_field removes only matching records.

    Verifies deleted_count, that targeted docs are gone, and that
    kept docs retain their original field values.
    """
    docs = [
        {
            "name": _TEST_SENTINEL,
            "tag": "to_delete",
            "payload": "gone_1",
        },
        {
            "name": _TEST_SENTINEL,
            "tag": "to_delete",
            "payload": "gone_2",
        },
        {
            "name": _TEST_SENTINEL,
            "tag": "to_keep",
            "payload": "kept_value",
        },
    ]
    store_custom_documents(_TEST_COLL, docs)

    result = delete_custom_documents_by_field(
        _TEST_COLL, "tag", "to_delete"
    )
    assert result.deleted_count == 2

    gone = list_custom_documents(_TEST_COLL, "tag", "to_delete")
    assert len(gone) == 0

    kept = list_custom_documents(_TEST_COLL, "tag", "to_keep")
    assert len(kept) == 1
    assert kept[0]["payload"] == "kept_value"
    assert kept[0]["name"] == _TEST_SENTINEL
    assert kept[0]["tag"] == "to_keep"


# ---------------------------------------------------------------------------
# review_custom_documents
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_custom_document_review_empty_collection_does_not_raise(
        cleanup_custom_docs):
    """review_custom_documents on an empty collection does not raise."""
    delete_custom_documents_by_field(
        _TEST_COLL, "name", _TEST_SENTINEL
    )
    # Should print a summary header and no docs without raising.
    review_custom_documents(_TEST_COLL, "name", _TEST_SENTINEL)
