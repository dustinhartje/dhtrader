"""Tests for the StoredImage class in dhtypes.py.

Covers construction, name validation, image_id generation,
to_clean_dict, from_dict round-trip, __eq__/__repr__/__str__,
GridFS storage, retrieval, and deletion.
All test images use "DELETEME" in their name field.
"""
import os
import tempfile
import pytest
from dhtrader import (
    DEFAULT_OBJ_NAME,
    StoredImage,
    delete_images_by_field,
    delete_images_by_image_id,
    get_image_data,
    get_images_metadata_by_field,
    list_images,
    review_images,
    store_image_from_path,
    store_images,
)


# ---------------------------------------------------------------------------
# Test sentinel, minimal JPEG bytes, and cleanup fixture
# ---------------------------------------------------------------------------

# Sentinel value in the name field of all test images.
_TEST_SENTINEL = "DELETEME_STORED_IMAGE_TESTS"

# Minimal valid 1x1 white JPEG (331 bytes).  Embedded inline so tests
# run without requiring a real image file on disk.
_TEST_JPEG = bytes([
    0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01,
    0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43,
    0x00, 0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08, 0x07, 0x07, 0x07, 0x09,
    0x09, 0x08, 0x0A, 0x0C, 0x14, 0x0D, 0x0C, 0x0B, 0x0B, 0x0C, 0x19, 0x12,
    0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E, 0x1D, 0x1A, 0x1C, 0x1C, 0x20,
    0x24, 0x2E, 0x27, 0x20, 0x22, 0x2C, 0x23, 0x1C, 0x1C, 0x28, 0x37, 0x29,
    0x2C, 0x30, 0x31, 0x34, 0x34, 0x34, 0x1F, 0x27, 0x39, 0x3D, 0x38, 0x32,
    0x3C, 0x2E, 0x33, 0x34, 0x32, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01,
    0x00, 0x01, 0x01, 0x01, 0x11, 0x00, 0xFF, 0xC4, 0x00, 0x1F, 0x00, 0x00,
    0x01, 0x05, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    0x09, 0x0A, 0x0B, 0xFF, 0xC4, 0x00, 0xB5, 0x10, 0x00, 0x02, 0x01, 0x03,
    0x03, 0x02, 0x04, 0x03, 0x05, 0x05, 0x04, 0x04, 0x00, 0x00, 0x01, 0x7D,
    0x01, 0x02, 0x03, 0x00, 0x04, 0x11, 0x05, 0x12, 0x21, 0x31, 0x41, 0x06,
    0x13, 0x51, 0x61, 0x07, 0x22, 0x71, 0x14, 0x32, 0x81, 0x91, 0xA1, 0x08,
    0x23, 0x42, 0xB1, 0xC1, 0x15, 0x52, 0xD1, 0xF0, 0x24, 0x33, 0x62, 0x72,
    0x82, 0x09, 0x0A, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x25, 0x26, 0x27, 0x28,
    0x29, 0x2A, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x43, 0x44, 0x45,
    0x46, 0x47, 0x48, 0x49, 0x4A, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59,
    0x5A, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x73, 0x74, 0x75,
    0x76, 0x77, 0x78, 0x79, 0x7A, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89,
    0x8A, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0xA2, 0xA3, 0xA4,
    0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7,
    0xB8, 0xB9, 0xBA, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA,
    0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xE1, 0xE2, 0xE3,
    0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5,
    0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01, 0x00,
    0x00, 0x3F, 0x00, 0xFB, 0x4D, 0xFF, 0xD9,
])


@pytest.fixture
def cleanup_stored_images():
    """Remove all test images from GridFS before and after each test.

    Uses delete_images_by_field directly to ensure cleanup even when
    the functions under test are themselves being exercised.
    """
    def _clean():
        delete_images_by_field(field="name", value=_TEST_SENTINEL)

    _clean()
    yield
    _clean()


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_StoredImage_construction_defaults():
    """Default construction sets expected attribute types and values."""
    img = StoredImage(name="test_DELETEME")
    assert img.name == "test_DELETEME"
    assert isinstance(img.image_id, str)
    assert img.image_id.startswith("test_DELETEME_")
    assert isinstance(img.created_epoch, int)
    assert img.created_epoch > 0
    assert isinstance(img.created_dt, str)
    assert img.content_type == "image/jpeg"
    assert img.filename is None
    assert img.description is None
    assert img.parent_collection is None
    assert img.parent_id_field is None
    assert img.parent_id_value is None
    assert img.tags == []


def test_StoredImage_construction_all_fields():
    """All constructor parameters are stored on the instance."""
    img = StoredImage(
        name="test_DELETEME_full",
        image_id="test_DELETEME_full_99999",
        content_type="image/png",
        filename="chart.png",
        description="A test chart",
        parent_collection="backtests",
        parent_id_field="eval_id",
        parent_id_value="abr_001",
        created_epoch=99999,
        created_dt="2020-01-01 00:00:00",
        tags=["a", "b"],
    )
    assert img.name == "test_DELETEME_full"
    assert img.image_id == "test_DELETEME_full_99999"
    assert img.content_type == "image/png"
    assert img.filename == "chart.png"
    assert img.description == "A test chart"
    assert img.parent_collection == "backtests"
    assert img.parent_id_field == "eval_id"
    assert img.parent_id_value == "abr_001"
    assert img.created_epoch == 99999
    assert img.created_dt == "2020-01-01 00:00:00"
    assert img.tags == ["a", "b"]


def test_StoredImage_image_id_generated_from_name_with_uuid():
    """When image_id is not supplied it is built as name_<uuid4_no_hyphens>."""
    img = StoredImage(name="test_DELETEME_id")
    # image_id must start with the name prefix followed by underscore
    assert img.image_id.startswith("test_DELETEME_id_")
    # The uuid portion must contain no hyphens
    uuid_part = img.image_id[len("test_DELETEME_id_"):]
    assert "-" not in uuid_part
    # It must be 32 hex chars (uuid4 stripped of hyphens)
    assert len(uuid_part) == 32
    # Two images with the same name must not share the same image_id
    img2 = StoredImage(name="test_DELETEME_id")
    assert img.image_id != img2.image_id


def test_StoredImage_image_id_short_is_last_8_chars():
    """image_id_short is name + last 8 chars of uuid portion."""
    img = StoredImage(name="test_DELETEME_short")
    expected = f"test_DELETEME_short_{img.uniq_id[-8:]}"
    assert img.image_id_short == expected
    # uuid suffix portion is 8 chars, plus name + underscore
    assert img.image_id_short.endswith(img.uniq_id[-8:])
    assert img.image_id_short.startswith("test_DELETEME_short_")


def test_StoredImage_image_id_short_in_to_clean_dict():
    """image_id_short appears in to_clean_dict and matches the attribute."""
    img = StoredImage(name="test_DELETEME_short_dict")
    d = img.to_clean_dict()
    assert d["image_id_short"] == img.image_id_short


def test_StoredImage_caller_supplied_image_id_preserved():
    """A caller-supplied image_id is not overwritten."""
    img = StoredImage(
        name="test_DELETEME_preset",
        image_id="custom-id-xyz",
    )
    assert img.image_id == "custom-id-xyz"


def test_StoredImage_created_dt_derived_from_epoch():
    """created_dt is derived from created_epoch when not supplied."""
    # Use a known epoch; just verify it is a non-empty string
    # since the exact formatted value depends on local timezone.
    img = StoredImage(name="test_DELETEME_dt", created_epoch=0)
    assert isinstance(img.created_dt, str)
    assert len(img.created_dt) > 0


def test_StoredImage_tags_defaults_to_empty_list():
    """tags defaults to an empty list when not supplied."""
    img = StoredImage(name="test_DELETEME_tags")
    assert img.tags == []
    assert isinstance(img.tags, list)


def test_StoredImage_tags_is_copied_from_input():
    """tags list is copied, not shared with the caller's list."""
    original_tags = ["x", "y"]
    img = StoredImage(name="test_DELETEME_tagcopy", tags=original_tags)
    original_tags.append("z")
    assert img.tags == ["x", "y"]


# ---------------------------------------------------------------------------
# Name validation
# ---------------------------------------------------------------------------

def test_StoredImage_blank_name_raises():
    """Construction with a blank name raises ValueError."""
    with pytest.raises(ValueError, match="non-blank"):
        StoredImage(name="")


def test_StoredImage_whitespace_only_name_raises():
    """Construction with whitespace-only name raises ValueError."""
    with pytest.raises(ValueError, match="non-blank"):
        StoredImage(name="   ")


def test_StoredImage_none_name_raises():
    """Construction with name=None raises ValueError."""
    with pytest.raises(ValueError, match="non-blank"):
        StoredImage(name=None)


def test_StoredImage_default_name_is_valid():
    """DEFAULT_OBJ_NAME is accepted as a valid name."""
    img = StoredImage(name=DEFAULT_OBJ_NAME)
    assert img.name == DEFAULT_OBJ_NAME


# ---------------------------------------------------------------------------
# to_clean_dict
# ---------------------------------------------------------------------------

def test_StoredImage_to_clean_dict_contains_all_keys():
    """to_clean_dict contains every expected metadata key."""
    img = StoredImage(name="test_DELETEME_dict", created_epoch=55555)
    d = img.to_clean_dict()
    expected_keys = {
        "name", "uniq_id", "image_id", "image_id_short", "content_type",
        "filename", "description", "parent_collection",
        "parent_id_field", "parent_id_value", "created_epoch",
        "created_dt", "tags",
    }
    assert expected_keys == set(d.keys())


def test_StoredImage_to_clean_dict_values_match_attributes():
    """to_clean_dict values match the instance attributes."""
    img = StoredImage(
        name="test_DELETEME_dictvals",
        image_id="test_DELETEME_dictvals_77",
        content_type="image/png",
        filename="foo.png",
        description="desc",
        parent_collection="col",
        parent_id_field="fld",
        parent_id_value="val",
        created_epoch=77,
        created_dt="2000-01-01 00:00:00",
        tags=["t1"],
    )
    d = img.to_clean_dict()
    assert d["name"] == "test_DELETEME_dictvals"
    assert d["image_id"] == "test_DELETEME_dictvals_77"
    assert d["content_type"] == "image/png"
    assert d["filename"] == "foo.png"
    assert d["description"] == "desc"
    assert d["parent_collection"] == "col"
    assert d["parent_id_field"] == "fld"
    assert d["parent_id_value"] == "val"
    assert d["created_epoch"] == 77
    assert d["created_dt"] == "2000-01-01 00:00:00"
    assert d["tags"] == ["t1"]


def test_StoredImage_to_clean_dict_tags_is_a_copy():
    """to_clean_dict tags list is a copy, not the stored list."""
    img = StoredImage(name="test_DELETEME_tagscopy", tags=["a"])
    d = img.to_clean_dict()
    d["tags"].append("b")
    assert img.tags == ["a"]


# ---------------------------------------------------------------------------
# to_json
# ---------------------------------------------------------------------------

def test_StoredImage_to_json_returns_valid_json_string():
    """to_json returns a str that can be parsed by json.loads."""
    import json
    img = StoredImage(
        name="test_DELETEME_tojson",
        description="json test",
        created_epoch=12345,
    )
    result = img.to_json()
    assert isinstance(result, str)
    parsed = json.loads(result)
    assert isinstance(parsed, dict)


def test_StoredImage_to_json_matches_to_clean_dict():
    """Parsed to_json equals to_clean_dict for the same instance."""
    import json
    img = StoredImage(
        name="test_DELETEME_tojson_match",
        description="match test",
        created_epoch=99999,
        tags=["x"],
    )
    assert json.loads(img.to_json()) == img.to_clean_dict()


def test_StoredImage_to_json_is_independent_of_instance():
    """Mutating the parsed to_json dict does not affect the instance."""
    import json
    img = StoredImage(
        name="test_DELETEME_tojson_iso",
        tags=["a"],
    )
    parsed = json.loads(img.to_json())
    parsed["tags"].append("b")
    parsed["description"] = "mutated"
    assert img.tags == ["a"]
    assert img.description is None


# ---------------------------------------------------------------------------
# from_dict round-trip
# ---------------------------------------------------------------------------

def test_StoredImage_from_dict_roundtrip():
    """from_dict(to_clean_dict()) reconstructs an equal instance."""
    # Use a proper uuid-based image_id (the standard format).
    _rt_uuid = "ab" * 16  # 32 hex chars
    original = StoredImage(
        name="test_DELETEME_roundtrip",
        image_id=f"test_DELETEME_roundtrip_{_rt_uuid}",
        uniq_id=_rt_uuid,
        content_type="image/jpeg",
        filename="chart.jpg",
        description="Round-trip test",
        parent_collection="backtests",
        parent_id_field="eval_id",
        parent_id_value="abr_rt_001",
        created_epoch=42,
        created_dt="2099-01-01 00:00:00",
        tags=["rt", "test"],
    )
    rebuilt = StoredImage.from_dict(original.to_clean_dict())
    assert rebuilt == original
    assert rebuilt.name == original.name
    assert rebuilt.image_id == original.image_id
    assert rebuilt.content_type == original.content_type
    assert rebuilt.filename == original.filename
    assert rebuilt.description == original.description
    assert rebuilt.parent_collection == original.parent_collection
    assert rebuilt.parent_id_field == original.parent_id_field
    assert rebuilt.parent_id_value == original.parent_id_value
    assert rebuilt.created_epoch == original.created_epoch
    assert rebuilt.created_dt == original.created_dt
    assert rebuilt.tags == original.tags


def test_StoredImage_from_dict_missing_keys_use_defaults():
    """from_dict with minimal dict uses defaults for missing keys."""
    img = StoredImage.from_dict({"name": "test_DELETEME_minimal"})
    assert img.name == "test_DELETEME_minimal"
    assert img.content_type == "image/jpeg"
    assert img.filename is None
    assert img.description is None
    assert img.tags == []


# ---------------------------------------------------------------------------
# Equality
# ---------------------------------------------------------------------------

def test_StoredImage_equal_when_name_and_image_id_match():
    """Two instances with same name and image_id are equal."""
    a = StoredImage(
        name="test_DELETEME_eq",
        image_id="test_DELETEME_eq_999",
        created_epoch=999,
    )
    b = StoredImage(
        name="test_DELETEME_eq",
        image_id="test_DELETEME_eq_999",
        created_epoch=999,
    )
    assert a == b


def test_StoredImage_not_equal_when_image_id_differs():
    """Two instances with different image_ids are not equal."""
    a = StoredImage(name="test_DELETEME_ne", image_id="id_a")
    b = StoredImage(name="test_DELETEME_ne", image_id="id_b")
    assert a != b


def test_StoredImage_not_equal_to_non_instance():
    """A StoredImage is not equal to a non-StoredImage object."""
    img = StoredImage(name="test_DELETEME_neq")
    assert img != {"name": img.name}
    assert img != "string"
    assert img != 42


# ---------------------------------------------------------------------------
# __repr__ and __str__
# ---------------------------------------------------------------------------

def test_StoredImage_repr_contains_name_and_image_id():
    """repr() includes name and image_id."""
    img = StoredImage(
        name="test_DELETEME_repr",
        image_id="test_DELETEME_repr_1",
    )
    r = repr(img)
    assert "test_DELETEME_repr" in r
    assert "test_DELETEME_repr_1" in r


def test_StoredImage_str_is_string():
    """str() returns a non-empty string."""
    img = StoredImage(name="test_DELETEME_str")
    assert isinstance(str(img), str)
    assert len(str(img)) > 0


# ---------------------------------------------------------------------------
# store_images + get_image_data roundtrip
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_store_and_get_image_data_roundtrip(
        cleanup_stored_images):
    """store_images then get_image_data returns the original bytes.

    Verifies that the image_id is returned, and that retrieving bytes
    by image_id returns the exact bytes that were stored.
    """
    img = StoredImage(
        name=_TEST_SENTINEL,
        content_type="image/jpeg",
        description="roundtrip test",
    )
    image_ids = store_images([img], [_TEST_JPEG])
    assert len(image_ids) == 1
    assert image_ids[0] == img.image_id

    retrieved = get_image_data(img.image_id)
    assert retrieved == _TEST_JPEG


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_load_data_returns_stored_bytes(
        cleanup_stored_images):
    """load_data() on a stored StoredImage returns the original bytes.

    Verifies that the load_data() convenience method on the class
    retrieves the same bytes that were stored.
    """
    img = StoredImage(
        name=_TEST_SENTINEL,
        description="load_data test",
    )
    store_images([img], [_TEST_JPEG])

    loaded = img.load_data()
    assert loaded == _TEST_JPEG


# ---------------------------------------------------------------------------
# get_images_metadata_by_field
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_get_images_metadata_by_field_returns_metadata_objects(
        cleanup_stored_images):
    """get_images_metadata_by_field returns StoredImage metadata objects.

    Stores two images sharing the same parent_collection and one with a
    different one; verifies that only the two matching images are returned
    with the correct field values.
    """
    img_a = StoredImage(
        name=_TEST_SENTINEL,
        content_type="image/jpeg",
        parent_collection="test_parent",
        description="image_a",
    )
    img_b = StoredImage(
        name=_TEST_SENTINEL,
        content_type="image/jpeg",
        parent_collection="test_parent",
        description="image_b",
    )
    img_c = StoredImage(
        name=_TEST_SENTINEL,
        content_type="image/jpeg",
        parent_collection="other_parent",
        description="image_c",
    )
    store_images([img_a, img_b, img_c], [_TEST_JPEG] * 3)

    results = get_images_metadata_by_field("parent_collection", "test_parent")
    assert len(results) == 2
    descriptions = {r.description for r in results}
    assert descriptions == {"image_a", "image_b"}
    for r in results:
        assert isinstance(r, StoredImage)
        assert r.name == _TEST_SENTINEL
        assert r.parent_collection == "test_parent"
        assert r.content_type == "image/jpeg"


# ---------------------------------------------------------------------------
# list_images
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_list_images_with_filter(cleanup_stored_images):
    """list_images with field/value returns only matching images.

    Verifies count, field values, and that descriptions from the stored
    images appear in the results.
    """
    img_x = StoredImage(
        name=_TEST_SENTINEL,
        description="list_x",
        tags=["x"],
    )
    img_y = StoredImage(
        name=_TEST_SENTINEL,
        description="list_y",
        tags=["y"],
    )
    store_images([img_x, img_y], [_TEST_JPEG] * 2)

    results = list_images(field="name", value=_TEST_SENTINEL)
    assert len(results) >= 2
    descriptions = {r.description for r in results}
    assert "list_x" in descriptions
    assert "list_y" in descriptions
    for r in results:
        assert isinstance(r, StoredImage)


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_list_images_without_filter_returns_all(
        cleanup_stored_images):
    """list_images with no filter returns at least all test images."""
    descriptions = [f"no_filter_{i}" for i in range(3)]
    imgs = [
        StoredImage(name=_TEST_SENTINEL, description=d)
        for d in descriptions
    ]
    store_images(imgs, [_TEST_JPEG] * 3)

    all_images = list_images()
    test_images = [
        r for r in all_images if r.name == _TEST_SENTINEL
    ]
    assert len(test_images) >= 3
    found_descriptions = {r.description for r in test_images}
    assert set(descriptions).issubset(found_descriptions)


# ---------------------------------------------------------------------------
# delete_images_by_field
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_delete_by_field_removes_only_matching(
        cleanup_stored_images):
    """delete_images_by_field removes targeted images and preserves others.

    Verifies deleted count, that targeted images are gone, and that
    the kept image retains its original description.
    """
    img_del = StoredImage(
        name=_TEST_SENTINEL,
        description="to_delete",
        parent_collection=f"{_TEST_SENTINEL}_del_target",
    )
    img_keep = StoredImage(
        name=_TEST_SENTINEL,
        description="to_keep",
        parent_collection=f"{_TEST_SENTINEL}_keep_target",
    )
    store_images([img_del, img_keep], [_TEST_JPEG] * 2)

    deleted_count = delete_images_by_field(
        field="parent_collection",
        value=f"{_TEST_SENTINEL}_del_target",
    )
    assert deleted_count == 1

    gone = list_images(
        field="parent_collection",
        value=f"{_TEST_SENTINEL}_del_target",
    )
    assert len(gone) == 0

    kept = list_images(
        field="parent_collection",
        value=f"{_TEST_SENTINEL}_keep_target",
    )
    assert len(kept) == 1
    assert kept[0].description == "to_keep"
    assert kept[0].name == _TEST_SENTINEL


# ---------------------------------------------------------------------------
# delete_images_by_image_id
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_delete_by_image_id_removes_targeted(
        cleanup_stored_images):
    """delete_images_by_image_id removes the specified images.

    Verifies deleted count, that targeted images raise on retrieval,
    and that un-targeted images are still retrievable.
    """
    img_a = StoredImage(
        name=_TEST_SENTINEL, description="del_by_id_a", created_epoch=10001
    )
    img_b = StoredImage(
        name=_TEST_SENTINEL, description="del_by_id_b", created_epoch=10002
    )
    img_c = StoredImage(
        name=_TEST_SENTINEL, description="del_by_id_c", created_epoch=10003
    )
    store_images([img_a, img_b, img_c], [_TEST_JPEG] * 3)

    deleted = delete_images_by_image_id([img_a.image_id, img_b.image_id])
    assert deleted == 2

    with pytest.raises(KeyError):
        get_image_data(img_a.image_id)
    with pytest.raises(KeyError):
        get_image_data(img_b.image_id)

    # img_c should still be retrievable.
    data_c = get_image_data(img_c.image_id)
    assert data_c == _TEST_JPEG


# ---------------------------------------------------------------------------
# store_images validation
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_store_empty_images_raises():
    """store_images raises ValueError when images list is empty."""
    with pytest.raises(ValueError, match="non-empty"):
        store_images([], [])


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_store_mismatched_lengths_raises():
    """store_images raises ValueError when list lengths do not match."""
    img = StoredImage(name=_TEST_SENTINEL)
    with pytest.raises(ValueError, match="same length"):
        store_images([img], [])


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_duplicate_image_id_rejected(
        cleanup_stored_images):
    """Storing an image with a duplicate image_id is rejected.

    Builds a second StoredImage with the same image_id as the first
    and asserts that the second store call raises an error, confirming
    that the unique index on metadata.image_id is enforced.
    """
    from gridfs.errors import FileExists
    img_first = StoredImage(
        name=_TEST_SENTINEL,
        description="original",
    )
    store_images([img_first], [_TEST_JPEG])
    # Force the same image_id onto a new object to simulate a collision.
    img_dup = StoredImage(
        name=_TEST_SENTINEL,
        image_id=img_first.image_id,
        description="duplicate",
    )
    with pytest.raises(FileExists):
        store_images([img_dup], [_TEST_JPEG])


# ---------------------------------------------------------------------------
# store_image_from_path
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_store_image_from_path_roundtrip(
        cleanup_stored_images):
    """store_image_from_path stores bytes and returns a valid image_id.

    Writes a temp JPEG file, stores it, verifies the image_id uses the
    filename stem as name, and that bytes retrieved match the written file.
    """
    with tempfile.NamedTemporaryFile(
        suffix=".jpg",
        prefix=f"{_TEST_SENTINEL}_",
        delete=False,
    ) as f:
        tmp_path = f.name
        f.write(_TEST_JPEG)

    try:
        stem = os.path.splitext(os.path.basename(tmp_path))[0]
        image_id = store_image_from_path(
            filepath=tmp_path,
            description="from_path test",
        )
        assert isinstance(image_id, str)
        assert image_id.startswith(f"{stem}_")

        retrieved = get_image_data(image_id)
        assert retrieved == _TEST_JPEG

        # Clean up the stored image by image_id.
        delete_images_by_image_id([image_id])
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# load_data raises after deletion
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_load_data_raises_after_deletion(
        cleanup_stored_images):
    """load_data() raises KeyError after the stored image is deleted."""
    img = StoredImage(
        name=_TEST_SENTINEL,
        description="deleted_then_load",
    )
    store_images([img], [_TEST_JPEG])
    delete_images_by_image_id([img.image_id])

    with pytest.raises(KeyError):
        img.load_data()


# ---------------------------------------------------------------------------
# review_images
# ---------------------------------------------------------------------------

@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_review_images_empty_does_not_raise(
        cleanup_stored_images):
    """review_images on a filtered empty result does not raise."""
    # Ensure no test images are present.
    delete_images_by_field(field="name", value=_TEST_SENTINEL)
    # Should print a header and no images without raising.
    review_images(field="name", value=_TEST_SENTINEL)


@pytest.mark.storage
@pytest.mark.suppress_stdout
def test_StoredImage_review_images_with_data_does_not_raise(
        cleanup_stored_images):
    """review_images with stored images prints and does not raise."""
    img = StoredImage(
        name=_TEST_SENTINEL,
        description="review_test",
    )
    store_images([img], [_TEST_JPEG])
    review_images(field="name", value=_TEST_SENTINEL)
