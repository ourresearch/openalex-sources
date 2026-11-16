import io
import json

from jobs.apply_source_curations import NOTE_PREFIX, fetch_curations, latest_by_source, plan

S = "https://openalex.org/S"


def cur(cid, sid, prop, value, created, action="replace", entity="sources"):
    return {"id": cid, "entity": entity, "entity_id": f"{S}{sid}", "property": prop, "action": action,
            "value": value, "created": created}


def test_latest_per_source_and_field_wins():
    rows = [cur("cur-a", 7, "is_oa", "false", "2026-11-16T10:00"),
            cur("cur-b", 7, "is_oa", "true", "2026-11-16T11:00"),
            cur("cur-c", 7, "oa_flip_year", "2019", "2026-11-16T09:00"),
            cur("cur-d", 8, "is_oa", "true", "2026-11-16T09:00")]
    assert latest_by_source(rows) == {7: {"is_oa": (True, "cur-b"), "oa_flip_year": (2019, "cur-c")},
                                      8: {"is_oa": (True, "cur-d")}}


def test_same_created_breaks_ties_by_id():
    rows = [cur("cur-b", 7, "is_oa", "true", "2026-11-16T10:00"),
            cur("cur-a", 7, "is_oa", "false", "2026-11-16T10:00")]
    assert latest_by_source(rows) == {7: {"is_oa": (True, "cur-b")}}


def test_other_shapes_are_left_out():
    rows = [cur("cur-a", 7, "apc_usd", "0", "2026-11-16"),
            cur("cur-b", 7, "is_oa", "true", "2026-11-16", action="remove"),
            cur("cur-c", 7, "is_oa", "true", "2026-11-16", entity="works"),
            {**cur("cur-d", 7, "is_oa", "true", "2026-11-16"), "entity_id": "https://openalex.org/W7"}]
    assert latest_by_source(rows) == {}


def test_new_row_from_a_curation():
    [(sid, before, after)] = plan({7: {"is_oa": (True, "cur-b"), "oa_flip_year": (2019, "cur-c")}}, {})
    assert (sid, before) == (7, None)
    assert after == {"curated_is_oa": True, "curated_flip_year": 2019, "note": NOTE_PREFIX + "cur-b, cur-c"}


def test_an_uncurated_column_keeps_the_hand_written_value_and_note():
    existing = {7: {"curated_is_oa": True, "curated_flip_year": None, "note": "ZD 9101 #1239: verified"}}
    [(_sid, before, after)] = plan({7: {"oa_flip_year": (2021, "cur-c")}}, existing)
    assert before == existing[7]
    assert after == {"curated_is_oa": True, "curated_flip_year": 2021,
                     "note": NOTE_PREFIX + "cur-c | was: ZD 9101 #1239: verified"}


def test_rerun_writes_nothing():
    desired = {7: {"is_oa": (False, "cur-a")}}
    existing = {7: {"curated_is_oa": True, "curated_flip_year": None, "note": "hand"}}
    [(_sid, _before, after)] = plan(desired, existing)
    assert plan(desired, {7: after}) == []


def test_a_later_curation_keeps_the_first_hand_note():
    existing = {7: {"curated_is_oa": True, "curated_flip_year": None, "note": NOTE_PREFIX + "cur-a | was: hand"}}
    [(_sid, _before, after)] = plan({7: {"is_oa": (False, "cur-z")}}, existing)
    assert after["note"] == NOTE_PREFIX + "cur-z | was: hand" and after["curated_is_oa"] is False


def test_fetch_reads_every_page():
    pages = {1: [cur(f"cur-{i}", i, "is_oa", "true", "t") for i in range(100)],
             2: [cur("cur-x", 1, "is_oa", "true", "t")]}
    seen = []

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def opener(req, timeout=None):
        page = int(req.full_url.rsplit("page=", 1)[1])
        seen.append((req.full_url, req.get_header("Authorization")))
        return _Resp(json.dumps({"results": pages[page]}).encode())

    rows = fetch_curations("KEY", base="https://u.example", opener=opener)
    assert len(rows) == 101
    assert seen[0] == ("https://u.example/curations?entity=sources&per_page=100&page=1", "Bearer KEY")
