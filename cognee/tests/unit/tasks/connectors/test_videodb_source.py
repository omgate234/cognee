import pytest
from fake_videodb import (
    FakeAnalyzer,
    FakeCollection,
    FakeConnection,
    FakeUnderstanding,
    transcript_run,
)

from cognee.tasks.ingestion.connectors.videodb import (
    _iter_rows,
    render_analyzer_output,
    videodb_source,
)
from cognee.tasks.ingestion.dlt_utils import NODE_SET_COLUMN


def _sync(collection, state, **kwargs):
    stats: dict[str, int] = {}
    rows = list(_iter_rows(collection, "c-1", state, stats, **kwargs))
    return rows, stats


def test_renders_completed_runs_as_one_timestamped_document():
    collection = FakeCollection()
    scenes = FakeUnderstanding(
        "und_b",
        "done",
        [FakeAnalyzer("scene", "vlm"), FakeAnalyzer("brands", "brand_detection")],
        {
            "scene": {"scenes": [{"start": 0, "end": 10, "data": {"text": "A whiteboard demo."}}]},
            "brands": {
                "scenes": [
                    {
                        "start": 10,
                        "end": 20,
                        "data": {"brand_names": ["Acme"], "mentions": [{"brand_name": "Acme"}]},
                    }
                ]
            },
        },
    )
    collection.add(
        "m-1", "Launch review", transcript_run("und_a", (65, 70, "Ship it Friday.")), scenes
    )

    rows, stats = _sync(collection, {})

    assert stats["changed"] == 1
    [row] = rows
    assert row["id"] == "m-1"
    assert row["title"] == "Launch review"
    assert row["url"] == "https://console.videodb.io/player?v=m-1"
    assert row[NODE_SET_COLUMN] == ["videodb:c-1"]
    assert row["content"] == (
        "VideoDB video m-1\nLength: 00:01:35\n\n"
        "## Transcript\n[00:01:05-00:01:10] Ship it Friday.\n\n"
        "## Brands\n[00:00:10-00:00:20] brand names: Acme\n\n"
        "## Scenes\n[00:00:00-00:00:10] A whiteboard demo."
    )


def test_unchanged_videos_are_not_rendered_again():
    collection = FakeCollection()
    run = transcript_run("und_a", (0, 5, "Hello."))
    collection.add("m-1", "Intro", run)
    state: dict = {}

    _sync(collection, state)
    rows, stats = _sync(collection, state)

    assert rows == []
    assert stats["unchanged"] == 1
    assert run.output_calls == 1


def test_a_new_completed_run_or_a_rename_re_renders_the_video():
    collection = FakeCollection()
    video = collection.add("m-1", "Intro", transcript_run("und_a", (0, 5, "Hello.")))
    state: dict = {}
    _sync(collection, state)

    video.understandings.append(transcript_run("und_c", (5, 9, "Second pass.")))
    rows, _ = _sync(collection, state)
    assert [row["id"] for row in rows] == ["m-1"]
    assert "Second pass." in rows[0]["content"]

    video.name = "Intro (final)"
    rows, _ = _sync(collection, state)
    assert rows[0]["title"] == "Intro (final)"


def test_videos_without_a_completed_run_are_skipped_until_one_finishes():
    collection = FakeCollection()
    pending = FakeUnderstanding("und_p", "running", [FakeAnalyzer("transcript", "spoken_words")])
    video = collection.add("m-1", "Raw upload", pending)
    state: dict = {}

    rows, stats = _sync(collection, state)
    assert rows == []
    assert stats["not_understood"] == 1

    video.understandings = [transcript_run("und_p", (0, 3, "Done now."))]
    rows, _ = _sync(collection, state)
    assert [row["id"] for row in rows] == ["m-1"]


def test_failed_analyzers_are_left_out():
    collection = FakeCollection()
    run = transcript_run("und_a", (0, 5, "Hello."))
    run.analyzers.append(FakeAnalyzer("ocr", "ocr", status="failed"))
    collection.add("m-1", "Intro", run)

    rows, _ = _sync(collection, {})

    assert "On-screen text" not in rows[0]["content"]


def test_removed_videos_are_tombstoned():
    collection = FakeCollection()
    collection.add("m-1", "Keep", transcript_run("und_a", (0, 5, "One.")))
    collection.add("m-2", "Drop", transcript_run("und_b", (0, 5, "Two.")))
    state: dict = {}
    _sync(collection, state)

    del collection.videos["m-2"]
    rows, stats = _sync(collection, state)

    assert rows == [{"id": "m-2", "_deleted": True}]
    assert stats["deleted"] == 1
    assert set(state["videos"]) == {"m-1"}


def test_a_failed_listing_aborts_before_any_tombstone():
    collection = FakeCollection()
    collection.add("m-1", "Keep", transcript_run("und_a", (0, 5, "One.")))
    state: dict = {}
    _sync(collection, state)
    before = dict(state["videos"])

    collection.fail_listing = True
    with pytest.raises(RuntimeError):
        _sync(collection, state)
    assert state["videos"] == before


def test_understand_starts_runs_only_for_videos_that_never_had_one():
    collection = FakeCollection()
    fresh = collection.add("m-1", "Fresh")
    failed = collection.add("m-2", "Failed", FakeUnderstanding("und_f", "failed", []))
    analyzers = [{"type": "spoken_words"}]

    rows, stats = _sync(collection, {}, understand=analyzers)

    assert rows == []
    assert stats["started"] == 1
    assert fresh.started == [analyzers]
    assert failed.started == []

    # The run it started is still pending, so the next sync does not start another.
    _sync(collection, {}, understand=analyzers)
    assert len(fresh.started) == 1


def test_object_detection_renders_distinct_labels():
    section = render_analyzer_output(
        "object_detection",
        "objects",
        {
            "scenes": [
                {
                    "start": 0,
                    "end": 2,
                    "data": {
                        "frames": [
                            {"timestamp": 0.5, "detections": [{"label": "person"}]},
                            {
                                "timestamp": 1.5,
                                "detections": [{"label": "car"}, {"label": "person"}],
                            },
                        ]
                    },
                }
            ]
        },
    )
    assert section == "## Objects\n[00:00:00-00:00:02] car, person"


def test_structured_vlm_fields_are_rendered_as_key_value_pairs():
    section = render_analyzer_output(
        "vlm",
        "kitchen",
        [{"start": 0, "end": 4, "data": {"activity": "cooking", "people_count": 2}}],
    )
    assert section == "## Scenes (kitchen)\n[00:00:00-00:00:04] activity: cooking; people count: 2"


def test_source_is_tagged_and_keeps_counts_only():
    pytest.importorskip("dlt")
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

    connection = FakeConnection(FakeCollection())
    source = videodb_source(collection_id="c-ABC 1", connection=connection)

    assert getattr(source, DOCUMENT_SOURCE_ATTR) == "videodb"
    assert source.name == "videodb_c_abc_1"
    assert source.cognee_sync_stats == {}


def test_missing_api_key_is_rejected(monkeypatch):
    pytest.importorskip("dlt")
    pytest.importorskip("videodb")
    monkeypatch.delenv("VIDEO_DB_API_KEY", raising=False)

    with pytest.raises(ValueError, match="VIDEO_DB_API_KEY"):
        videodb_source(collection_id="c-1")
