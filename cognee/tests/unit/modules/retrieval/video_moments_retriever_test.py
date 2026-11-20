import sys
from types import SimpleNamespace

import pytest

from cognee.modules.retrieval.video_moments_retriever import (
    VideoMomentsRetriever,
    _collection_id,
    parse_moment_lines,
)


def test_parses_only_timestamped_lines():
    text = "## Scenes\n[00:00:05-00:00:09] A demo.\nnot a moment\n[01:02:03-01:02:10] Later."
    assert parse_moment_lines(text) == [(5, 9, "A demo."), (3723, 3730, "Later.")]


def test_collection_id_reads_the_videodb_node_set():
    assert _collection_id(["other", "videodb:c-1"]) == "c-1"
    assert _collection_id('["videodb:c-2"]') == "c-2"
    assert _collection_id(None) is None


@pytest.mark.asyncio
async def test_completion_returns_moments_and_context_names_the_video():
    retriever = VideoMomentsRetriever()
    moments = [{"video_id": "m-1", "title": "Intro", "start": 5, "end": 9, "text": "Hi."}]

    assert await retriever.get_completion_from_context("q", moments, None) == moments
    assert await retriever.get_context_from_objects("q", moments) == "Intro (m-1) 5s-9s:\nHi."


class FakeVideodbError(Exception):
    pass


def _fake_videodb(monkeypatch, fail_for=()):
    calls = []

    class Video:
        def __init__(self, video_id):
            self.id = video_id

        def generate_stream(self, timeline):
            if self.id in fail_for:
                raise FakeVideodbError("not found")
            calls.append((self.id, timeline))
            return f"https://stream.videodb.io/{self.id}.m3u8"

    collection = SimpleNamespace(get_video=Video)
    connection = SimpleNamespace(get_collection=lambda collection_id: collection)
    monkeypatch.setitem(sys.modules, "videodb", SimpleNamespace(connect=lambda api_key: connection))
    monkeypatch.setitem(
        sys.modules, "videodb.exceptions", SimpleNamespace(VideodbError=FakeVideodbError)
    )
    monkeypatch.setenv("VIDEO_DB_API_KEY", "vdb-test-key")
    return calls


def test_streams_are_added_and_one_failure_keeps_the_rest(monkeypatch):
    calls = _fake_videodb(monkeypatch, fail_for={"m-2"})
    moments = [
        {"video_id": "m-1", "collection_id": "c-1", "start": 5, "end": 9},
        {"video_id": "m-2", "collection_id": None, "start": 1, "end": 2},
    ]

    VideoMomentsRetriever._add_streams(moments)

    assert calls == [("m-1", [(5, 9)])]
    assert moments[0]["stream_url"] == "https://stream.videodb.io/m-1.m3u8"
    assert "stream_url" not in moments[1]


def test_streams_are_skipped_without_a_key(monkeypatch):
    calls = _fake_videodb(monkeypatch)
    monkeypatch.delenv("VIDEO_DB_API_KEY")
    moments = [{"video_id": "m-1", "collection_id": "c-1", "start": 5, "end": 9}]

    VideoMomentsRetriever._add_streams(moments)

    assert calls == []
    assert "stream_url" not in moments[0]
