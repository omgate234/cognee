"""VIDEO_MOMENTS over documents the VideoDB connector stored for real.

The Data rows come from the connector through cognee.add(); only the vector
search is mocked, because filling DocumentChunk_text needs cognify and an LLM.
Checks that hits map back to their video and collection, that chunks of other
documents are dropped, and that each moment spans its timestamped lines.
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import cognee
from cognee.modules.data.methods import get_authorized_existing_datasets
from cognee.modules.data.methods.get_dataset_data import get_dataset_data
from cognee.modules.retrieval.video_moments_retriever import VideoMomentsRetriever
from cognee.modules.users.methods import get_default_user
from cognee.tasks.ingestion.connectors import videodb_source

sys.path.insert(0, str(Path(__file__).parents[3] / "unit" / "tasks" / "connectors"))
from fake_videodb import FakeCollection, FakeConnection, transcript_run

DATASET_NAME = "videodb_moments_test"


def _hit(document_id, text, score=0.1):
    return SimpleNamespace(
        id="chunk-" + text[:6], score=score, payload={"document_id": document_id, "text": text}
    )


@pytest.mark.asyncio
async def test_hits_map_back_to_video_moments(clean_environment):
    collection = FakeCollection()
    collection.add("m-1", "Launch review", transcript_run("und_a", (60, 65, "Ship it Friday.")))
    await cognee.add(
        videodb_source(collection_id="c-1", connection=FakeConnection(collection)),
        dataset_name=DATASET_NAME,
        primary_key="id",
        write_disposition="merge",
        max_rows_per_table=0,
    )
    await cognee.add("A plain note about Friday.", dataset_name=DATASET_NAME)

    user = await get_default_user()
    [dataset] = await get_authorized_existing_datasets(
        user=user, permission_type="read", datasets=[DATASET_NAME]
    )
    data = await get_dataset_data(dataset.id)
    video_doc = next(d for d in data if (d.system_metadata or {}).get("source") == "videodb")
    note_doc = next(d for d in data if d.id != video_doc.id)

    vector = AsyncMock()
    vector.search = AsyncMock(
        return_value=[
            _hit(
                str(video_doc.id),
                "## Transcript\n[00:01:00-00:01:05] Ship it Friday.\n"
                "[00:01:05-00:01:12] Then the retro.",
            ),
            _hit(str(note_doc.id), "A plain note about Friday."),
            _hit(str(video_doc.id), "# Launch review\n\nVideoDB video m-1\nLength: 00:01:35"),
        ]
    )
    with patch(
        "cognee.modules.retrieval.chunks_retriever.get_unified_engine",
        return_value=SimpleNamespace(vector=vector, graph=AsyncMock()),
    ):
        moments = await VideoMomentsRetriever(top_k=5).get_retrieved_objects("when do we ship?")

    assert len(moments) == 1
    [moment] = moments
    assert moment["video_id"] == "m-1"
    assert moment["collection_id"] == "c-1"
    assert moment["title"] == "Launch review"
    assert moment["player_url"] == "https://console.videodb.io/player?v=m-1"
    assert (moment["start"], moment["end"]) == (60, 72)
    assert moment["text"] == "Ship it Friday.\nThen the retro."
    assert "stream_url" not in moment
