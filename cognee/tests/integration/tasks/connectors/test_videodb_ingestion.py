"""VideoDB source through the real cognee.add() pipeline, with the SDK faked.

``remember()`` runs this same add() step and then cognify(), which needs an LLM,
so this stops at the Data records: documents are tagged ``videodb`` under the
collection's node set, an unchanged re-sync creates no new Data, a new completed
understanding run replaces just that video, and a removed video is forgotten.
"""

import sys
from pathlib import Path

import pytest

import cognee
from cognee.modules.data.methods import get_authorized_existing_datasets
from cognee.modules.data.methods.get_dataset_data import get_dataset_data
from cognee.modules.users.methods import get_default_user
from cognee.tasks.ingestion.connectors import videodb_source

# The SDK fake is shared with the unit tests.
sys.path.insert(0, str(Path(__file__).parents[3] / "unit" / "tasks" / "connectors"))
from fake_videodb import FakeCollection, FakeConnection, transcript_run

DATASET_NAME = "videodb_integration_test"
TABLE = "videodb_c_1"


async def _videodb_data():
    user = await get_default_user()
    datasets = await get_authorized_existing_datasets(
        user=user, permission_type="write", datasets=[DATASET_NAME]
    )
    if not datasets:
        return []
    return [
        data
        for data in await get_dataset_data(datasets[0].id)
        if isinstance(data.system_metadata, dict)
        and data.system_metadata.get("source") == "videodb"
    ]


async def _sync(collection: FakeCollection):
    source = videodb_source(collection_id="c-1", connection=FakeConnection(collection))
    await cognee.add(
        source,
        dataset_name=DATASET_NAME,
        primary_key="id",
        write_disposition="merge",
        max_rows_per_table=0,
    )
    return source


def _collection() -> FakeCollection:
    collection = FakeCollection()
    collection.add("m-1", "Launch review", transcript_run("und_a", (0, 5, "Ship it Friday.")))
    collection.add("m-2", "Retro", transcript_run("und_b", (0, 5, "The deploy slipped.")))
    return collection


@pytest.mark.asyncio
async def test_sync_is_incremental_and_forgets_removed_videos(clean_environment):
    collection = _collection()
    await _sync(collection)
    first = {d.system_metadata["external_id"]: d for d in await _videodb_data()}
    assert set(first) == {"m-1", "m-2"}
    assert {d.system_metadata["table_name"] for d in first.values()} == {TABLE}
    assert all(d.node_set and "videodb:c-1" in d.node_set for d in first.values())

    again = await _sync(collection)
    assert again.cognee_sync_stats["changed"] == 0
    assert {d.id for d in await _videodb_data()} == {d.id for d in first.values()}

    collection.videos["m-2"].understandings.append(transcript_run("und_c", (5, 9, "Fixed.")))
    del collection.videos["m-1"]
    await _sync(collection)
    after = {d.system_metadata["external_id"]: d.id for d in await _videodb_data()}

    assert set(after) == {"m-2"}
    assert after["m-2"] != first["m-2"].id
