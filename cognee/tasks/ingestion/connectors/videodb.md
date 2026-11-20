# VideoDB connector

A [VideoDB](https://videodb.io) data-source connector for cognee: turn a collection of
videos into memory, with every line timestamped so answers point back to the moment
they came from.

It exposes a `dlt` source you hand straight to `cognee.remember(...)`. One source is one
VideoDB collection. The sync is incremental and forgets videos that leave the collection.

## Install

```bash
pip install "cognee[videodb]"
export VIDEO_DB_API_KEY=...
```

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import videodb_source

await cognee.remember(
    videodb_source(collection_id="c-...", understand=True),
    dataset_name="videos",
    primary_key="id",
    write_disposition="merge",
    max_rows_per_table=0,
)

await cognee.recall("When did we decide to delay the launch?", datasets=["videos"])
```

Run it again with the same dataset to sync only what changed.

## What is ingested

One document per video, from its understanding runs whose status is `done`:

| Field | Value |
| --- | --- |
| `id` | the video id (`m-...`) |
| `title` | the video name |
| `url` | the video's player URL |
| `content` | one section per successful analyzer, one `[HH:MM:SS-HH:MM:SS]` line per scene |
| node set | `videodb:<collection id>` |

Every analyzer type is rendered: `spoken_words` (transcript), `vlm` (scene descriptions,
or the fields of its `schema`), `ocr`, `brand_detection`, `activity_recognition`,
`location_detection` and `object_detection` (the distinct labels per scene).

- A video with no completed understanding run is skipped until one completes.
- A video is rendered again when its name changes or a new run completes.
- A video removed from the collection, or whose completed runs were all deleted, is
  forgotten.
- A failed API call aborts the sync before anything is forgotten. The next run
  starts from the last good state.

## Finding moments

`SearchType.VIDEO_MOMENTS` answers with where in a video something happens. It runs the
same vector search as `CHUNKS`, keeps the chunks of VideoDB documents and returns one
moment per chunk:

```python
from cognee import SearchType

moments = await cognee.search(
    "when did we decide to delay the launch?",
    query_type=SearchType.VIDEO_MOMENTS,
    datasets=["videos"],
    retriever_specific_config={"stream_clips": True},
)
# each moment: {"video_id": "m-...", "collection_id": "c-...", "title": ..., "start": 312,
#   "end": 341, "text": ..., "player_url": ..., "stream_url": ..., "score": ...}
```

`start` and `end` are seconds, the span of the chunk's timestamped lines. With
`stream_clips` each moment also gets a VideoDB stream URL that plays just that span (one
API call per moment, needs `VIDEO_DB_API_KEY`); a clip that cannot be streamed keeps its
other fields. The search itself calls no LLM.

## Understanding runs

`understand=True` starts a `spoken_words` + `vlm` run for each video that has never had
one. Pass a list of analyzer dicts to start those instead:

```python
videodb_source(
    collection_id="c-...",
    understand=[{"type": "spoken_words"}, {"type": "ocr"}],
)
```

The sync never waits for a run. The video is picked up by the first sync after its run
completes. Runs are billed to the VideoDB account, so `understand` is off by default, and
a video whose run failed is not retried.

## Options

| Argument | Default | |
| --- | --- | --- |
| `collection_id` | `default` | the collection to sync |
| `api_key` | `VIDEO_DB_API_KEY` | |
| `base_url` | the SDK's | |
| `resource_name` | `videodb_<collection id>` | staging table and state key; give each source in a dataset its own |
| `understand` | `False` | see above |
| `check_active` | `None` | host authorization checkpoint during extraction |
| `connection` | `None` | pre-built `videodb` connection |

`source.cognee_sync_stats` holds counts from the last run: `scanned`, `changed`,
`unchanged`, `not_understood`, `started` and `deleted`.
