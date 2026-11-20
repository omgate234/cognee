"""VideoDB connector for cognee: a ``dlt`` source that turns a VideoDB collection into memory.

One source is one VideoDB collection. Each video becomes one document built from
its completed understanding runs (transcript, scene descriptions, on-screen text,
brands, activity, location, objects), with every line timestamped so answers can
point back to the moment in the video::

    import cognee
    from cognee.tasks.ingestion.connectors import videodb_source

    await cognee.remember(
        videodb_source(collection_id="c-...", api_key="<VideoDB API key>"),
        dataset_name="videos",
        primary_key="id",
        write_disposition="merge",
        max_rows_per_table=0,
    )

Design
------
* **Rows** are ``{id, title, content, url, _deleted, cognee_node_set}``. ``id`` is the
  VideoDB video id (``m-...``) and ``url`` its player URL. The node set is
  ``videodb:<collection id>``.
* **Content** comes from the video's understanding runs whose status is ``done``:
  the output of every analyzer that succeeded, one section per analyzer, one
  ``[HH:MM:SS-HH:MM:SS]`` line per scene. A video with no completed run is skipped
  until one completes.
* **Incremental**: the collection is listed on every run (VideoDB has no change
  feed). A video is rendered again only when its name or its set of completed runs
  changed. Videos that left the collection, or lost all their completed runs, are
  emitted as ``_deleted`` tombstones. Any API error aborts the run before a
  tombstone is emitted, so a partial listing never looks like a mass deletion.
* **understand=True** starts an understanding run (``spoken_words`` + ``vlm``, or the
  analyzers you pass) for each video that has no run at all, and skips it this time.
  It never waits: the next sync picks the video up once the run is done. Runs are
  billed to the VideoDB account, so this is off by default.

Privacy
-------
Nothing is fetched until you construct a source and call ``remember``. Anyone with
read access to the target dataset can read the transcripts and descriptions that
were ingested. The API key lives only in the connection object.
"""

import os
import re
from collections.abc import Callable, Iterator
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR, NODE_SET_COLUMN

logger = get_logger("videodb_connector")

DEFAULT_ANALYZERS: list[dict[str, Any]] = [{"type": "spoken_words"}, {"type": "vlm"}]

_SECTION_TITLES = {
    "spoken_words": "Transcript",
    "vlm": "Scenes",
    "ocr": "On-screen text",
    "brand_detection": "Brands",
    "activity_recognition": "Activity",
    "location_detection": "Location",
    "object_detection": "Objects",
}
# The names VideoDB gives analyzers by default; a custom name is shown in the heading.
_DEFAULT_NAMES = {
    "spoken_words": "transcript",
    "vlm": "scene",
    "ocr": "ocr",
    "brand_detection": "brands",
    "activity_recognition": "activity",
    "location_detection": "location",
    "object_detection": "objects",
}
# Analyzer output keys that are bookkeeping, not content.
_SKIPPED_KEYS = {"mentions", "frames", "metadata"}


def _timestamp(seconds: Any) -> str:
    total = int(float(seconds or 0))
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if item not in (None, ""))
    if isinstance(value, dict):
        return ", ".join(f"{k}: {_value(v)}" for k, v in value.items() if v not in (None, ""))
    return str(value).strip()


def _render_scene_data(data: Any) -> str:
    """One line of text for a scene's analyzer output, whatever the analyzer type."""
    if not isinstance(data, dict):
        return _value(data)
    parts = []
    text = _value(data.get("text") or "")
    if text:
        parts.append(text)
    labels = sorted(
        {
            str(detection.get("label"))
            for frame in data.get("frames") or []
            for detection in frame.get("detections") or []
            if detection.get("label")
        }
    )
    if labels:
        parts.append(", ".join(labels))
    for key in sorted(data):
        if key == "text" or key in _SKIPPED_KEYS:
            continue
        rendered = _value(data[key])
        if rendered:
            parts.append(f"{key.replace('_', ' ')}: {rendered}")
    return "; ".join(parts)


def render_analyzer_output(analyzer_type: str, name: str, output: Any) -> str:
    """Render one analyzer's output as a titled section of timestamped lines."""
    scenes = output.get("scenes") if isinstance(output, dict) else output
    lines = []
    for scene in scenes or []:
        if not isinstance(scene, dict):
            continue
        line = _render_scene_data(scene.get("data"))
        if not line:
            continue
        lines.append(f"[{_timestamp(scene.get('start'))}-{_timestamp(scene.get('end'))}] {line}")
    if not lines:
        return ""
    title = _SECTION_TITLES.get(analyzer_type, analyzer_type or "Analysis")
    default = name in ("", analyzer_type, _DEFAULT_NAMES.get(analyzer_type))
    heading = title if default else f"{title} ({name})"
    return "\n".join([f"## {heading}", *lines])


def render_video(video: Any, sections: list[str]) -> dict[str, Any]:
    title = str(getattr(video, "name", None) or video.id)
    header = [f"VideoDB video {video.id}", f"Length: {_timestamp(getattr(video, 'length', 0))}"]
    return {
        "id": video.id,
        "title": title,
        "content": "\n\n".join(["\n".join(header), *sections]),
        "url": getattr(video, "player_url", None) or "",
        "_deleted": False,
    }


def _sections(runs: list[Any]) -> list[str]:
    sections = []
    for run in runs:
        for analyzer in sorted(run.analyzers, key=lambda a: (a.name or "", a.id or "")):
            if analyzer.status != "done":
                continue
            output = run.get_analyzer_output(analyzer.id or analyzer.name)
            section = render_analyzer_output(analyzer.type or "", analyzer.name or "", output)
            if section:
                sections.append(section)
    return sections


def _iter_rows(
    collection: Any,
    collection_id: str,
    state: dict,
    stats: dict[str, int],
    *,
    understand: list[dict[str, Any]] | None = None,
) -> Iterator[dict]:
    """Yield changed videos, then tombstones. Pure of dlt, so tests drive it with a dict."""
    stats.clear()
    stats.update(scanned=0, changed=0, unchanged=0, not_understood=0, started=0, deleted=0)
    previous: dict[str, dict] = state.get("videos", {})
    present: dict[str, dict] = {}
    node_set = [f"videodb:{collection_id}"]

    for video in collection.get_videos():
        stats["scanned"] += 1
        runs = video.list_understandings()
        done = sorted((run for run in runs if run.status == "done" and run.id), key=lambda r: r.id)
        if not done:
            stats["not_understood"] += 1
            # Only a video with no run at all: a failed run is not retried at the
            # account's expense on every sync.
            if understand and not runs:
                video.understand(analyzers=[dict(analyzer) for analyzer in understand])
                stats["started"] += 1
            continue

        seen = {"name": video.name or "", "runs": [run.id for run in done]}
        if previous.get(video.id) == seen:
            present[video.id] = seen
            stats["unchanged"] += 1
            continue

        sections = _sections(done)
        if not sections:
            stats["not_understood"] += 1
            continue
        present[video.id] = seen
        stats["changed"] += 1
        yield {**render_video(video, sections), NODE_SET_COLUMN: node_set}

    for video_id in sorted(set(previous) - set(present)):
        stats["deleted"] += 1
        yield {"id": video_id, "_deleted": True}

    state["videos"] = present
    logger.info(
        "VideoDB: scanned %d video(s): %d changed, %d unchanged, %d without understanding "
        "(%d runs started), %d deletion(s).",
        stats["scanned"],
        stats["changed"],
        stats["unchanged"],
        stats["not_understood"],
        stats["started"],
        stats["deleted"],
    )


def _default_resource_name(collection_id: str) -> str:
    return "videodb_" + re.sub(r"[^a-z0-9]+", "_", collection_id.lower()).strip("_")


def videodb_source(
    *,
    collection_id: str = "default",
    api_key: str | None = None,
    base_url: str | None = None,
    resource_name: str | None = None,
    understand: bool | list[dict[str, Any]] = False,
    check_active: Callable[[], None] | None = None,
    connection: Any = None,
):
    """Return a ``dlt`` resource that yields one VideoDB collection's videos for ``remember``.

    Args:
        collection_id: The VideoDB collection to sync (``c-...``, or ``default``).
        api_key: VideoDB API key. Falls back to ``VIDEO_DB_API_KEY``.
        base_url: VideoDB API base URL. Defaults to the SDK's.
        resource_name: Stable dlt resource name, also the staging table and the key
            of the incremental state. Defaults to one derived from the collection id.
            Two sources sharing a name share state, so each would forget the other's
            videos.
        understand: ``True`` starts a ``spoken_words`` + ``vlm`` understanding run for
            each video that has none; a list of analyzer dicts starts those instead.
            Runs are billed by VideoDB. Off by default.
        check_active: Optional host authorization checkpoint during extraction.
        connection: Pre-built ``videodb`` connection (mainly a test-injection point).

    Returns:
        A ``dlt`` resource configured with ``primary_key="id"``,
        ``write_disposition="merge"`` and a ``_deleted`` hard-delete column.
    """
    import dlt

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 2:
        raise RuntimeError(
            "VideoDB sync requires a Cognee build that reads per-row node sets "
            "(DOCUMENT_SYNC_VERSION >= 2). Upgrade Cognee before syncing."
        )

    if connection is None:
        try:
            import videodb
        except ImportError as exc:
            raise ImportError(
                'The VideoDB connector requires the videodb package: pip install "cognee[videodb]"'
            ) from exc
        resolved_key = api_key or os.getenv("VIDEO_DB_API_KEY")
        if not resolved_key:
            raise ValueError("VideoDB API key required: set api_key= or VIDEO_DB_API_KEY.")
        options = {"base_url": base_url} if base_url else {}
        connection = videodb.connect(api_key=resolved_key, **options)

    if understand is True:
        analyzers = DEFAULT_ANALYZERS
    elif understand:
        analyzers = list(understand)
    else:
        analyzers = None

    name = resource_name or _default_resource_name(collection_id)
    stats: dict[str, int] = {}

    @dlt.resource(
        name=name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def videodb_videos():
        collection = connection.get_collection(collection_id)
        yield from dlt_utils.guarded_rows(
            _iter_rows(
                collection,
                collection_id,
                dlt.current.resource_state(),
                stats,
                understand=analyzers,
            ),
            check_active,
        )

    resource = videodb_videos()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "videodb")
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, name)
    resource.cognee_sync_stats = stats
    return resource
