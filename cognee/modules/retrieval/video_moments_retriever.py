import asyncio
import json
import os
import re
from typing import Any
from uuid import UUID

from sqlalchemy import select

from cognee.infrastructure.databases.relational import get_relational_engine
from cognee.modules.data.models import Data
from cognee.modules.retrieval.chunks_retriever import ChunksRetriever
from cognee.shared.logging_utils import get_logger

logger = get_logger("VideoMomentsRetriever")

# The line format the VideoDB connector and loader write: "[HH:MM:SS-HH:MM:SS] text".
_LINE = re.compile(r"^\[(\d+):(\d\d):(\d\d)-(\d+):(\d\d):(\d\d)\] (.+)$")
_SOURCE = "videodb"


def _seconds(hours: str, minutes: str, seconds: str) -> int:
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds)


def parse_moment_lines(text: str) -> list[tuple[int, int, str]]:
    """The timestamped lines of a chunk as ``(start, end, text)``."""
    lines = []
    for line in text.splitlines():
        match = _LINE.match(line.strip())
        if match:
            groups = match.groups()
            lines.append((_seconds(*groups[0:3]), _seconds(*groups[3:6]), groups[6]))
    return lines


def _collection_id(node_set: Any) -> str | None:
    if isinstance(node_set, str):
        try:
            node_set = json.loads(node_set)
        except ValueError:
            return None
    for name in node_set or []:
        if isinstance(name, str) and name.startswith(f"{_SOURCE}:"):
            return name[len(_SOURCE) + 1 :]
    return None


class VideoMomentsRetriever(ChunksRetriever):
    """
    Finds the moments in VideoDB videos that answer a query.

    Runs the same vector search as ``CHUNKS``, keeps the chunks of documents
    ingested from VideoDB (``videodb_source`` or ``videodb_loader``) and returns
    one moment per chunk: the video id, the span its timestamped lines cover and
    those lines. With ``stream_clips=True`` each moment also gets a VideoDB
    stream URL that plays just that span; that needs ``VIDEO_DB_API_KEY`` and the
    ``videodb`` extra, and costs one API call per moment.
    """

    def __init__(
        self,
        top_k: int | None = 5,
        node_name: list[str] | None = None,
        node_name_filter_operator: str = "OR",
        stream_clips: bool = False,
    ):
        super().__init__(
            top_k=top_k, node_name=node_name, node_name_filter_operator=node_name_filter_operator
        )
        self.stream_clips = stream_clips

    async def get_retrieved_objects(self, query: str) -> list[dict]:
        chunks = await super().get_retrieved_objects(query)
        videos = await self._video_documents(chunks)
        moments = []
        for chunk in chunks:
            payload = chunk.payload or {}
            video = videos.get(str(payload.get("document_id")))
            lines = parse_moment_lines(payload.get("text") or "")
            if video is None or not lines:
                continue
            moments.append(
                {
                    **video,
                    "start": min(start for start, _, _ in lines),
                    "end": max(end for _, end, _ in lines),
                    "text": "\n".join(text for _, _, text in lines),
                    "score": chunk.score,
                    "document_id": payload.get("document_id"),
                    "chunk_id": str(chunk.id),
                }
            )
        if self.stream_clips and moments:
            await asyncio.to_thread(self._add_streams, moments)
        return moments

    async def get_context_from_objects(self, query: str, retrieved_objects: Any) -> str:
        return "\n\n".join(
            f"{m['title']} ({m['video_id']}) {m['start']}s-{m['end']}s:\n{m['text']}"
            for m in retrieved_objects or []
        )

    async def get_completion_from_context(
        self, query: str, retrieved_objects: Any, context: Any
    ) -> list[dict]:
        return list(retrieved_objects or [])

    async def _video_documents(self, chunks: list[Any]) -> dict[str, dict]:
        """Map the hits' document ids to their VideoDB video, for documents from VideoDB."""
        ids = set()
        for chunk in chunks:
            try:
                ids.add(UUID(str((chunk.payload or {}).get("document_id"))))
            except ValueError:
                continue
        if not ids:
            return {}
        engine = get_relational_engine()
        async with engine.get_async_session() as session:
            rows = await session.execute(
                select(Data.id, Data.system_metadata, Data.node_set).where(Data.id.in_(ids))
            )
        videos = {}
        for data_id, system_metadata, node_set in rows:
            metadata = system_metadata or {}
            if metadata.get("source") != _SOURCE or not metadata.get("external_id"):
                continue
            videos[str(data_id)] = {
                "video_id": metadata["external_id"],
                "collection_id": _collection_id(node_set),
                "title": metadata.get("title") or metadata["external_id"],
                "player_url": metadata.get("url"),
            }
        return videos

    @staticmethod
    def _add_streams(moments: list[dict]) -> None:
        try:
            import videodb
            from videodb.exceptions import VideodbError
        except ImportError:
            logger.warning('stream_clips needs the videodb extra: pip install "cognee[videodb]"')
            return
        api_key = os.getenv("VIDEO_DB_API_KEY")
        if not api_key:
            logger.warning("stream_clips needs the VIDEO_DB_API_KEY environment variable.")
            return
        connection = videodb.connect(api_key=api_key)
        for moment in moments:
            try:
                collection = connection.get_collection(moment["collection_id"] or "default")
                video = collection.get_video(moment["video_id"])
                moment["stream_url"] = video.generate_stream(
                    timeline=[(moment["start"], moment["end"])]
                )
            except VideodbError as error:
                # One clip that cannot be streamed must not hide the other moments.
                logger.warning(
                    "Could not stream %s %ss-%ss: %s",
                    moment["video_id"],
                    moment["start"],
                    moment["end"],
                    type(error).__name__,
                )
