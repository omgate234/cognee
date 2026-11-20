import asyncio
import os
from typing import Any

import videodb

from cognee.infrastructure.files.storage import get_file_storage, get_storage_config
from cognee.infrastructure.files.utils.get_file_metadata import get_file_metadata
from cognee.infrastructure.loaders.LoaderInterface import LoaderInterface, LoaderResult
from cognee.infrastructure.loaders.store_derived_text import store_derived_text
from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.connectors.videodb import (
    DEFAULT_ANALYZERS,
    render_runs,
    render_video,
)

logger = get_logger(__name__)

DEFAULT_TIMEOUT_SECONDS = 1800


class VideoDBLoader(LoaderInterface):
    """
    Video loader backed by VideoDB.

    Uploads the file to a VideoDB collection, runs an understanding pass on it
    (``spoken_words`` + ``vlm`` by default) and stores the result as text: one
    section per analyzer, one ``[HH:MM:SS-HH:MM:SS]`` line per scene. Unlike
    ``video_loader`` it describes what is on screen, not only what is said, and
    needs no LLM or ffmpeg on the cognee side.

    It is never picked by default. Ask for it per call::

        await cognee.add(
            "talk.mp4",
            preferred_loaders={"videodb_loader": {"collection_id": "c-..."}},
        )

    Options (all optional): ``collection_id`` (default ``default``),
    ``analyzers`` (VideoDB analyzer dicts), ``segmentation``, ``transform``,
    ``timeout`` in seconds (default 1800) and ``base_url``. The API key is read
    from ``VIDEO_DB_API_KEY`` only, so it never travels with pipeline options.
    The uploaded video stays in the collection.
    """

    loader_name = "videodb_loader"

    @property
    def supported_extensions(self) -> list[str]:
        return ["mp4", "m4v", "mov", "webm", "mkv", "avi"]

    @property
    def supported_mime_types(self) -> list[str]:
        return [
            "video/mp4",
            "video/x-m4v",
            "video/quicktime",
            "video/webm",
            "video/x-matroska",
            "video/x-msvideo",
        ]

    def can_handle(self, extension: str, mime_type: str) -> bool:
        return bool(
            extension in self.supported_extensions and mime_type in self.supported_mime_types
        )

    async def load(self, file_path: str, **kwargs: Any) -> "str | LoaderResult":
        """Upload, understand and render the video.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If ``VIDEO_DB_API_KEY`` is not set.
            RuntimeError: If the understanding run fails or yields no content.
            TimeoutError: If the run does not finish within ``timeout``.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        api_key = os.getenv("VIDEO_DB_API_KEY")
        if not api_key:
            raise ValueError("videodb_loader needs the VIDEO_DB_API_KEY environment variable.")

        with open(file_path, "rb") as f:
            file_metadata = await get_file_metadata(f)
        storage_file_name = "text_" + file_metadata["content_hash"] + ".txt"

        content = await asyncio.to_thread(self._understand, file_path, api_key, kwargs)

        if not kwargs.get("persist", True):
            return content

        storage = get_file_storage(get_storage_config()["data_root_directory"])
        return await store_derived_text(storage, storage_file_name, content)

    def _understand(self, file_path: str, api_key: str, options: dict[str, Any]) -> str:
        connect_options = {"base_url": options["base_url"]} if options.get("base_url") else {}
        connection = videodb.connect(api_key=api_key, **connect_options)
        collection = connection.get_collection(options.get("collection_id") or "default")

        video = collection.upload(file_path=file_path)
        logger.info("videodb_loader: uploaded %s as %s", os.path.basename(file_path), video.id)

        run = video.understand(
            analyzers=[dict(a) for a in options.get("analyzers") or DEFAULT_ANALYZERS],
            segmentation=options.get("segmentation"),
            transform=options.get("transform"),
        )
        run.wait_until_complete(timeout=int(options.get("timeout") or DEFAULT_TIMEOUT_SECONDS))
        if not run.is_successful:
            raise RuntimeError(
                f"VideoDB understanding run {run.id} for video {video.id} ended as {run.status!r}."
            )

        sections = render_runs([run])
        if not sections:
            raise RuntimeError(
                f"VideoDB understanding run {run.id} for video {video.id} produced no content."
            )
        return render_video(video, sections)["content"]
