"""In-memory stand-in for the parts of the videodb SDK the connector uses."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakeAnalyzer:
    name: str
    type: str
    status: str = "done"
    id: str | None = None


@dataclass
class FakeUnderstanding:
    id: str
    status: str
    analyzers: list[FakeAnalyzer]
    outputs: dict[str, Any] = field(default_factory=dict)
    output_calls: int = 0

    def get_analyzer_output(self, name_or_id: str) -> Any:
        self.output_calls += 1
        return self.outputs[name_or_id]


@dataclass
class FakeVideo:
    id: str
    name: str
    length: float = 0.0
    player_url: str | None = None
    understandings: list[FakeUnderstanding] = field(default_factory=list)
    started: list[list[dict]] = field(default_factory=list)

    def list_understandings(self) -> list[FakeUnderstanding]:
        return list(self.understandings)

    def understand(self, analyzers: list[dict]) -> FakeUnderstanding:
        self.started.append(analyzers)
        run = FakeUnderstanding(f"und_started{len(self.started)}", "queued", [])
        self.understandings.append(run)
        return run


class FakeCollection:
    def __init__(self) -> None:
        self.videos: dict[str, FakeVideo] = {}
        self.fail_listing = False

    def get_videos(self) -> list[FakeVideo]:
        if self.fail_listing:
            raise RuntimeError("VideoDB is unavailable")
        return list(self.videos.values())

    def add(self, video_id: str, name: str, *runs: FakeUnderstanding) -> FakeVideo:
        video = FakeVideo(
            video_id,
            name,
            length=95.0,
            player_url=f"https://console.videodb.io/player?v={video_id}",
            understandings=list(runs),
        )
        self.videos[video_id] = video
        return video


class FakeConnection:
    def __init__(self, collection: FakeCollection) -> None:
        self.collection = collection
        self.requested: list[str] = []

    def get_collection(self, collection_id: str) -> FakeCollection:
        self.requested.append(collection_id)
        return self.collection


def transcript_run(run_id: str, *lines: tuple[float, float, str]) -> FakeUnderstanding:
    scenes = [{"start": s, "end": e, "data": {"text": text}} for s, e, text in lines]
    return FakeUnderstanding(
        run_id,
        "done",
        [FakeAnalyzer("transcript", "spoken_words")],
        {"transcript": {"name": "transcript", "type": "spoken_words", "scenes": scenes}},
    )
