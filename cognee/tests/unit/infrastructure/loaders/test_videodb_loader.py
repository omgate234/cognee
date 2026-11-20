"""videodb_loader uploads, understands and renders a video, and is never a default."""

from types import SimpleNamespace

import pytest

videodb_loader = pytest.importorskip("cognee.infrastructure.loaders.external.videodb_loader")
VideoDBLoader = videodb_loader.VideoDBLoader


class FakeRun:
    def __init__(self, status="done"):
        self.id = "und_1"
        self.status = status
        self.waited_for = None
        self.analyzers = [
            SimpleNamespace(id=None, name="transcript", type="spoken_words", status="done")
        ]

    @property
    def is_successful(self):
        return self.status == "done"

    def wait_until_complete(self, timeout):
        self.waited_for = timeout
        return self

    def get_analyzer_output(self, name):
        return {"scenes": [{"start": 3, "end": 8, "data": {"text": "Welcome to the demo."}}]}


class FakeVideo:
    def __init__(self, run):
        self.id = "m-9"
        self.name = "demo.mp4"
        self.length = 12.0
        self.player_url = "https://console.videodb.io/player?v=m-9"
        self.run = run
        self.understood_with = None

    def understand(self, analyzers, segmentation=None, transform=None):
        self.understood_with = analyzers
        return self.run


@pytest.fixture
def fake_videodb(monkeypatch):
    calls = SimpleNamespace(connect=None, collection=None, uploaded=None)
    run = FakeRun()
    video = FakeVideo(run)

    def upload(file_path):
        calls.uploaded = file_path
        return video

    def get_collection(collection_id):
        calls.collection = collection_id
        return SimpleNamespace(upload=upload)

    def connect(api_key, **options):
        calls.connect = (api_key, options)
        return SimpleNamespace(get_collection=get_collection)

    monkeypatch.setenv("VIDEO_DB_API_KEY", "vdb-test-key")
    monkeypatch.setattr(videodb_loader.videodb, "connect", connect)
    return SimpleNamespace(calls=calls, run=run, video=video)


@pytest.fixture
def clip(tmp_path):
    path = tmp_path / "demo.mp4"
    # An MP4 "ftyp" box, so content sniffing reads it as video/mp4.
    path.write_bytes(b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom" + b"\x00" * 64)
    return str(path)


@pytest.mark.asyncio
async def test_renders_the_understanding_as_timestamped_text(fake_videodb, clip):
    text = await VideoDBLoader().load(clip, persist=False, collection_id="c-1")

    assert text == (
        "VideoDB video m-9\nLength: 00:00:12\n\n## Transcript\n[00:00:03-00:00:08] Welcome to the demo."
    )
    assert fake_videodb.calls.connect == ("vdb-test-key", {})
    assert fake_videodb.calls.collection == "c-1"
    assert fake_videodb.calls.uploaded == clip
    assert fake_videodb.video.understood_with == [{"type": "spoken_words"}, {"type": "vlm"}]
    assert fake_videodb.run.waited_for == 1800


@pytest.mark.asyncio
async def test_options_choose_analyzers_timeout_and_base_url(fake_videodb, clip):
    await VideoDBLoader().load(
        clip,
        persist=False,
        analyzers=[{"type": "ocr"}],
        timeout=60,
        base_url="https://api.dev.videodb.io",
    )

    assert fake_videodb.video.understood_with == [{"type": "ocr"}]
    assert fake_videodb.run.waited_for == 60
    assert fake_videodb.calls.connect[1] == {"base_url": "https://api.dev.videodb.io"}
    assert fake_videodb.calls.collection == "default"


@pytest.mark.asyncio
async def test_a_failed_run_raises(fake_videodb, clip):
    fake_videodb.run.status = "failed"

    with pytest.raises(RuntimeError, match="ended as 'failed'"):
        await VideoDBLoader().load(clip, persist=False)


@pytest.mark.asyncio
async def test_missing_api_key_raises_before_upload(fake_videodb, clip, monkeypatch):
    monkeypatch.delenv("VIDEO_DB_API_KEY")

    with pytest.raises(ValueError, match="VIDEO_DB_API_KEY"):
        await VideoDBLoader().load(clip, persist=False)
    assert fake_videodb.calls.uploaded is None


def test_only_runs_when_preferred(clip):
    from cognee.infrastructure.loaders import get_loader_engine

    engine = get_loader_engine()

    assert engine.get_loader(clip, None).loader_name == "video_loader"
    assert engine.get_loader(clip, {"videodb_loader": {}}).loader_name == "videodb_loader"
