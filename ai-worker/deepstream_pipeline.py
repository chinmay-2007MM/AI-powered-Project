"""NVIDIA DeepStream video-to-observation adapter for IntelliWatch.

This process runs next to the DeepStream SDK, not in the FastAPI container. It
captures configured camera/video sources, runs configured TensorRT inference
and the DeepStream tracker, then forwards actual frame metadata through the
existing versioned observation API. Model files, labels and source credentials
are deployment inputs and are deliberately not bundled.
"""
from __future__ import annotations

import json
import logging
import math
import os
import queue
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx


LOG = logging.getLogger("intelliwatch.deepstream")
LOG_HANDLER = logging.StreamHandler()
LOG_HANDLER.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), handlers=[LOG_HANDLER], force=True)


@dataclass(frozen=True)
class Source:
    camera_id: str
    uri: str
    model_version: str


@dataclass
class SourceState:
    frames: int = 0
    last_frame_at: float = 0.0
    posted: int = 0
    last_posted_at: float = 0.0
    last_health_state: str | None = None


def _redact_uri(uri: str) -> str:
    """Remove user info and query parameters before a URI enters the logs."""
    try:
        parts = urlsplit(uri)
        host = parts.hostname or ""
        if parts.port:
            host += f":{parts.port}"
        return urlunsplit((parts.scheme, host, parts.path, "[redacted]" if parts.query else "", ""))
    except ValueError:
        return "[configured video source]"


def _load_sources() -> list[Source]:
    raw = os.getenv("DEEPSTREAM_SOURCES_JSON", "")
    if not raw:
        raise RuntimeError("DEEPSTREAM_SOURCES_JSON is required; provide real registered camera IDs and RTSP/file URIs")
    try:
        values = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("DEEPSTREAM_SOURCES_JSON must be a JSON array") from exc
    if not isinstance(values, list) or not values:
        raise RuntimeError("DEEPSTREAM_SOURCES_JSON must contain at least one source")
    sources: list[Source] = []
    camera_ids: set[str] = set()
    for item in values:
        if not isinstance(item, dict) or not all(isinstance(item.get(k), str) and item[k].strip() for k in ("camera_id", "uri", "model_version")):
            raise RuntimeError("Each source requires non-empty camera_id, uri, and model_version strings")
        if item["camera_id"] in camera_ids:
            raise RuntimeError(f"Duplicate camera_id in DEEPSTREAM_SOURCES_JSON: {item['camera_id']}")
        camera_ids.add(item["camera_id"])
        sources.append(Source(item["camera_id"], item["uri"], item["model_version"]))
    return sources


class ObservationPublisher:
    """Bounded handoff keeps HTTP/database latency out of the GStreamer probe."""

    def __init__(self, api_url: str, token: str, sources: list[Source], states: list[SourceState], capacity: int):
        if not token:
            raise RuntimeError("AI_SERVICE_TOKEN is required to submit observations")
        self.sources = sources
        self.states = states
        self.items: queue.Queue[tuple[str, dict[str, Any]] | None] = queue.Queue(maxsize=capacity)
        self.dropped = 0
        self.stop = threading.Event()
        self.client = httpx.Client(base_url=api_url.rstrip("/"), timeout=httpx.Timeout(8.0), headers={"Authorization": f"Bearer {token}"})
        self.thread = threading.Thread(target=self._run, name="observation-publisher", daemon=True)
        self.thread.start()

    def submit(self, camera_id: str, payload: dict[str, Any]) -> None:
        try:
            self.items.put_nowait((camera_id, payload))
        except queue.Full:
            self.dropped += 1
            if self.dropped == 1 or self.dropped % 100 == 0:
                LOG.error("Observation queue is full; dropped=%d. Reduce inference rate or increase OBSERVATION_QUEUE_CAPACITY.", self.dropped)

    def get(self, path: str) -> Any:
        response = self.client.get(path)
        response.raise_for_status()
        return response.json()

    def heartbeat(self, index: int, health_state: str, fps: float) -> None:
        state = self.states[index]
        if state.last_health_state == health_state and health_state not in {"online", "degraded"}:
            return
        body = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "fps": max(0.0, fps),
            "health_state": health_state,
        }
        try:
            response = self.client.post(f"/api/v1/cameras/{self.sources[index].camera_id}/health", json=body)
            response.raise_for_status()
            state.last_health_state = health_state
        except httpx.HTTPError as exc:
            LOG.warning("Camera health update failed for %s: %s", self.sources[index].camera_id, exc.__class__.__name__)

    def _run(self) -> None:
        while not self.stop.is_set() or not self.items.empty():
            try:
                entry = self.items.get(timeout=0.5)
            except queue.Empty:
                continue
            if entry is None:
                self.items.task_done()
                break
            camera_id, payload = entry
            for attempt in range(4):
                try:
                    response = self.client.post("/api/v1/observations", json=payload)
                    response.raise_for_status()
                    source_index = next(i for i, source in enumerate(self.sources) if source.camera_id == camera_id)
                    self.states[source_index].posted += 1
                    self.states[source_index].last_posted_at = time.monotonic()
                    break
                except (httpx.HTTPError, StopIteration) as exc:
                    if attempt == 3:
                        LOG.error("Observation delivery failed for camera %s after retries (%s)", camera_id, exc.__class__.__name__)
                    else:
                        time.sleep(min(0.25 * (2**attempt), 2.0))
            self.items.task_done()

    def close(self, timeout: float = 10.0) -> None:
        self.stop.set()
        self.thread.join(timeout=timeout)
        self.client.close()


def _read_meta(value: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        attribute = getattr(value, name, None)
        if attribute is not None:
            return attribute() if callable(attribute) else attribute
    return default


def _inside_polygon(point: tuple[float, float], polygon: list[list[float]]) -> bool:
    x, y = point
    inside = False
    previous = len(polygon) - 1
    for current, (xi, yi) in enumerate(polygon):
        xj, yj = polygon[previous]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi:
            inside = not inside
        previous = current
    return inside


def run() -> None:
    # PyServiceMaker is NVIDIA's recommended Python API for current DeepStream.
    try:
        from pyservicemaker import BatchMetadataOperator, Pipeline, Probe
    except ImportError as exc:
        raise RuntimeError("NVIDIA DeepStream PyServiceMaker was not found. Run this adapter inside a compatible DeepStream SDK runtime.") from exc

    api_url = os.getenv("API_URL", "http://127.0.0.1:8000")
    token = os.getenv("AI_SERVICE_TOKEN", "")
    pgie_config = os.getenv("DEEPSTREAM_PGIE_CONFIG", "")
    if not pgie_config or not os.path.isfile(pgie_config):
        raise RuntimeError("DEEPSTREAM_PGIE_CONFIG must point to a deployed DeepStream nvinfer config file")
    sources = _load_sources()
    interval = float(os.getenv("DEEPSTREAM_OBSERVATION_INTERVAL_SECONDS", "1.0"))
    if not math.isfinite(interval) or interval <= 0:
        raise RuntimeError("DEEPSTREAM_OBSERVATION_INTERVAL_SECONDS must be a positive number")
    queue_capacity = int(os.getenv("OBSERVATION_QUEUE_CAPACITY", "2048"))
    if queue_capacity < 1:
        raise RuntimeError("OBSERVATION_QUEUE_CAPACITY must be positive")
    states = [SourceState() for _ in sources]
    last_sent: dict[tuple[int, int], float] = {}
    dwell_since: dict[tuple[int, int, str], float] = {}
    publisher = ObservationPublisher(api_url, token, sources, states, queue_capacity)

    try:
        cameras = [publisher.get(f"/api/v1/cameras/{source.camera_id}") for source in sources]
        zones = publisher.get("/api/v1/zones")
    except httpx.HTTPError as exc:
        publisher.close()
        raise RuntimeError(f"Could not load registered cameras and zone calibration from IntelliWatch ({exc.__class__.__name__})") from exc
    site_ids = {source.camera_id: cameras[i].get("site_id") for i, source in enumerate(sources)}
    zones_lock = threading.Lock()
    zones_by_camera: dict[str, list[dict[str, Any]]] = {}
    for index, source in enumerate(sources):
        zones_by_camera[source.camera_id] = [
            zone for zone in zones
            if zone.get("enabled", True)
            and zone.get("site_id") == site_ids[source.camera_id]
            and zone.get("camera_id") in (None, source.camera_id)
        ]

    def refresh_zones() -> None:
        try:
            current_zones = publisher.get("/api/v1/zones")
            next_zones = {
                source.camera_id: [
                    zone for zone in current_zones
                    if zone.get("enabled", True)
                    and zone.get("site_id") == site_ids[source.camera_id]
                    and zone.get("camera_id") in (None, source.camera_id)
                ]
                for source in sources
            }
            with zones_lock:
                zones_by_camera.clear()
                zones_by_camera.update(next_zones)
        except httpx.HTTPError as exc:
            LOG.warning("Zone configuration refresh failed (%s); retaining last known polygons", exc.__class__.__name__)

    mux_width = int(os.getenv("DEEPSTREAM_MUX_WIDTH", "1920"))
    mux_height = int(os.getenv("DEEPSTREAM_MUX_HEIGHT", "1080"))
    tracker_config = os.getenv("DEEPSTREAM_TRACKER_CONFIG", "")
    if not tracker_config or not os.path.isfile(tracker_config):
        publisher.close()
        raise RuntimeError("DEEPSTREAM_TRACKER_CONFIG must point to a real DeepStream nvtracker config file; observations require stable track IDs")
    tracker_library = os.getenv("DEEPSTREAM_TRACKER_LIBRARY", "")
    if not tracker_library or not os.path.isfile(tracker_library):
        publisher.close()
        raise RuntimeError("DEEPSTREAM_TRACKER_LIBRARY must point to the NVIDIA DeepStream multi-object tracker library")
    try:
        pipeline = Pipeline("intelliwatch-perception")
        for index, source in enumerate(sources):
            pipeline.add("nvurisrcbin", f"source_{index}", {"uri": source.uri})
        pipeline.add("nvstreammux", "mux", {
            "batch-size": len(sources), "width": mux_width, "height": mux_height,
            "live-source": 1, "batched-push-timeout": int(os.getenv("DEEPSTREAM_BATCH_TIMEOUT_USEC", "40000")),
        })
        pipeline.add("nvinferbin", "primary_inference", {"config-file-path": os.path.abspath(pgie_config)})
        pipeline.add("nvtracker", "tracker", {"ll-lib-file": os.path.abspath(tracker_library), "ll-config-file": os.path.abspath(tracker_config)})
        pipeline.add("fakesink", "metadata_sink", {"sync": False, "async": False})
        for index in range(len(sources)):
            pipeline.link((f"source_{index}", "mux"), ("", f"sink_{index}"))
        pipeline.link("mux", "primary_inference", "tracker", "metadata_sink")
    except Exception as exc:
        publisher.close()
        raise RuntimeError(f"DeepStream Service Maker pipeline could not be configured ({exc.__class__.__name__}); check source, plugin and model settings") from exc

    class ObservationMetadata(BatchMetadataOperator):
        def handle_metadata(self, batch_meta: Any) -> None:
            for frame in batch_meta.frame_items:
                source_index = int(_read_meta(frame, "source_id", "sourceId", "pad_index", default=-1))
                if source_index < 0 or source_index >= len(sources):
                    continue
                state = states[source_index]
                state.frames += 1
                state.last_frame_at = time.monotonic()
                camera_id = sources[source_index].camera_id
                buffer_pts = int(_read_meta(frame, "buffer_pts", "bufferPTS", default=0) or 0)
                media_seconds = buffer_pts / 1_000_000_000 if buffer_pts > 0 else time.monotonic()
                for obj in frame.object_items:
                    track_id = int(_read_meta(obj, "object_id", "objectId", default=0xFFFFFFFFFFFFFFFF))
                    # DeepStream's sentinel means tracking has not assigned an
                    # identity. Do not persist a fabricated track ID.
                    if track_id == 0xFFFFFFFFFFFFFFFF:
                        continue
                    now_mono = time.monotonic()
                    key = (source_index, track_id)
                    if now_mono - last_sent.get(key, 0.0) < interval:
                        continue
                    last_sent[key] = now_mono
                    label = str(_read_meta(obj, "label", "obj_label", default="") or "").strip()
                    if not label:
                        continue
                    rect = _read_meta(obj, "rect_params", "rectParams")
                    if rect is None:
                        continue
                    left, top = max(0.0, float(_read_meta(rect, "left", default=0))), max(0.0, float(_read_meta(rect, "top", default=0)))
                    right = max(left, left + float(_read_meta(rect, "width", default=0)))
                    bottom = max(top, top + float(_read_meta(rect, "height", default=0)))
                    source_width = float(_read_meta(frame, "source_width", "sourceWidth", default=0))
                    source_height = float(_read_meta(frame, "source_height", "sourceHeight", default=0))
                    pipeline_width = float(_read_meta(frame, "pipeline_width", "pipelineWidth", default=0))
                    pipeline_height = float(_read_meta(frame, "pipeline_height", "pipelineHeight", default=0))
                    # Zone polygons are calibrated in source-image pixels, while
                    # detector boxes are attached after streammux scaling.
                    # Transform boxes back before persistence and point-in-zone.
                    if source_width > 0 and pipeline_width > 0:
                        scale_x = source_width / pipeline_width
                        left, right = left * scale_x, right * scale_x
                    if source_height > 0 and pipeline_height > 0:
                        scale_y = source_height / pipeline_height
                        top, bottom = top * scale_y, bottom * scale_y
                    confidence = float(_read_meta(obj, "confidence", default=-1))
                    if confidence < 0 or not all(math.isfinite(v) for v in (left, top, right, bottom, confidence)):
                        continue
                    timestamp = datetime.now(timezone.utc)
                    person_labels = {value.strip().lower() for value in os.getenv("DEEPSTREAM_PERSON_LABELS", "person,pedestrian").split(",") if value.strip()}
                    zone_id = None
                    dwell_seconds = None
                    if label.lower() in person_labels:
                        center = ((left + right) / 2, (top + bottom) / 2)
                        with zones_lock:
                            camera_zones = list(zones_by_camera.get(camera_id, []))
                        active_zone = next((zone for zone in camera_zones if _inside_polygon(center, zone["polygon"])), None)
                        track_zone_key = (source_index, track_id, str(active_zone["id"])) if active_zone else None
                        if track_zone_key:
                            dwell_since.setdefault(track_zone_key, media_seconds)
                            dwell_seconds = max(0.0, media_seconds - dwell_since[track_zone_key])
                            zone_id = str(active_zone["id"])
                        for old_key in [k for k in dwell_since if k[:2] == key and k != track_zone_key]:
                            del dwell_since[old_key]
                    observation_key = sha256(f"{camera_id}:{track_id}:{timestamp.isoformat()}:{sources[source_index].model_version}".encode()).hexdigest()
                    publisher.submit(camera_id, {
                        "schema_version": "1.0", "observation_key": observation_key,
                        "camera_id": camera_id, "timestamp": timestamp.isoformat(),
                        "object_id": f"{camera_id}:{track_id}", "class": label,
                        "confidence": max(0.0, min(1.0, confidence)),
                        "bbox": [left, top, right, bottom],
                        "model_version": sources[source_index].model_version,
                        "zone_id": zone_id, "dwell_seconds": dwell_seconds,
                    })

    pipeline.attach("tracker", Probe("intelliwatch-observations", ObservationMetadata()))
    LOG.info("DeepStream pipeline configured for %d source(s); inference_config=%s", len(sources), os.path.basename(pgie_config))
    for source in sources:
        LOG.info("Configured camera=%s source=%s model_version=%s", source.camera_id, _redact_uri(source.uri), source.model_version)
    last_health_at = time.monotonic()
    last_frame_counts = [0] * len(sources)

    def report_health() -> None:
        nonlocal last_health_at, last_frame_counts
        now = time.monotonic()
        elapsed = max(1.0, now - last_health_at)
        for index, state in enumerate(states):
            fps = max(0.0, (state.frames - last_frame_counts[index]) / elapsed)
            age = now - state.last_frame_at if state.last_frame_at else float("inf")
            publisher.heartbeat(index, "online" if age < 15 else "offline", fps)
            last_frame_counts[index] = state.frames
        last_health_at = now

    def zone_refresh_loop() -> None:
        while not publisher.stop.wait(30):
            refresh_zones()

    def health_loop() -> None:
        while not publisher.stop.wait(10):
            report_health()

    threading.Thread(target=zone_refresh_loop, name="zone-config-refresh", daemon=True).start()
    threading.Thread(target=health_loop, name="camera-health", daemon=True).start()
    try:
        pipeline.start().wait()
    except KeyboardInterrupt:
        LOG.info("DeepStream pipeline stopped by operator")
    except Exception as exc:
        LOG.error("DeepStream pipeline stopped with an SDK error (%s); detailed source diagnostics suppressed", exc.__class__.__name__)
        for index in range(len(sources)):
            publisher.heartbeat(index, "offline", 0.0)
        raise
    finally:
        publisher.close()


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        LOG.error("DeepStream adapter stopped (%s); detailed runtime messages suppressed to protect camera credentials", exc.__class__.__name__)
        sys.exit(1)
