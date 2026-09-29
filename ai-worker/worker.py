"""Consumes versioned perception observations; GPU inference stays outside the API process.

Producers (OpenCV development adapter or NVIDIA DeepStream integration) publish JSON to
the Redis stream `ai:observations`. No detector weights are bundled or fabricated here.
"""
import asyncio
import json
import logging
import os
import math
from datetime import datetime, timezone
from hashlib import sha256
import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from redis.asyncio import Redis
from redis.exceptions import ResponseError

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("intelliwatch.worker")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
API_URL = os.getenv("API_URL", "http://api:8000")
AI_SERVICE_TOKEN = os.getenv("AI_SERVICE_TOKEN", "")
DEMO_MODE = os.getenv("DEMO_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}

class Observation(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    schema_version: str = "1.0"
    camera_id: str
    timestamp: datetime
    object_id: str
    object_class: str = Field(alias="class")
    confidence: float = Field(ge=0, le=1)
    bbox: tuple[float, float, float, float]
    model_version: str
    center: tuple[float, float] | None = None
    zone_id: str | None = None
    zone_polygon: list[tuple[float, float]] | None = None
    zone_type: str | None = None
    dwell_threshold_seconds: int | None = None
    dwell_seconds: float | None = None
    ppe: dict[str, bool | None] = Field(default_factory=dict)
    ppe_confidences: dict[str, float] = Field(default_factory=dict)
    posture: str | None = None
    low_motion_seconds: float | None = None
    machine_id: str | None = None
    machine_state: str | None = None
    anomaly_score: float | None = Field(default=None, ge=0, le=1)
    machine_baseline: dict | None = None
    evidence_ref: str | None = None

def inside_polygon(point: tuple[float, float], polygon: list[tuple[float, float]]) -> bool:
    x, y = point; inside = False; j = len(polygon) - 1
    for i, (xi, yi) in enumerate(polygon):
        xj, yj = polygon[j]
        if ((yi > y) != (yj > y)) and x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi: inside = not inside
        j = i
    return inside

def rules(observation: Observation) -> list[dict]:
    """Derive explainable safety candidates from model output and calibrated zone metadata."""
    out = []; base = {"timestamp": observation.timestamp, "camera_id": observation.camera_id, "object_ids": [observation.object_id], "confidence": observation.confidence, "evidence_ref": observation.evidence_ref, "source": "scene-rules-v1"}
    for item in ("helmet", "vest"):
        if observation.ppe.get(item) is False:
            out.append({**base, "confidence": observation.ppe_confidences.get(item, observation.confidence), "event_type": f"missing_{item}", "severity": "high", "reason_codes": [f"ppe_{item}_not_detected"], "payload": {"ppe_confidence": observation.ppe_confidences.get(item), "model_version": observation.model_version}})
    if observation.zone_id and observation.center and observation.zone_polygon and inside_polygon(observation.center, observation.zone_polygon):
        if observation.zone_type == "restricted":
            out.append({**base, "event_type": "restricted_zone_intrusion", "severity": "high", "reason_codes": ["zone_crossing", "person_present"], "zone_id": observation.zone_id, "payload": {"model_version": observation.model_version}})
        if observation.dwell_threshold_seconds and (observation.dwell_seconds or 0) >= observation.dwell_threshold_seconds:
            out.append({**base, "event_type": "zone_dwell_violation", "severity": "medium", "reason_codes": ["zone_occupancy", "dwell_threshold_exceeded"], "zone_id": observation.zone_id, "payload": {"dwell_seconds": observation.dwell_seconds, "model_version": observation.model_version}})
    if observation.posture == "horizontal" and (observation.low_motion_seconds or 0) >= 3:
        out.append({**base, "event_type": "possible_fall", "severity": "high", "reason_codes": ["horizontal_pose", "sustained_low_motion"], "payload": {"low_motion_seconds": observation.low_motion_seconds, "model_version": observation.model_version}})
    if observation.machine_id and observation.anomaly_score is not None and observation.anomaly_score >= 0.8:
        reasons = ["machine_state_deviation", "anomaly_threshold_exceeded"]
        if observation.machine_baseline and observation.machine_baseline.get("method") == "rolling_state_frequency_v1": reasons.append("historical_state_rarity")
        out.append({**base, "confidence": observation.anomaly_score, "event_type": "machine_anomaly_candidate", "severity": "medium", "reason_codes": reasons, "payload": {"machine_id": observation.machine_id, "machine_state": observation.machine_state, "anomaly_score": observation.anomaly_score, "machine_baseline": observation.machine_baseline, "model_version": observation.model_version}})
    return out

async def main():
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    pending_cursor = "0-0"
    try: await redis.xgroup_create("ai:observations", "scene-rules", id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc): raise
    async with httpx.AsyncClient(base_url=API_URL, timeout=8, headers={"Authorization": "Bearer " + AI_SERVICE_TOKEN}) as client:
        async def simulate():
            """Explicitly synthetic input for exercising the production ingestion path."""
            n = 0
            while True:
                n += 1
                now = datetime.now(timezone.utc)
                camera_id = "demo-camera-01" if (n // 15) % 2 == 0 else "demo-camera-02"
                x = 720 + 250 * math.sin(n / 7)
                y = 460 + 160 * math.cos(n / 9)
                sample = {
                    "schema_version": "1.0", "observation_key": f"demo-simulator:{camera_id}:{n}",
                    "camera_id": camera_id, "timestamp": now.isoformat(), "object_id": f"demo-worker-{(n // 15) % 2 + 1}",
                    "class": "person", "confidence": 0.91, "bbox": [x-36, y-90, x+36, y+90],
                    "model_version": "demo-simulator-1.0", "zone_id": "demo-restricted-zone" if camera_id == "demo-camera-01" else None,
                    "speed_mps": round(0.4 + (n % 5) * 0.12, 2), "depth_m": 3.4,
                    "ppe": {"helmet": False if n % 17 in (0, 1, 2) else True, "vest": True},
                    "ppe_confidences": {"helmet": 0.91, "vest": 0.94},
                    "posture": "standing", "dwell_seconds": 24 if n % 17 == 0 else 0,
                }
                await redis.xadd("ai:observations", {"payload": json.dumps(sample)})
                if n % 5 == 1:
                    try:
                        await client.post(f"/api/v1/cameras/{camera_id}/health", json={"timestamp": now.isoformat(), "fps": 15, "latency_ms": 120, "dropped_frames": 0, "health_state": "online"})
                    except Exception:
                        log.exception("demo camera health sample was not accepted")
                await asyncio.sleep(2)

        simulator = asyncio.create_task(simulate()) if DEMO_MODE else None
        if DEMO_MODE:
            log.warning("DEMO_MODE is enabled: synthetic observations will be labeled demo-simulator and persisted through the normal queue")
        while True:
            try:
                await redis.set("ai:worker:heartbeat", datetime.now(timezone.utc).isoformat(), ex=30)
                pending = await redis.xautoclaim("ai:observations", "scene-rules", "worker-1", min_idle_time=3000, start_id=pending_cursor, count=32)
                pending_cursor = pending[0]
                batches = [("ai:observations", pending[1])] if pending[1] else []
                fresh = await redis.xreadgroup("scene-rules", "worker-1", {"ai:observations": ">"}, count=32, block=5000)
                batches.extend(fresh)
                for _, rows in batches:
                    for row_id, fields in rows:
                        try:
                            raw = fields.get("payload", "{}")
                            observation = Observation.model_validate_json(raw)
                            for candidate in rules(observation):
                                stable = f"{observation.camera_id}:{observation.object_id}:{candidate['event_type']}:{int(observation.timestamp.timestamp()) // 10}"
                                if not await redis.set("cooldown:" + sha256(stable.encode()).hexdigest(), "1", ex=30, nx=True): continue
                                body = {"schema_version": "1.0", "idempotency_key": sha256(stable.encode()).hexdigest(), **candidate, "timestamp": observation.timestamp.isoformat().replace("+00:00", "Z")}
                                try:
                                    response = await client.post("/api/v1/events/ingest", json=body)
                                    response.raise_for_status()
                                except Exception:
                                    await redis.delete("cooldown:" + sha256(stable.encode()).hexdigest())
                                    raise
                            await redis.xack("ai:observations", "scene-rules", row_id)
                        except (ValidationError, ValueError) as exc:
                            log.error("invalid observation %s: %s", row_id, exc)
                            await redis.xack("ai:observations", "scene-rules", row_id)
                        except Exception:
                            log.exception("failed processing observation %s; leaving pending for retry", row_id)
            except Exception:
                log.exception("worker loop failure")
                await asyncio.sleep(2)

if __name__ == "__main__": asyncio.run(main())
