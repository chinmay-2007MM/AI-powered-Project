from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class CameraCreate(BaseModel):
    id: str | None = None
    site_id: str
    name: str = Field(min_length=1, max_length=160)
    stream_url_ref: str | None = None
    playback_url: str | None = None
    resolution: str | None = None
    fps: float | None = Field(default=None, gt=0)


class FactoryCreate(BaseModel):
    id: str | None = None
    name: str = Field(min_length=1, max_length=160)
    location: str | None = None
    timezone: str = "UTC"


class SiteCreate(BaseModel):
    id: str | None = None
    factory_id: str
    name: str = Field(min_length=1, max_length=160)
    floor_plan_ref: str | None = None


class SiteRead(ORMModel):
    id: str
    factory_id: str
    name: str
    floor_plan_ref: str | None
    status: str


class CameraRead(ORMModel):
    id: str
    site_id: str
    name: str
    resolution: str | None
    fps: float | None
    status: str
    playback_url: str | None


class CameraUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    stream_url_ref: str | None = None
    playback_url: str | None = None
    resolution: str | None = None
    fps: float | None = Field(default=None, gt=0)


class ZoneCreate(BaseModel):
    id: str | None = None
    site_id: str
    camera_id: str | None = None
    name: str = Field(min_length=1, max_length=160)
    polygon: list[tuple[float, float]] = Field(min_length=3)
    zone_type: str = "restricted"
    severity: Literal["low", "medium", "high", "critical"] = "high"
    dwell_threshold_seconds: int | None = Field(default=None, gt=0)


class ZoneRead(ORMModel):
    id: str
    site_id: str
    camera_id: str | None
    name: str
    polygon: list
    zone_type: str
    severity: str
    dwell_threshold_seconds: int | None
    enabled: bool


class ZoneUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    polygon: list[tuple[float, float]] | None = Field(default=None, min_length=3)
    zone_type: str | None = None
    severity: Literal["low", "medium", "high", "critical"] | None = None
    dwell_threshold_seconds: int | None = Field(default=None, gt=0)
    enabled: bool | None = None
    camera_id: str | None = None


class CameraHealthIn(BaseModel):
    timestamp: datetime
    fps: float | None = Field(default=None, ge=0)
    latency_ms: float | None = Field(default=None, ge=0)
    dropped_frames: int | None = Field(default=None, ge=0)
    health_state: Literal["online", "degraded", "offline", "frozen", "stale", "black", "blurred"]

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_include_timezone(cls, value: datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return value


class AIResult(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    observation_key: str = Field(min_length=8, max_length=180)
    camera_id: str
    timestamp: datetime
    object_id: str | None = None
    object_class: str = Field(alias="class")
    confidence: float = Field(ge=0, le=1)
    bbox: tuple[float, float, float, float]
    zone_id: str | None = None
    speed_mps: float | None = Field(default=None, ge=0)
    direction_deg: float | None = None
    depth_m: float | None = Field(default=None, ge=0)
    model_version: str
    ppe: dict[str, bool | None] = Field(default_factory=dict)
    ppe_confidences: dict[str, float] = Field(default_factory=dict)
    evidence_ref: str | None = None
    posture: str | None = None
    low_motion_seconds: float | None = Field(default=None, ge=0)
    dwell_seconds: float | None = Field(default=None, ge=0)
    machine_id: str | None = None
    machine_state: str | None = None
    anomaly_score: float | None = Field(default=None, ge=0, le=1)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_include_timezone(cls, value: datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return value


class EventIngest(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    idempotency_key: str = Field(min_length=8, max_length=180)
    event_type: str
    timestamp: datetime
    severity: Literal["low", "medium", "high", "critical"]
    confidence: float = Field(ge=0, le=1)
    source: str
    camera_id: str
    object_ids: list[str] = Field(default_factory=list)
    zone_id: str | None = None
    reason_codes: list[str] = Field(min_length=1)
    evidence_ref: str | None = None
    payload: dict = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_include_timezone(cls, value: datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return value


class IncidentStatusUpdate(BaseModel):
    status: Literal["ACKNOWLEDGED", "INVESTIGATING", "RESOLVED"]
    assigned_to: str | None = None


class IncidentRead(BaseModel):
    id: str
    event_id: str
    status: str
    assigned_to: str | None
    opened_at: datetime
    resolved_at: datetime | None
    event_type: str
    severity: str
    confidence: float
    timestamp: datetime
    camera_id: str
    object_ids: list[str]
    reason_codes: list[str]
    evidence_ref: str | None
