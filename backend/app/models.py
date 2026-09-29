from datetime import datetime, timezone
from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base


def now_utc(): return datetime.now(timezone.utc)


class Factory(Base):
    __tablename__ = "factories"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    location: Mapped[str | None] = mapped_column(String(240))
    timezone: Mapped[str] = mapped_column(String(80), default="UTC")
    configuration: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class Site(Base):
    __tablename__ = "sites"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    factory_id: Mapped[str] = mapped_column(ForeignKey("factories.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    floor_plan_ref: Mapped[str | None] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(String(32), default="unconfigured")


class Camera(Base):
    __tablename__ = "cameras"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    site_id: Mapped[str] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    stream_url_ref: Mapped[str | None] = mapped_column(String(500))
    playback_url: Mapped[str | None] = mapped_column(String(500))
    resolution: Mapped[str | None] = mapped_column(String(32))
    fps: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(32), default="unconfigured")
    calibration_ref: Mapped[str | None] = mapped_column(String(500))
    calibration: Mapped[dict | None] = mapped_column(JSON)
    floorplan_x: Mapped[float | None] = mapped_column(Float)
    floorplan_y: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class Zone(Base):
    __tablename__ = "zones"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    site_id: Mapped[str] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), index=True)
    camera_id: Mapped[str | None] = mapped_column(ForeignKey("cameras.id", ondelete="SET NULL"), index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    polygon: Mapped[list] = mapped_column(JSON, nullable=False)
    zone_type: Mapped[str] = mapped_column(String(48), default="restricted")
    severity: Mapped[str] = mapped_column(String(24), default="high")
    dwell_threshold_seconds: Mapped[int | None] = mapped_column(Integer)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class TrackedObject(Base):
    __tablename__ = "tracked_objects"
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    camera_id: Mapped[str] = mapped_column(ForeignKey("cameras.id", ondelete="CASCADE"), index=True)
    object_class: Mapped[str] = mapped_column(String(64), nullable=False)
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    embedding_ref: Mapped[str | None] = mapped_column(String(500))


class GlobalTrack(Base):
    """Facility-level identity hypothesis; never overwrites camera-local track IDs."""
    __tablename__ = "global_tracks"
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    site_id: Mapped[str] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), index=True)
    object_class: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class GlobalTrackMembership(Base):
    __tablename__ = "global_track_memberships"
    __table_args__ = (UniqueConstraint("local_track_id", name="uq_global_membership_local_track"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    global_track_id: Mapped[str] = mapped_column(ForeignKey("global_tracks.id", ondelete="CASCADE"), index=True)
    local_track_id: Mapped[str] = mapped_column(ForeignKey("tracked_objects.id", ondelete="CASCADE"), index=True)
    camera_id: Mapped[str] = mapped_column(ForeignKey("cameras.id", ondelete="CASCADE"), index=True)
    matched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    method: Mapped[str] = mapped_column(String(80), nullable=False)
    provenance: Mapped[str] = mapped_column(String(80), nullable=False)


class ModelVersion(Base):
    __tablename__ = "model_versions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    model: Mapped[str] = mapped_column(String(120), nullable=False)
    version: Mapped[str] = mapped_column(String(80), nullable=False)
    framework: Mapped[str] = mapped_column(String(80), nullable=False)
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    deployed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Detection(Base):
    __tablename__ = "detections"
    __table_args__ = (UniqueConstraint("observation_key", name="uq_detections_observation_key"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    observation_key: Mapped[str] = mapped_column(String(180), nullable=False)
    camera_id: Mapped[str] = mapped_column(ForeignKey("cameras.id", ondelete="CASCADE"), index=True)
    object_id: Mapped[str | None] = mapped_column(ForeignKey("tracked_objects.id", ondelete="SET NULL"), index=True)
    model_version_id: Mapped[str | None] = mapped_column(ForeignKey("model_versions.id", ondelete="SET NULL"))
    frame_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    object_class: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    bbox: Mapped[list] = mapped_column(JSON, nullable=False)
    mask_ref: Mapped[str | None] = mapped_column(String(500))


class TrackPoint(Base):
    __tablename__ = "track_points"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    object_id: Mapped[str] = mapped_column(ForeignKey("tracked_objects.id", ondelete="CASCADE"), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    x: Mapped[float] = mapped_column(Float, nullable=False)
    y: Mapped[float] = mapped_column(Float, nullable=False)
    depth_m: Mapped[float | None] = mapped_column(Float)
    speed_mps: Mapped[float | None] = mapped_column(Float)
    direction_deg: Mapped[float | None] = mapped_column(Float)
    world_x_m: Mapped[float | None] = mapped_column(Float)
    world_y_m: Mapped[float | None] = mapped_column(Float)
    world_z_m: Mapped[float | None] = mapped_column(Float)


class PPEObservation(Base):
    __tablename__ = "ppe_observations"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    object_id: Mapped[str] = mapped_column(ForeignKey("tracked_objects.id", ondelete="CASCADE"), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    helmet: Mapped[bool | None] = mapped_column(Boolean)
    vest: Mapped[bool | None] = mapped_column(Boolean)
    gloves: Mapped[bool | None] = mapped_column(Boolean)
    goggles: Mapped[bool | None] = mapped_column(Boolean)
    confidences: Mapped[dict] = mapped_column(JSON, default=dict)


class BehaviourObservation(Base):
    __tablename__ = "behaviour_observations"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    object_id: Mapped[str] = mapped_column(ForeignKey("tracked_objects.id", ondelete="CASCADE"), index=True)
    camera_id: Mapped[str] = mapped_column(ForeignKey("cameras.id", ondelete="CASCADE"), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    behaviour_type: Mapped[str] = mapped_column(String(80), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    evidence_ref: Mapped[str | None] = mapped_column(String(500))


class MachineState(Base):
    __tablename__ = "machine_states"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    camera_id: Mapped[str] = mapped_column(ForeignKey("cameras.id", ondelete="CASCADE"), index=True)
    machine_id: Mapped[str] = mapped_column(String(100), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    state: Mapped[str] = mapped_column(String(80), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    anomaly_score: Mapped[float | None] = mapped_column(Float)


class SceneRelationship(Base):
    __tablename__ = "scene_relationships"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    subject_id: Mapped[str] = mapped_column(String(100), index=True)
    relation: Mapped[str] = mapped_column(String(80), nullable=False)
    object_id: Mapped[str] = mapped_column(String(100), index=True)
    camera_id: Mapped[str] = mapped_column(ForeignKey("cameras.id", ondelete="CASCADE"), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (UniqueConstraint("idempotency_key", name="uq_events_idempotency_key"), Index("ix_events_ts_severity", "timestamp", "severity"))
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(180), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(24), nullable=False, index=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(80), nullable=False)
    camera_id: Mapped[str] = mapped_column(ForeignKey("cameras.id", ondelete="CASCADE"), index=True)
    object_ids: Mapped[list] = mapped_column(JSON, default=list)
    zone_id: Mapped[str | None] = mapped_column(ForeignKey("zones.id", ondelete="SET NULL"))
    reason_codes: Mapped[list] = mapped_column(JSON, default=list)
    evidence_ref: Mapped[str | None] = mapped_column(String(500))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class Incident(Base):
    __tablename__ = "incidents"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_id: Mapped[str] = mapped_column(ForeignKey("events.id", ondelete="RESTRICT"), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(32), default="DETECTED", index=True)
    assigned_to: Mapped[str | None] = mapped_column(String(100))
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Evidence(Base):
    __tablename__ = "evidence"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    incident_id: Mapped[str] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"), index=True)
    before_clip: Mapped[str | None] = mapped_column(String(500))
    event_clip: Mapped[str | None] = mapped_column(String(500))
    after_clip: Mapped[str | None] = mapped_column(String(500))
    snapshot: Mapped[str | None] = mapped_column(String(500))
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CameraHealth(Base):
    __tablename__ = "camera_health"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    camera_id: Mapped[str] = mapped_column(ForeignKey("cameras.id", ondelete="CASCADE"), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    fps: Mapped[float | None] = mapped_column(Float)
    latency_ms: Mapped[float | None] = mapped_column(Float)
    dropped_frames: Mapped[int | None] = mapped_column(Integer)
    health_state: Mapped[str] = mapped_column(String(32), nullable=False)


class AuditLog(Base):
    __tablename__ = "audit_logs"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    actor: Mapped[str] = mapped_column(String(120), nullable=False)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    entity: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(100), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, index=True)
    metadata_json: Mapped[dict] = mapped_column("metadata", JSON, default=dict)


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    email: Mapped[str] = mapped_column(String(254), unique=True, index=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(32), default="viewer", nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
