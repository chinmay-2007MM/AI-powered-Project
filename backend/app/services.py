import json
from uuid import uuid4
from redis import Redis
from sqlalchemy.orm import Session
from .config import settings
from .models import Event, Incident, AuditLog, Camera, Zone
from datetime import timedelta


def publish(channel: str, payload: dict) -> None:
    try:
        Redis.from_url(settings.redis_url, socket_connect_timeout=1).publish(channel, json.dumps(payload, default=str))
    except Exception:
        # Persistence is authoritative; realtime publication failure is exposed by health.
        pass


def ensure_camera(db: Session, camera_id: str) -> Camera:
    camera = db.get(Camera, camera_id)
    if not camera:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Camera not found")
    return camera


def create_event(db: Session, data: dict) -> tuple[Event, Incident, bool]:
    data = {key: value for key, value in data.items() if key != "schema_version"}
    existing = db.query(Event).filter_by(idempotency_key=data["idempotency_key"]).first()
    if existing:
        incident = db.query(Incident).filter_by(event_id=existing.id).first()
        return existing, incident, False
    # Coalesce bursts with the same explainable scene fingerprint. Keep the
    # original event/incident and retain aggregate counts in its payload.
    object_ids = set(data.get("object_ids") or [])
    if object_ids:
        floor = data["timestamp"] - timedelta(seconds=settings.event_correlation_window_seconds)
        recent = db.query(Event).filter(
            Event.camera_id == data["camera_id"],
            Event.event_type == data["event_type"],
            Event.severity == data["severity"],
            Event.zone_id == data.get("zone_id"),
            Event.timestamp >= floor,
            Event.timestamp <= data["timestamp"],
        ).order_by(Event.timestamp.desc()).limit(settings.event_correlation_max_candidates).all()
        for prior in recent:
            if not object_ids.intersection(prior.object_ids or []) or set(prior.reason_codes or []) != set(data.get("reason_codes") or []):
                continue
            prior.payload = {**(prior.payload or {}), "correlation_count": int((prior.payload or {}).get("correlation_count", 1)) + 1,
                             "last_correlated_at": data["timestamp"].isoformat(),
                             "correlated_reason_codes": sorted(set((prior.payload or {}).get("correlated_reason_codes", prior.reason_codes or [])) | set(data.get("reason_codes") or []))}
            db.add(AuditLog(id=str(uuid4()), actor="ai-pipeline", action="event.correlated", entity="event", entity_id=prior.id,
                            metadata_json={"window_seconds": settings.event_correlation_window_seconds, "source": data.get("source"), "reason_codes": data.get("reason_codes", [])}))
            db.commit(); db.refresh(prior)
            incident = db.query(Incident).filter_by(event_id=prior.id).first()
            publish("events", {"event_id": prior.id, "correlated": True, "correlation_count": prior.payload["correlation_count"]})
            return prior, incident, False
    camera = ensure_camera(db, data["camera_id"])
    if data.get("zone_id"):
        zone = db.get(Zone, data["zone_id"])
        if not zone:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Zone not found")
        if zone.site_id != camera.site_id or (zone.camera_id and zone.camera_id != camera.id):
            from fastapi import HTTPException
            raise HTTPException(status_code=422, detail="Event zone must belong to its camera's site")
    event = Event(id=str(uuid4()), **data)
    db.add(event)
    db.flush()
    incident = Incident(id=str(uuid4()), event_id=event.id)
    db.add(incident)
    db.add(AuditLog(id=str(uuid4()), actor="ai-pipeline", action="event.created", entity="incident", entity_id=incident.id, metadata_json={"event_type": event.event_type, "reason_codes": event.reason_codes}))
    db.commit()
    db.refresh(event)
    db.refresh(incident)
    publish("events", {"event_id": event.id, "incident_id": incident.id, "event_type": event.event_type, "severity": event.severity, "timestamp": event.timestamp, "camera_id": event.camera_id})
    publish("incidents", {"incident_id": incident.id, "status": incident.status, "event_id": event.id})
    return event, incident, True
