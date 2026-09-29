import unittest
from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.auth import make_token
from app.db import Base, get_db
from app.main import app
from app.models import AuditLog, Camera, Event, Factory, GlobalTrackMembership, Site, TrackPoint, TrackedObject


class StageTwoAPITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        cls.Session = sessionmaker(bind=cls.engine, expire_on_commit=False)
        Base.metadata.create_all(cls.engine)

        def override_db():
            with cls.Session() as session:
                yield session

        app.dependency_overrides[get_db] = override_db
        cls.client = TestClient(app)
        cls.ai_headers = {"Authorization": "Bearer " + make_token("test-ai", "ai_service")}
        with cls.Session() as db:
            db.add(Factory(id="f1", name="Factory", timezone="UTC"))
            db.add(Site(id="s1", factory_id="f1", name="Site"))
            db.add(Camera(id="c1", site_id="s1", name="Camera", resolution="1920x1080"))
            db.add(TrackedObject(id="t1", camera_id="c1", object_class="person", first_seen=datetime.now(timezone.utc), last_seen=datetime.now(timezone.utc)))
            db.add(TrackPoint(id="p1", object_id="t1", timestamp=datetime.now(timezone.utc), x=720, y=480, depth_m=2.0, world_x_m=12, world_y_m=8, world_z_m=0))
            db.commit()

    @classmethod
    def tearDownClass(cls):
        app.dependency_overrides.clear()
        cls.engine.dispose()

    def test_event_correlation_is_idempotent_audited_and_retains_count(self):
        timestamp = datetime.now(timezone.utc).isoformat()
        base = {"event_type": "missing_helmet", "timestamp": timestamp, "severity": "high", "confidence": 0.9, "source": "test-provider", "camera_id": "c1", "object_ids": ["t1"], "reason_codes": ["ppe_helmet_not_detected"], "payload": {}}
        first = self.client.post("/api/v1/events/ingest", headers=self.ai_headers, json={**base, "idempotency_key": "stage2-event-first"})
        self.assertEqual(first.status_code, 202, first.text)
        duplicate = self.client.post("/api/v1/events/ingest", headers=self.ai_headers, json={**base, "idempotency_key": "stage2-event-second"})
        self.assertEqual(duplicate.status_code, 202, duplicate.text)
        self.assertFalse(duplicate.json()["created"])
        incident_id = first.json()["incident_id"]
        summary = self.client.get(f"/api/v1/incidents/{incident_id}/summary", headers=self.ai_headers)
        self.assertEqual(summary.status_code, 200, summary.text)
        self.assertEqual(summary.json()["assessment"]["correlation_count"], 2)
        viewer_headers = {"Authorization": "Bearer " + make_token("test-viewer", "viewer")}
        investigation = self.client.post("/api/v1/investigations/query", headers=viewer_headers, json={"question": "Show high severity PPE events today"})
        self.assertEqual(investigation.status_code, 200, investigation.text)
        self.assertEqual(investigation.json()["events"][0]["incident_id"], incident_id)
        report = self.client.get(f"/api/v1/incidents/{incident_id}/report", headers=self.ai_headers)
        self.assertEqual(report.status_code, 200, report.text)
        self.assertIn("INTELLIWATCH", report.text)
        with self.Session() as db:
            self.assertEqual(db.query(func.count(Event.id)).scalar(), 1)
            event = db.query(Event).one()
            self.assertEqual(event.payload["correlation_count"], 2)
            self.assertEqual(db.query(AuditLog).filter_by(action="event.correlated").count(), 1)

    def test_global_identity_and_spatial_graph_are_provider_grounded(self):
        response = self.client.post("/api/v1/global-tracks/link", headers=self.ai_headers, json={"global_track_id": "g1", "local_track_id": "t1", "confidence": 0.82, "method": "provider_reid_v2", "provenance": "model:reid-2.1"})
        self.assertEqual(response.status_code, 201, response.text)
        graph = self.client.get("/api/v1/scene-graph?site_id=s1&radius_m=5&center_x_m=12&center_y_m=8", headers=self.ai_headers)
        self.assertEqual(graph.status_code, 200, graph.text)
        self.assertEqual(graph.json()["count"], 2)
        self.assertEqual({node["kind"] for node in graph.json()["nodes"]}, {"local_track", "global_identity_hypothesis"})
        self.assertEqual(graph.json()["edges"][0]["provenance"], "model:reid-2.1")
        far = self.client.get("/api/v1/scene-graph?site_id=s1&radius_m=2&center_x_m=100&center_y_m=100", headers=self.ai_headers)
        self.assertEqual(far.json()["count"], 0)

    def test_camera_calibration_is_validated_and_audited(self):
        admin = {"Authorization": "Bearer " + make_token("test-admin", "admin")}
        response = self.client.put("/api/v1/cameras/c1/calibration", headers=admin, json={"homography": [[0.01, 0, 0], [0, 0.01, 0], [0, 0, 1]], "reference_width": 1920, "reference_height": 1080, "units": "m", "ground_reference": "bbox_bottom_center"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["calibrated"])
        mismatch = self.client.put("/api/v1/cameras/c1/calibration", headers=admin, json={"homography": [[0.01, 0, 0], [0, 0.01, 0], [0, 0, 1]], "reference_width": 640, "reference_height": 480})
        self.assertEqual(mismatch.status_code, 422)
        with self.Session() as db:
            self.assertEqual(db.query(AuditLog).filter_by(action="camera.calibration_updated").count(), 1)

    def test_analytics_only_counts_persisted_observations(self):
        heatmap = self.client.get("/api/v1/analytics/heatmap?camera_id=c1", headers=self.ai_headers)
        self.assertEqual(heatmap.status_code, 200, heatmap.text)
        self.assertEqual(heatmap.json()["observation_count"], 1)
        self.assertEqual(heatmap.json()["coordinate_frame"], "normalized_camera_image")

    def test_both_frontend_origins_pass_auth_preflight(self):
        for origin in ("http://localhost:5173", "http://localhost:5174"):
            response = self.client.options("/api/v1/auth/token", headers={"Origin": origin, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type"})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers.get("access-control-allow-origin"), origin)


if __name__ == "__main__":
    unittest.main()
