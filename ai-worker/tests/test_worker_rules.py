import unittest
from datetime import datetime, timezone

from worker import Observation, inside_polygon, rules


class WorkerRuleTests(unittest.TestCase):
    def sample(self, **changes):
        values = {
            "camera_id": "cam-1", "timestamp": datetime.now(timezone.utc),
            "object_id": "local-1", "class": "person", "confidence": 0.9,
            "bbox": [10, 10, 30, 50], "model_version": "test-1",
            "center": [20, 30], "zone_id": "zone-1",
            "zone_polygon": [[0, 0], [40, 0], [40, 60], [0, 60]],
            "zone_type": "restricted", "ppe": {"helmet": False, "vest": True},
            "ppe_confidences": {"helmet": 0.88},
        }
        values.update(changes)
        return Observation.model_validate(values)

    def test_point_in_polygon_boundary_context(self):
        self.assertTrue(inside_polygon((20, 20), [(0, 0), (40, 0), (40, 40), (0, 40)]))
        self.assertFalse(inside_polygon((50, 20), [(0, 0), (40, 0), (40, 40), (0, 40)]))

    def test_rules_emit_reasoned_ppe_and_zone_events(self):
        emitted = rules(self.sample())
        self.assertEqual({"missing_helmet", "restricted_zone_intrusion"}, {event["event_type"] for event in emitted})
        self.assertTrue(all(event["reason_codes"] for event in emitted))
        self.assertTrue(all(event["source"] == "scene-rules-v1" for event in emitted))

    def test_rules_do_not_flag_missing_ppe_when_provider_reports_present(self):
        emitted = rules(self.sample(ppe={"helmet": True, "vest": True}))
        self.assertNotIn("missing_helmet", {event["event_type"] for event in emitted})


if __name__ == "__main__":
    unittest.main()
