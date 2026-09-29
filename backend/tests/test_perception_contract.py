import unittest
from datetime import datetime, timezone

from pydantic import ValidationError
from app.schemas import AIResult


class PerceptionContractTests(unittest.TestCase):
    def payload(self):
        return {
            "observation_key": "camera-frame-object-001", "camera_id": "camera-1",
            "timestamp": datetime.now(timezone.utc).isoformat(), "class": "person",
            "confidence": 0.9, "bbox": [10, 12, 60, 90], "model_version": "provider-1",
        }

    def test_accepts_timezone_aware_positive_bbox_and_depth(self):
        parsed = AIResult.model_validate({**self.payload(), "depth_m": 2.5, "world_position_m": [1, 2, 3]})
        self.assertEqual(parsed.world_position_m, (1.0, 2.0, 3.0))

    def test_rejects_inverted_bbox(self):
        with self.assertRaises(ValidationError):
            AIResult.model_validate({**self.payload(), "bbox": [60, 90, 10, 12]})

    def test_rejects_naive_timestamp(self):
        with self.assertRaises(ValidationError):
            AIResult.model_validate({**self.payload(), "timestamp": "2026-09-28T10:00:00"})


if __name__ == "__main__":
    unittest.main()
