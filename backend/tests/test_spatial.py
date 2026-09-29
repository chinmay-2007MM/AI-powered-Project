import unittest

from app.schemas import CameraCalibration
from app.spatial import project_to_floor
from pydantic import ValidationError


class SpatialCalibrationTests(unittest.TestCase):
    def test_image_to_floor_homography(self):
        config = {"homography": [[0.1, 0, 0], [0, 0.1, 0], [0, 0, 1]]}
        self.assertEqual(project_to_floor(50, 20, config), (5.0, 2.0))

    def test_calibration_rejects_singular_matrix(self):
        with self.assertRaises(ValidationError):
            CameraCalibration(homography=[[1, 2, 3], [2, 4, 6], [0, 0, 0]], reference_width=1920, reference_height=1080)

    def test_calibration_rejects_non_finite_matrix(self):
        with self.assertRaises(ValidationError):
            CameraCalibration(homography=[[1, 0, 0], [0, float("nan"), 0], [0, 0, 1]], reference_width=1920, reference_height=1080)


if __name__ == "__main__":
    unittest.main()
