"""Small, explicit camera image-to-floor-plane projection helpers."""
from math import isfinite


def project_to_floor(pixel_x: float, pixel_y: float, calibration: dict) -> tuple[float, float] | None:
    """Project an image point with a configured 3x3 image-to-metre homography."""
    matrix = calibration.get("homography") if calibration else None
    if not matrix or len(matrix) != 3 or any(len(row) != 3 for row in matrix):
        return None
    denominator = matrix[2][0] * pixel_x + matrix[2][1] * pixel_y + matrix[2][2]
    if not isfinite(denominator) or abs(denominator) < 1e-12:
        return None
    x = (matrix[0][0] * pixel_x + matrix[0][1] * pixel_y + matrix[0][2]) / denominator
    y = (matrix[1][0] * pixel_x + matrix[1][1] * pixel_y + matrix[1][2]) / denominator
    if not isfinite(x) or not isfinite(y):
        return None
    return x, y
