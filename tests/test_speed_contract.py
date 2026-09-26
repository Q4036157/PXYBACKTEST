import pytest
from pydantic import ValidationError

from app.default_profiles import default_profile_catalog
from app.models import ExecutionModelV2, SetSpeedRequest


def test_visual_speed_defaults_to_three_and_accepts_half_speed() -> None:
    assert ExecutionModelV2().speed == 3
    assert SetSpeedRequest(speed=0.5).speed == 0.5
    assert SetSpeedRequest(speed=100).speed == 100
    with pytest.raises(ValidationError):
        SetSpeedRequest(speed=0.49)
    with pytest.raises(ValidationError):
        SetSpeedRequest(speed=100.01)


def test_recommended_profiles_use_three_x() -> None:
    assert all(
        profile["defaults"]["execution"]["speed"] == 3
        for profile in default_profile_catalog()
    )
