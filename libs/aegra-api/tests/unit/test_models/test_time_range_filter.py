from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from aegra_api.core.orm import Run as RunORM
from aegra_api.models.filters import TimeRange
from aegra_api.models.runs import RunSearchRequest

MORNING = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
EVENING = datetime(2026, 8, 2, 0, 0, tzinfo=UTC)


def _operators(range_: TimeRange) -> list[str]:
    return [str(p).split(" :", 1)[0] for p in range_.predicates(RunORM.created_at)]


def test_a_half_open_day_is_two_predicates() -> None:
    assert _operators(TimeRange(gte=MORNING, lt=EVENING)) == [
        "runs.created_at >=",
        "runs.created_at <",
    ]


@pytest.mark.parametrize(
    ("range_", "operator"),
    [
        (TimeRange(gte=MORNING), ">="),
        (TimeRange(gt=MORNING), ">"),
        (TimeRange(lte=MORNING), "<="),
        (TimeRange(lt=MORNING), "<"),
    ],
)
def test_each_operator_maps_to_its_comparison(range_: TimeRange, operator: str) -> None:
    assert _operators(range_) == [f"runs.created_at {operator}"]


def test_an_empty_range_is_rejected() -> None:
    with pytest.raises(ValidationError, match="at least one"):
        TimeRange()


@pytest.mark.parametrize("bounds", [{"gte": MORNING, "gt": MORNING}, {"lte": EVENING, "lt": EVENING}])
def test_two_bounds_on_the_same_side_are_rejected(bounds: dict[str, datetime]) -> None:
    with pytest.raises(ValidationError, match="mutually exclusive"):
        TimeRange(**bounds)


def test_an_unknown_operator_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TimeRange.model_validate({"after": MORNING.isoformat()})


def test_search_accepts_the_range_and_leaves_it_optional() -> None:
    parsed = RunSearchRequest.model_validate({"created_at": {"gte": MORNING.isoformat()}})

    assert parsed.created_at == TimeRange(gte=MORNING)
    assert RunSearchRequest().created_at is None
