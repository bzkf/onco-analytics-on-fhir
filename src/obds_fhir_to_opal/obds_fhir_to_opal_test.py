import datetime

import pytest

from .obds_fhir_to_opal import calculate_age_at_conditiondate


@pytest.mark.parametrize(
    "birthday,conditiondate,expected", [
        (datetime.date(1978, 1, 1), datetime.date(2024, 1, 1), 46),
        (datetime.date(1978, 1, 1), datetime.date(2024, 1, 2), 46),
        (datetime.date(1978, 4, 2), datetime.date(2024, 1, 1), 45),
        (datetime.date(1978, 1, 1), datetime.date(2024, 12, 31), 46),
    ]
)
def test_should_return_correct_age_at_conditiondate(birthday, conditiondate, expected):
    assert calculate_age_at_conditiondate(birthday, conditiondate) == expected

