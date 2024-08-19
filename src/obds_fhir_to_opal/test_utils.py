import pytest

from .utils_onco_analytics import group_entities, map_gender, map_icd10


# test functions separately
@pytest.mark.parametrize(
    "icd10_code, expected",
    [
        ("C31.8", "3" + "31.8"),  # C -> 3
        ("C50.9", "3" + "50.9"),  # C -> 3
        ("D06.4", "4" + "06.4"),  # D -> 4
        ("A00.0", "1" + "00.0"),  # A -> 1
        ("Z99.9", "26" + "99.9"),  # Z -> 26
        ("B12.5", "2" + "12.5"),  # B -> 2
        ("F13.2", "6" + "13.2"),  # F -> 6
        ("X99.0", "24" + "99.0"),  # X -> 24
    ],
)
def test_map_icd10(icd10_code, expected):
    result = map_icd10(icd10_code)
    assert result == expected


@pytest.mark.parametrize(
    "gender, expected",
    [
        (None, 0),
        ("", 0),
        ("female", 1),
        ("weiblich", 1),
        ("male", 2),
        ("männlich", 2),
        ("other", 3),
        ("diverse", 3),
        ("non-binary", 3),
    ],
)
def test_map_gender(gender, expected):
    result = map_gender(gender)
    assert result == expected


""" icd10_mapped = float(icd10_mapped)
    ranges = [
        (300, 315, 0),  # Lippe, Mundhöhle und Rachen (C00-C14)
        (315, 316, 1),  # Speiseröhre (C15)
        (316, 317, 2),  # Magen (C16)
        (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        (322, 323, 4),  # Leber (C22)
        (323, 325, 5),  # Gallenblase und Gallenwege (C23-C24)
        (325, 326, 6),  # Bauchspeicheldrüse (C25)
        (332, 333, 7),  # Kehlkopf (C32)
        (333, 335, 8),  # Trachea, Bronchien und Lunge (C33-C34)
        (343, 344, 9),  # Malignes Melanom der Haut (C43)
        (350, 351, 10),  # Brust (C50, D05)
        (405, 406, 10),
        (353, 354, 11),  # Gebärmutterhals (C53, D06)
        (406, 407, 11),
        (354, 356, 12),  # Gebärmutterkörper (C54-C55)
        (356, 357, 13),  # Eierstöcke (C56, D39.1)
        (439.1, 439.2, 13),
        (361, 362, 14),  # Prostata (C61)
        (362, 363, 15),  # Hoden (C62)
        (364, 365, 16),  # Niere (C64)
        (367, 368, 17),  # Harnblase (C67, D09.0, D41.4)
        (409.0, 409.1, 17),
        (441.4, 441.5, 17),
        (370, 373, 18),  # Gehirn und zentrales Nervensystem (C70-C72)
        (373, 374, 19),  # Schilddrüse (C73)
        (381, 382, 20),  # Morbus Hodgkin (C81)
        (382, 389, 21),  # Non-Hodgkin-Lymphome (C82-C88, C96)
        (396, 397, 21),
        (390, 391, 22),  # Plasmozytom (C90)
        (391, 396, 23),  # Leukämien (C91-C95)
    ]

    for start, end, group in ranges:
        if start <= icd10_code_mapped < end:
            return group

    return -100 """


@pytest.mark.parametrize(
    "icd10_mapped, expected",
    [
        ("309", 0),  # (300, 315, 0),  # Lippe, Mundhöhle und Rachen (C00-C14)
        ("409.0", 17),  # (367, 368, 17),(409.0, 409.1, 17), (441.4, 441.5, 17),
        ("441.4", 17),  # Harnblase (C67, D09.0, D41.4)
        ("316", 2),  # (316, 317, 2),  # Magen (C16)
        ("354", 12),  # (354, 356, 12),  # Gebärmutterkörper (C54-C55)
        ("318", 3),  # (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        ("319", 3),  # (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        ("320", 3),  # (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        ("321", 3),  # (318, 322, 3),  # Dickdarm und Rektum (C18-C21)
        ("395", 23),  # (391, 396, 23),  # Leukämien (C91-C95)
    ],
)
def test_group_entities(icd10_mapped, expected):
    result = group_entities(icd10_mapped)
    assert result == expected
