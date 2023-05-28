import xml.etree.ElementTree as ET

import pytest

from decompose_xmls.decompose_xmls import decompose_sammelmeldung


@pytest.mark.parametrize(
    "adt_input_file_path", [("input-adt-reports/test-2patients.xml")]
)
def test_decompose_sammelmeldung(snapshot, adt_input_file_path):
    tree = ET.parse(adt_input_file_path)
    root = tree.getroot()

    result = decompose_sammelmeldung(root, adt_input_file_path)
    assert result == snapshot
