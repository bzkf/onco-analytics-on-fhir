import os
import xml.etree.ElementTree as ET

import pytest

from .decompose_xmls import decompose_sammelmeldung


@pytest.mark.parametrize(
    "obds_input_file_path", [("input-obds-reports/test-2patients.xml")]
)
def test_decompose_sammelmeldung(snapshot, obds_input_file_path):
    os.environ['REMOVE_LEADING_PATIENTID_ZEROS'] = 'false'
    tree = ET.parse(obds_input_file_path)
    root = tree.getroot()

    result = decompose_sammelmeldung(root, obds_input_file_path)
    assert result == snapshot


@pytest.mark.parametrize(
    "obds_input_file_path", [("input-obds-reports/test-patientid-with-zeros.xml")]
)
def test_decompose_with_pathient_id_starting_with_zero(snapshot, obds_input_file_path):
    os.environ['REMOVE_LEADING_PATIENTID_ZEROS'] = 'true'
    tree = ET.parse(obds_input_file_path)
    root = tree.getroot()

    result = decompose_sammelmeldung(root, obds_input_file_path)
    assert result == snapshot


@pytest.mark.parametrize(
    "obds_input_file_path", [("input-obds-reports/test-patientid-with-zeros.xml")]
)
def test_decompose_keep_pathient_id_starting_with_zero(snapshot, obds_input_file_path):
    os.environ['REMOVE_LEADING_PATIENTID_ZEROS'] = 'false'

    tree = ET.parse(obds_input_file_path)
    root = tree.getroot()

    result = decompose_sammelmeldung(root, obds_input_file_path)
    assert result == snapshot
