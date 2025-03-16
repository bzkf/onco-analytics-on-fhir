#!/bin/bash
set -e
if pytest -v test_utils_onco_analytics.py; then
  echo "Tests erfolgreich, starte obds_fhir_to_opal.py..."
  python obds_fhir_to_opal.py
else
  echo "Tests fehlgeschlagen, breche ab."
  exit 1
fi
