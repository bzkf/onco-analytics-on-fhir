#!/bin/bash
pytest -v test_utils_onco_analytics.py
if [ $? -eq 0 ]; then
  echo "Tests erfolgreich, starte obds_fhir_to_opal.py..."
  python obds_fhir_to_opal.py
else
  echo "Tests fehlgeschlagen, breche ab."
  exit 1
fi
