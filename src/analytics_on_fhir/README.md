# analytics_on_fhir

Collection of analytics scripts.

## Development

### Prerequisites

* uv: <https://docs.astral.sh/uv/getting-started/installation/>
* Java JDK 21
* graphviz

### Install

```sh
uv sync
```

### Run in VS Code interactive mode
From the project root src/analytics_on_fhir, run:
```sh
uv pip install -e .
```
This installs the project in editable mode so all local packages
(e.g. analytics_on_fhir) can be imported without modifying PYTHONPATH.

### Test

```sh
uv run pytest -vv --log-cli-level=20 --cov=analytics_on_fhir --cov-report=html --capture=no
```
