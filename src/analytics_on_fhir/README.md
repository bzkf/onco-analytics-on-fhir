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

### Test

```sh
uv run pytest -vv --log-cli-level=20 --cov=analytics_on_fhir --cov-report=html --capture=no
```
