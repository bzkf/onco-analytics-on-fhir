# Data preparation

## Prepare and Merge Zenzy Exports

1. Change into this folder: `cd analytics_on_fhir/aml`
1. Place all Zenzy export `.xlsx` files in the `./data` directory
1. Install [uv](https://docs.astral.sh/uv/)
1. Run

    ```sh
    uv run prepare_zenzy_export.py
    ```

## Prepare and Merge Cato Exports

1. Change into this folder: `cd analytics_on_fhir/aml`
1. Place all Cato export `.csv` files in the `./data` directory
1. Install [uv](https://docs.astral.sh/uv/)
1. Run

    ```sh
    uv run prepare_cato_export.py
    ```
