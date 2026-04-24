from __future__ import annotations

import argparse
from pathlib import Path

from src.configurable_pipeline import (
    ensure_parent_dir,
    load_json,
    prepare_dataset_for_anonymization,
    write_json,
    write_tabular_dataset,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the preparation stage and generate the prepared merged dataset plus an anonymization config.",
    )
    parser.add_argument(
        "--config",
        default="output/config/bootstrap_config.json",
        help="Path to the edited preparation-stage JSON config file",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = load_json(args.config)
    outputs = prepare_dataset_for_anonymization(config)

    prepared_dataset_path = config["outputs"]["prepared_dataset_path"]
    anonymization_config_path = config["outputs"]["anonymization_config_path"]

    ensure_parent_dir(Path(prepared_dataset_path))
    write_tabular_dataset(outputs.prepared_df, prepared_dataset_path)
    write_json(outputs.anonymization_config, anonymization_config_path)

    print(f"Prepared dataset written to {prepared_dataset_path}")
    print(f"Anonymization config written to {anonymization_config_path}")


if __name__ == "__main__":
    main()
