from __future__ import annotations

import argparse
from pathlib import Path

from src.configurable_pipeline import ensure_parent_dir, load_json, run_configured_pipeline, write_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the configurable anonymization pipeline from a JSON config.",
    )
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--config",
        help="Path to the edited JSON config file",
    )
    input_group.add_argument(
        "--refinement",
        help="Path to a refinement_iteration.json file. The runner will use its next_config section.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.config:
        config = load_json(args.config)
    else:
        refinement_payload = load_json(args.refinement)
        config = refinement_payload.get("next_config")
        if not config:
            raise ValueError(
                "The provided refinement file does not contain a next_config section."
            )

    outputs = run_configured_pipeline(config)

    merged_path = config["outputs"]["merged_dataset_path"]
    anonymized_path = config["outputs"]["anonymized_dataset_path"]
    metrics_path = config["outputs"]["privacy_metrics_path"]
    refinement_path = config["outputs"].get(
        "refinement_path",
        "output/configurable/refinement_iteration.json",
    )

    ensure_parent_dir(Path(merged_path))
    ensure_parent_dir(Path(anonymized_path))

    outputs.merged_df.to_excel(merged_path, index=False)
    outputs.anonymized_df.to_excel(anonymized_path, index=False)
    write_json(outputs.privacy_metrics, metrics_path)
    write_json(outputs.refinement_payload, refinement_path)

    print(f"Merged dataset written to {merged_path}")
    print(f"Anonymized dataset written to {anonymized_path}")
    print(f"Privacy metrics written to {metrics_path}")
    print(f"Refinement output written to {refinement_path}")


if __name__ == "__main__":
    main()
