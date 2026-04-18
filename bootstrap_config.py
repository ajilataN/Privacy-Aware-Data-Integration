from __future__ import annotations

import argparse

from src.configurable_pipeline import (
    build_bootstrap_config,
    load_any_dataset,
    write_json,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect graduates and mobility datasets and generate an editable JSON config template.",
    )
    parser.add_argument(
        "--graduates",
        default="data/graduates",
        help="Path to graduates file or folder",
    )
    parser.add_argument(
        "--mobility",
        default="data/mobility",
        help="Path to mobility file or folder",
    )
    parser.add_argument(
        "--student-id-column",
        default="enrollment_number",
        help="Optional explicit join column used in both datasets",
    )
    parser.add_argument(
        "--output",
        default="output/config/bootstrap_config.json",
        help="Output JSON template path",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    graduates_df = load_any_dataset(args.graduates)
    mobility_df = load_any_dataset(args.mobility)

    config = build_bootstrap_config(
        graduates_df=graduates_df,
        mobility_df=mobility_df,
        graduates_path=args.graduates,
        mobility_path=args.mobility,
        join_key=args.student_id_column,
    )

    output_path = write_json(config, args.output)
    print(f"Bootstrap config written to {output_path}")


if __name__ == "__main__":
    main()
