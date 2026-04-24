from __future__ import annotations

import argparse

from src.configurable_pipeline import (
    build_bootstrap_config,
    load_any_dataset,
    write_json,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect graduates and mobility datasets and generate an editable preparation-stage JSON config.",
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
        "--left-join-column",
        default="enrollment_number",
        help="Join column for the graduates dataset",
    )
    parser.add_argument(
        "--right-join-column",
        default="enrollment_number",
        help="Join column for the mobility dataset",
    )
    parser.add_argument(
        "--output",
        default="output/config/bootstrap_config.json",
        help="Output preparation-stage JSON template path",
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
        left_join_key=args.left_join_column,
        right_join_key=args.right_join_column,
    )

    output_path = write_json(config, args.output)
    print(f"Bootstrap config written to {output_path}")


if __name__ == "__main__":
    main()
