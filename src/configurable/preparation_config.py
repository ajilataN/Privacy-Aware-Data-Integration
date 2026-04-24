from __future__ import annotations

import pandas as pd

from src.configurable.prepared_dataset import (
    suggest_default_prepared_drop_columns,
    suggest_preparation_transformations,
)
from src.configurable.profiling import (
    SUPPORTED_AGGREGATIONS,
    possible_semantic_types,
    profile_column,
    suggest_aggregations,
    suggest_join_key,
)


def build_preparation_config(
    graduates_df: pd.DataFrame,
    mobility_df: pd.DataFrame,
    graduates_path: str,
    mobility_path: str,
    left_join_key: str | None = None,
    right_join_key: str | None = None,
) -> dict:
    if not left_join_key or not right_join_key:
        left_join_column, right_join_column = suggest_join_key(
            graduates_df,
            mobility_df,
        )
        left_join_key = left_join_key or left_join_column
        right_join_key = right_join_key or right_join_column

    preparation_transformations = suggest_preparation_transformations(graduates_df)
    drop_columns = suggest_default_prepared_drop_columns(
        graduates_df,
        left_join_key,
        right_join_key,
    )

    return {
        "workflow_version": 2,
        "workflow_stage": "preparation",
        "instructions": {
            "how_to_use": [
                "Review the raw columns first so you understand the graduates and mobility datasets before preparing the merged dataset.",
                "If a detected semantic_type is wrong, replace it with one value from possible_semantic_types. The configurable pipeline will coerce the column to that semantic type during execution.",
                "Choose mobility aggregation rules, filters, post-merge missing-value fills, and any preparation transformations that should exist before anonymization suggestions are generated.",
                "Use prepared_dataset.drop_columns to remove columns from the merged dataset, including direct identifiers you do not want to carry into the anonymization stage.",
                "Run the preparation stage with the edited JSON file. It will write the prepared merged dataset and a second JSON config for the anonymization stage.",
            ],
            "allowed_semantic_types": possible_semantic_types(),
            "allowed_aggregation_values": sorted(SUPPORTED_AGGREGATIONS),
            "sections": {
                "datasets": [
                    "Set the input paths for the graduates and mobility datasets."
                ],
                "join": [
                    "Confirm which column in the graduates dataset links to which column in the mobility dataset.",
                    "Use left_on for the graduates key column and right_on for the mobility key column. They may have different names.",
                    "The join is many-to-one: mobility will be aggregated to student level before merge."
                ],
                "columns": [
                    "Use this section to explore each column.",
                    "semantic_type is the detected semantic type for the column.",
                    "If needed, replace semantic_type with one of the values listed in possible_semantic_types.",
                    "sample_values shows all distinct values when unique_count is below 20; otherwise it shows five example values to help you understand the column."
                ],
                "mobility_aggregation": [
                    "Choose how columns from the mobility dataset should be aggregated to one row per student before the merge.",
                    "Each aggregation entry can also define a filter. Use a single value or a list of values to keep only mobility rows matching that column before aggregation.",
                    "Example: set mobility_aggregation.status.filter to [\"student\"] if only student-status mobility rows should be included in the aggregated mobility dataset."
                ],
                "prepared_dataset": [
                    "Use drop_columns to remove columns from the merged dataset before the anonymization stage. This is where you can drop direct identifiers.",
                    "Use post_merge_missing_values to fill columns that become missing for students without mobility after the left join.",
                    "Use transformations to create prepared-dataset columns, such as graduation_year, before anonymization suggestions are generated."
                ],
                "outputs": [
                    "Set where the prepared merged dataset and the anonymization-stage config should be written."
                ]
            }
        },
        "datasets": {
            "graduates_path": graduates_path,
            "mobility_path": mobility_path,
        },
        "join": {
            "left_on": left_join_key,
            "right_on": right_join_key,
            "join_type": "left",
            "left_dataset": "graduates",
            "right_dataset": "mobility",
            "many_to_one_note": "Graduates are treated as one row per student. Mobility is aggregated to student level before merge.",
        },
        "columns": {
            "graduates": [profile_column(graduates_df, column) for column in graduates_df.columns],
            "mobility": [profile_column(mobility_df, column) for column in mobility_df.columns],
        },
        "mobility_aggregation": suggest_aggregations(mobility_df, right_join_key),
        "prepared_dataset": {
            "drop_columns": drop_columns,
            "post_merge_missing_values": {
                "mobility_record_count": 0,
            },
            "transformations": preparation_transformations,
        },
        "outputs": {
            "prepared_dataset_path": "output/prepared_dataset.xlsx",
            "anonymization_config_path": "config/anonymization_config.json",
        },
    }
