from __future__ import annotations

import pandas as pd

from src.configurable.profiling import (
    SUPPORTED_AGGREGATIONS,
    possible_user_values_for_type,
    profile_column,
    suggest_aggregations,
    suggest_join_key,
    suggest_qi_candidates,
)


def build_bootstrap_config(
    graduates_df: pd.DataFrame,
    mobility_df: pd.DataFrame,
    graduates_path: str,
    mobility_path: str,
    join_key: str | None,
) -> dict:
    resolved_join_key = suggest_join_key(graduates_df, mobility_df, join_key)

    return {
        "workflow_version": 1,
        "instructions": {
            "how_to_use": [
                "Review inferred column types and update them where needed.",
                "Choose direct identifiers, quasi identifiers, and sensitive attributes.",
                "Select mobility aggregation rules for columns that should survive the many-to-one merge.",
                "Optionally add value mappings under generalizations.",
                "Then run the configurable pipeline with this JSON file.",
            ],
            "allowed_column_types": possible_user_values_for_type(),
            "allowed_aggregation_values": sorted(SUPPORTED_AGGREGATIONS),
        },
        "datasets": {
            "graduates_path": graduates_path,
            "mobility_path": mobility_path,
        },
        "join": {
            "student_id_column": resolved_join_key,
            "join_type": "left",
            "left_dataset": "graduates",
            "right_dataset": "mobility",
            "many_to_one_note": "Graduates are treated as one row per student. Mobility is aggregated to student level before merge.",
        },
        "columns": {
            "graduates": [profile_column(graduates_df, column) for column in graduates_df.columns],
            "mobility": [profile_column(mobility_df, column) for column in mobility_df.columns],
        },
        "mobility_aggregation": suggest_aggregations(mobility_df, resolved_join_key),
        "selection": {
            "direct_identifiers": [],
            "drop_columns": ["source_file"],
            "quasi_identifiers": [],
            "sensitive_attributes": [],
            "report_columns": [],
        },
        "suggestions": {
            "qi_candidates": suggest_qi_candidates(graduates_df, resolved_join_key),
        },
        "generalizations": {},
        "privacy_targets": {
            "target_k": 3,
            "sensitive_attributes_for_checks": [],
        },
        "outputs": {
            "merged_dataset_path": "output/configurable/merged_dataset.xlsx",
            "anonymized_dataset_path": "output/configurable/anonymized_dataset.xlsx",
            "privacy_metrics_path": "output/configurable/privacy_metrics.json",
            "refinement_path": "output/configurable/refinement_iteration.json",
        },
    }
