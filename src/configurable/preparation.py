from __future__ import annotations

from typing import Any

import pandas as pd

from src.configurable.models import PreparationOutputs
from src.configurable.profiling import (
    possible_semantic_types,
    profile_column,
    suggest_generalization_candidates,
    suggest_qi_candidates,
)
from src.configurable.transformations import (
    apply_configured_semantic_types,
    apply_missing_value_fills,
    apply_mobility_aggregation,
    apply_transformations,
    keep_configured_aggregated_columns,
    normalize_id_column,
)
from src.loader import load_any_dataset


def suggest_default_prepared_drop_columns(
    graduates_df: pd.DataFrame,
    left_join_key: str | None,
    right_join_key: str | None,
) -> list[str]:
    candidates = [
        left_join_key,
        right_join_key if right_join_key != left_join_key else None,
        "first_name",
        "last_name",
        "source_file",
        "source_year",
    ]

    return [
        column
        for column in dict.fromkeys(candidates)
        if column and (column in graduates_df.columns or column == right_join_key)
    ]


def suggest_preparation_transformations(graduates_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    transformations: dict[str, dict[str, Any]] = {}

    if "graduation_date" in graduates_df.columns:
        transformations["graduation_year"] = {
            "operation": "extract_year",
            "source_column": "graduation_date",
        }

    transformations["mobility_count_bucket"] = {
        "operation": "bucket_numeric",
        "source_column": "mobility_record_count",
        "fillna_value": 0,
        "bins": [
            {"label": "0", "min": 0, "max": 0},
            {"label": "1", "min": 1, "max": 1},
            {"label": "2+", "min": 2},
        ],
    }
    transformations["had_mobility"] = {
        "operation": "derive_boolean_from_numeric",
        "source_column": "mobility_record_count",
        "fillna_value": 0,
        "threshold": 0,
        "true_value": 1,
        "false_value": 0,
    }

    return transformations


def build_anonymization_config(
    prepared_df: pd.DataFrame,
    prepared_dataset_path: str,
) -> dict[str, Any]:
    return {
        "workflow_version": 2,
        "workflow_stage": "anonymization",
        "instructions": {
            "how_to_use": [
                "Review the prepared dataset columns. The suggestions in this file are based on the merged dataset after aggregation, missing-value handling, drops, and preparation transformations.",
                "Choose any additional drop_columns, then select quasi_identifiers and sensitive_attributes for the anonymization run.",
                "Use suggestions.qi_candidates as starting points and generalization_candidates to see full distinct values for likely generalizable columns.",
                "Run the anonymization stage with this edited JSON file. If needed, use the refinement output for later iterations.",
            ],
            "allowed_semantic_types": possible_semantic_types(),
            "sections": {
                "prepared_dataset": [
                    "Confirm the path of the prepared merged dataset produced in the preparation stage."
                ],
                "columns": [
                    "Use this section to explore the prepared merged dataset.",
                    "semantic_type is the detected semantic type for each prepared column.",
                    "If needed, replace semantic_type with one of the values listed in possible_semantic_types before running anonymization."
                ],
                "selection": [
                    "Choose any additional drop_columns, then define quasi_identifiers and sensitive_attributes on the prepared dataset."
                ],
                "suggestions": [
                    "qi_candidates are suggested on the prepared dataset, so they are more relevant than raw-dataset suggestions.",
                    "generalization_candidates list distinct values for likely generalizable prepared columns."
                ],
                "transformations": [
                    "Optional: define any extra prepared-dataset transformations needed before anonymization."
                ],
                "generalizations": [
                    "Add value-mapping rules that generalize prepared columns before privacy checks."
                ],
                "privacy_targets": [
                    "Set the target k value and choose which sensitive attributes should be checked for l-diversity and t-closeness."
                ],
                "outputs": [
                    "Set where the anonymized dataset, privacy metrics, and refinement files should be written."
                ],
            },
        },
        "prepared_dataset": {
            "path": prepared_dataset_path,
        },
        "columns": [profile_column(prepared_df, column) for column in prepared_df.columns],
        "selection": {
            "drop_columns": [],
            "quasi_identifiers": [],
            "sensitive_attributes": [],
            "report_columns": [],
        },
        "suggestions": {
            "qi_candidates": suggest_qi_candidates(prepared_df, None),
            "generalization_candidates": suggest_generalization_candidates(prepared_df),
        },
        "transformations": {},
        "generalizations": {},
        "privacy_targets": {
            "target_k": 3,
            "sensitive_attributes_for_checks": [],
        },
        "outputs": {
            "anonymized_dataset_path": "output/configurable/anonymized_dataset.xlsx",
            "privacy_metrics_path": "output/configurable/privacy_metrics.json",
            "refinement_path": "output/configurable/refinement_iteration.json",
        },
    }


def prepare_dataset_for_anonymization(config: dict[str, Any]) -> PreparationOutputs:
    graduates_df = load_any_dataset(config["datasets"]["graduates_path"])
    mobility_df = load_any_dataset(config["datasets"]["mobility_path"])
    columns_config = config.get("columns", {})

    graduates_df = apply_configured_semantic_types(
        graduates_df,
        columns_config.get("graduates"),
    )
    mobility_df = apply_configured_semantic_types(
        mobility_df,
        columns_config.get("mobility"),
    )

    join_config = config.get("join", {})
    left_join_key = join_config.get("left_on")
    right_join_key = join_config.get("right_on")
    if not left_join_key or not right_join_key:
        raise ValueError("Config must define join.left_on and join.right_on.")

    graduates_df = normalize_id_column(graduates_df, left_join_key)
    mobility_df = normalize_id_column(mobility_df, right_join_key)

    aggregated_mobility_df = apply_mobility_aggregation(
        mobility_df,
        join_key=right_join_key,
        aggregation_config=config.get("mobility_aggregation", {}),
    )
    aggregated_mobility_df = keep_configured_aggregated_columns(
        aggregated_mobility_df,
        join_key=right_join_key,
        aggregation_config=config.get("mobility_aggregation", {}),
    )

    prepared_df = graduates_df.merge(
        aggregated_mobility_df,
        left_on=left_join_key,
        right_on=right_join_key,
        how=join_config.get("join_type", "left"),
    )

    prepared_settings = config.get("prepared_dataset", {})
    prepared_df = apply_missing_value_fills(
        prepared_df,
        prepared_settings.get("post_merge_missing_values", {}),
    )
    prepared_df = apply_transformations(
        prepared_df,
        prepared_settings.get("transformations", {}),
    )
    prepared_df = prepared_df.drop(
        columns=[
            column
            for column in prepared_settings.get("drop_columns", [])
            if column in prepared_df.columns
        ],
        errors="ignore",
    )

    anonymization_config = build_anonymization_config(
        prepared_df=prepared_df,
        prepared_dataset_path=config["outputs"]["prepared_dataset_path"],
    )

    return PreparationOutputs(
        prepared_df=prepared_df,
        anonymization_config=anonymization_config,
    )
