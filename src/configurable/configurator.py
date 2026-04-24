from __future__ import annotations

from typing import Any

from src.anonymization import (
    compute_equivalence_classes,
    compute_l_diversity,
    compute_max_k,
    compute_t_closeness,
    suppress_small_equivalence_classes,
)
from src.configurable.models import PipelineOutputs
from src.configurable.refinement import build_refinement_payload
from src.configurable.transformations import (
    apply_configured_semantic_types,
    apply_generalizations,
    apply_transformations,
)
from src.loader import load_any_dataset


def run_configured_pipeline(config: dict[str, Any]) -> PipelineOutputs:
    prepared_dataset_config = config.get("prepared_dataset", {})
    prepared_dataset_path = prepared_dataset_config.get("path")
    if not prepared_dataset_path:
        raise ValueError("Config must define prepared_dataset.path.")

    prepared_df = load_any_dataset(prepared_dataset_path)
    prepared_df = apply_configured_semantic_types(prepared_df, config.get("columns"))
    transformed_df = apply_transformations(prepared_df, config.get("transformations", {}))
    transformed_df = apply_generalizations(transformed_df, config.get("generalizations", {}))

    selection = config.get("selection", {})
    removable_columns = selection.get("drop_columns", [])
    anonymization_input = transformed_df.drop(
        columns=[column for column in removable_columns if column in transformed_df.columns],
        errors="ignore",
    )

    quasi_identifiers = selection.get("quasi_identifiers", [])
    if not quasi_identifiers:
        raise ValueError("Config must define at least one quasi identifier")

    groups = compute_equivalence_classes(anonymization_input, quasi_identifiers)
    max_k = compute_max_k(groups)
    target_k = int(config.get("privacy_targets", {}).get("target_k", 1))

    anonymized_df = suppress_small_equivalence_classes(
        anonymization_input,
        quasi_identifiers=quasi_identifiers,
        min_k=target_k,
    )

    privacy_metrics: dict[str, Any] = {
        "rows_before_suppression": int(anonymization_input.shape[0]),
        "rows_after_suppression": int(anonymized_df.shape[0]),
        "equivalence_class_count": int(groups.shape[0]),
        "max_k_before_suppression": int(max_k),
        "quasi_identifiers": quasi_identifiers,
        "target_k": target_k,
    }

    sensitive_attributes = config.get("privacy_targets", {}).get("sensitive_attributes_for_checks", [])
    sensitive_metrics: dict[str, Any] = {}
    for sensitive_attribute in sensitive_attributes:
        if sensitive_attribute not in anonymized_df.columns:
            continue

        l_groups = compute_l_diversity(anonymized_df, quasi_identifiers, sensitive_attribute)
        t_groups = compute_t_closeness(anonymized_df, quasi_identifiers, sensitive_attribute)
        sensitive_metrics[sensitive_attribute] = {
            "min_l_diversity": None if l_groups.empty else int(l_groups["l_diversity"].min()),
            "mean_l_diversity": None if l_groups.empty else float(l_groups["l_diversity"].mean()),
            "max_t_distance": None if t_groups.empty else float(t_groups["t_distance"].max()),
            "mean_t_distance": None if t_groups.empty else float(t_groups["t_distance"].mean()),
        }

    privacy_metrics["sensitive_attributes"] = sensitive_metrics

    return PipelineOutputs(
        anonymized_df=anonymized_df,
        privacy_metrics=privacy_metrics,
        refinement_payload=build_refinement_payload(
            config=config,
            privacy_metrics=privacy_metrics,
            groups=groups,
            anonymized_df=anonymized_df,
        ),
    )
