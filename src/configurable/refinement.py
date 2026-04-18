from __future__ import annotations

import json
from typing import Any

import pandas as pd


def summarize_smallest_groups(groups: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    if groups.empty:
        return []
    return groups.sort_values("group_size").head(limit).to_dict(orient="records")


def build_refinement_suggestions(
    config: dict[str, Any],
    privacy_metrics: dict[str, Any],
    groups: pd.DataFrame,
    anonymized_df: pd.DataFrame,
) -> list[str]:
    suggestions: list[str] = []

    target_k = int(privacy_metrics["target_k"])
    max_k = int(privacy_metrics["max_k_before_suppression"])
    rows_before = int(privacy_metrics["rows_before_suppression"])
    rows_after = int(privacy_metrics["rows_after_suppression"])

    if max_k < target_k:
        suggestions.append(
            f"Target k={target_k} is not reachable before suppression because the smallest equivalence class is {max_k}. Add stronger generalization or reduce the quasi-identifier set."
        )

    if rows_after == 0:
        suggestions.append(
            "Suppression removed all rows. Lower target_k, broaden generalization mappings, or choose less specific quasi-identifiers."
        )
    elif rows_before > 0 and rows_after / rows_before < 0.8:
        suggestions.append(
            "Suppression removed more than 20% of rows. Consider stronger generalization before suppression so more records remain usable."
        )
    else:
        suggestions.append(
            "The anonymized dataset retained most rows after suppression and is suitable for downstream report generation."
        )

    for sensitive_attribute, metrics in privacy_metrics.get("sensitive_attributes", {}).items():
        min_l = metrics.get("min_l_diversity")
        max_t = metrics.get("max_t_distance")

        if min_l is not None and min_l < 2:
            suggestions.append(
                f"Sensitive attribute '{sensitive_attribute}' has l-diversity below 2 in at least one group. Consider bucketing or generalizing that attribute."
            )

        if max_t is not None and max_t > 0.3:
            suggestions.append(
                f"Sensitive attribute '{sensitive_attribute}' has some groups far from the global distribution (max t-distance {max_t:.3f}). Consider broader buckets or stronger quasi-identifier generalization."
            )

    if not config.get("generalizations"):
        suggestions.append(
            "No config-driven value generalizations are defined yet. Add mappings under 'generalizations' if you need finer control than suppression alone."
        )

    if not config.get("selection", {}).get("sensitive_attributes"):
        suggestions.append(
            "Selection.sensitive_attributes is still empty. Fill it so the config documents which attributes are treated as sensitive for the release dataset."
        )

    if not config.get("selection", {}).get("quasi_identifiers"):
        suggestions.append(
            "Selection.quasi_identifiers is empty. Choose a quasi-identifier set before the next run."
        )

    if groups.empty:
        suggestions.append(
            "No equivalence classes were computed. Verify the quasi-identifiers exist in the merged dataset and are not fully null."
        )

    return suggestions


def build_refinement_payload(
    config: dict[str, Any],
    privacy_metrics: dict[str, Any],
    groups: pd.DataFrame,
    anonymized_df: pd.DataFrame,
) -> dict[str, Any]:
    updated_config = json.loads(json.dumps(config))
    updated_config.setdefault("iteration", {})
    updated_config["iteration"]["last_run_metrics"] = privacy_metrics
    updated_config["iteration"]["smallest_equivalence_classes_preview"] = summarize_smallest_groups(groups)
    updated_config["iteration"]["rows_available_for_reports"] = int(anonymized_df.shape[0])

    return {
        "workflow_version": config.get("workflow_version", 1),
        "status": {
            "report_ready": bool(anonymized_df.shape[0] > 0),
            "rows_available_for_reports": int(anonymized_df.shape[0]),
        },
        "privacy_metrics": privacy_metrics,
        "suggestions": build_refinement_suggestions(config, privacy_metrics, groups, anonymized_df),
        "next_config": updated_config,
    }
