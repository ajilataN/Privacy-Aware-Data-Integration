from __future__ import annotations

from typing import Any

import pandas as pd

from src.configurable.profiling import SUPPORTED_AGGREGATIONS


def normalize_id_column(df: pd.DataFrame, join_key: str) -> pd.DataFrame:
    normalized = df.copy()
    normalized[join_key] = normalized[join_key].astype(str).str.strip()
    return normalized


def apply_mobility_aggregation(
    mobility_df: pd.DataFrame,
    join_key: str,
    aggregation_config: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    aggregations: dict[str, tuple[str, Any]] = {}

    for output_column, settings in aggregation_config.items():
        selected = settings.get("selected")
        source_column = settings.get("source_column", output_column)

        if selected not in SUPPORTED_AGGREGATIONS or source_column not in mobility_df.columns:
            continue

        if selected == "mode":
            aggregations[output_column] = (
                source_column,
                lambda series: series.mode().iloc[0] if not series.mode().empty else pd.NA,
            )
        else:
            pandas_agg = "nunique" if selected == "nunique" else selected
            aggregations[output_column] = (source_column, pandas_agg)

    if not aggregations:
        return mobility_df[[join_key]].drop_duplicates().copy()

    return mobility_df.groupby(join_key).agg(**aggregations).reset_index()


def apply_generalizations(
    df: pd.DataFrame,
    generalizations: dict[str, dict[str, str]],
) -> pd.DataFrame:
    generalized = df.copy()
    for column, mapping in generalizations.items():
        if column in generalized.columns and isinstance(mapping, dict):
            generalized[column] = generalized[column].replace(mapping)
    return generalized


def bucket_numeric_column(
    series: pd.Series,
    bins: list[dict[str, Any]],
    fillna_value: Any | None = None,
) -> pd.Series:
    if fillna_value is not None:
        series = series.fillna(fillna_value)

    bucketed = pd.Series(index=series.index, dtype="object")
    for item in bins:
        label = item["label"]
        minimum = item.get("min")
        maximum = item.get("max")
        mask = pd.Series(True, index=series.index)
        if minimum is not None:
            mask &= series >= minimum
        if maximum is not None:
            mask &= series <= maximum
        bucketed.loc[mask] = label

    return bucketed.fillna(series.astype(str))


def apply_transformations(
    df: pd.DataFrame,
    transformations: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    transformed = df.copy()

    for new_column, settings in transformations.items():
        source_column = settings.get("source_column", new_column)
        if source_column not in transformed.columns:
            continue

        operation = settings.get("operation")
        if operation == "bucket_numeric":
            transformed[new_column] = bucket_numeric_column(
                pd.to_numeric(transformed[source_column], errors="coerce"),
                settings.get("bins", []),
                fillna_value=settings.get("fillna_value"),
            )
        elif operation == "derive_boolean_from_numeric":
            numeric_series = pd.to_numeric(transformed[source_column], errors="coerce").fillna(
                settings.get("fillna_value", 0)
            )
            threshold = settings.get("threshold", 0)
            true_value = settings.get("true_value", 1)
            false_value = settings.get("false_value", 0)
            transformed[new_column] = numeric_series.gt(threshold).map(
                {True: true_value, False: false_value}
            )
        elif operation == "fill_missing_literal":
            transformed[new_column] = transformed[source_column].fillna(
                settings.get("value", "Unknown")
            )
        elif operation == "extract_year":
            transformed[new_column] = pd.to_datetime(
                transformed[source_column],
                errors="coerce",
            ).dt.year

    return transformed
