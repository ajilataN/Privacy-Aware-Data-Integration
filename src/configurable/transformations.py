from __future__ import annotations

from typing import Any

import pandas as pd

from src.configurable.profiling import SUPPORTED_AGGREGATIONS


def normalize_id_column(df: pd.DataFrame, join_key: str) -> pd.DataFrame:
    normalized = df.copy()
    normalized[join_key] = normalized[join_key].astype(str).str.strip()
    return normalized


def coerce_series_to_semantic_type(series: pd.Series, semantic_type: str) -> pd.Series:
    if semantic_type == "numeric":
        return pd.to_numeric(series, errors="coerce")

    if semantic_type == "datetime":
        return pd.to_datetime(series, errors="coerce")

    if semantic_type == "boolean":
        lowered = series.astype("string").str.strip().str.lower()
        mapping = {
            "true": True,
            "false": False,
            "yes": True,
            "no": False,
            "y": True,
            "n": False,
            "1": True,
            "0": False,
        }
        return lowered.map(mapping).astype("boolean")

    if semantic_type in {"categorical", "identifier_like", "free_text"}:
        return series.astype("string").str.strip()

    return series


def apply_configured_semantic_types(
    df: pd.DataFrame,
    column_settings: list[dict[str, Any]] | None,
) -> pd.DataFrame:
    if not column_settings:
        return df

    transformed = df.copy()
    for settings in column_settings:
        column_name = settings.get("name")
        semantic_type = settings.get("semantic_type")
        if not column_name or not semantic_type or column_name not in transformed.columns:
            continue

        transformed[column_name] = coerce_series_to_semantic_type(
            transformed[column_name],
            semantic_type,
        )

    return transformed


def apply_mobility_aggregation(
    mobility_df: pd.DataFrame,
    join_key: str,
    aggregation_config: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    filtered_df = mobility_df.copy()
    aggregations: dict[str, tuple[str, Any]] = {}

    def normalize_filter_values(raw_filter: Any) -> list[Any]:
        if raw_filter is None:
            return []
        if isinstance(raw_filter, list):
            values = raw_filter
        else:
            values = [raw_filter]

        normalized_values: list[Any] = []
        for value in values:
            if value is None:
                continue
            if isinstance(value, str):
                stripped = value.strip()
                if stripped == "":
                    continue
                normalized_values.append(stripped)
            else:
                normalized_values.append(value)
        return normalized_values

    def filter_dataframe_by_column(
        df: pd.DataFrame,
        column_name: str,
        raw_filter: Any,
    ) -> pd.DataFrame:
        filter_values = normalize_filter_values(raw_filter)
        if not filter_values or column_name not in df.columns:
            return df

        series = df[column_name]
        if pd.api.types.is_numeric_dtype(series):
            normalized_values = pd.to_numeric(pd.Series(filter_values), errors="coerce").dropna().tolist()
            if not normalized_values:
                return df.iloc[0:0].copy()
            mask = pd.to_numeric(series, errors="coerce").isin(normalized_values)
            return df.loc[mask].copy()

        if pd.api.types.is_datetime64_any_dtype(series):
            normalized_values = pd.to_datetime(pd.Series(filter_values), errors="coerce").dropna().tolist()
            if not normalized_values:
                return df.iloc[0:0].copy()
            mask = pd.to_datetime(series, errors="coerce").isin(normalized_values)
            return df.loc[mask].copy()

        if pd.api.types.is_bool_dtype(series):
            lowered_values = {str(value).strip().lower() for value in filter_values}
            truthy = {"true", "yes", "y", "1"}
            falsy = {"false", "no", "n", "0"}
            normalized_booleans: list[bool] = []
            if lowered_values & truthy:
                normalized_booleans.append(True)
            if lowered_values & falsy:
                normalized_booleans.append(False)
            if not normalized_booleans:
                return df.iloc[0:0].copy()
            mask = series.isin(normalized_booleans)
            return df.loc[mask].copy()

        normalized_values = {str(value).strip() for value in filter_values}
        mask = series.astype("string").str.strip().isin(normalized_values)
        return df.loc[mask].copy()

    for output_column, settings in aggregation_config.items():
        source_column = settings.get("source_column", output_column)
        filtered_df = filter_dataframe_by_column(
            filtered_df,
            source_column,
            settings.get("filter"),
        )

    for output_column, settings in aggregation_config.items():
        selected = settings.get("selected")
        source_column = settings.get("source_column", output_column)

        if selected not in SUPPORTED_AGGREGATIONS or source_column not in filtered_df.columns:
            continue

        if selected == "mode":
            aggregations[output_column] = (
                source_column,
                lambda series: series.mode().iloc[0] if not series.mode().empty else pd.NA,
            )
        else:
            pandas_agg = "nunique" if selected == "nunique" else selected
            aggregations[output_column] = (source_column, pandas_agg)

    if filtered_df.empty:
        return pd.DataFrame(columns=[join_key, *aggregations.keys()])

    if not aggregations:
        return filtered_df[[join_key]].drop_duplicates().copy()

    return filtered_df.groupby(join_key).agg(**aggregations).reset_index()


def keep_configured_aggregated_columns(
    aggregated_df: pd.DataFrame,
    join_key: str,
    aggregation_config: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    keep_columns = [join_key]

    for output_column, settings in aggregation_config.items():
        if settings.get("keep_after_merge") and output_column in aggregated_df.columns:
            keep_columns.append(output_column)

    existing_columns = [column for column in keep_columns if column in aggregated_df.columns]
    if not existing_columns:
        return aggregated_df[[join_key]].drop_duplicates().copy()

    return aggregated_df[existing_columns].copy()


def apply_missing_value_fills(
    df: pd.DataFrame,
    fill_config: dict[str, Any],
) -> pd.DataFrame:
    if not fill_config:
        return df

    filled = df.copy()
    for column_name, fill_value in fill_config.items():
        if column_name not in filled.columns:
            continue
        filled[column_name] = filled[column_name].fillna(fill_value)

    return filled


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
