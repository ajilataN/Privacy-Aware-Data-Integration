from __future__ import annotations

from itertools import combinations
from typing import Any

import pandas as pd


SUPPORTED_AGGREGATIONS = {
    "first",
    "last",
    "min",
    "max",
    "sum",
    "mean",
    "mode",
    "count",
    "nunique",
}


def infer_semantic_type(series: pd.Series) -> str:
    non_null = series.dropna()
    if non_null.empty:
        return "unknown"

    if pd.api.types.is_bool_dtype(series):
        return "boolean"

    if pd.api.types.is_numeric_dtype(series):
        return "numeric"

    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"

    parsed_datetime = pd.to_datetime(non_null, errors="coerce")
    if parsed_datetime.notna().mean() >= 0.8:
        return "datetime"

    unique_ratio = non_null.nunique(dropna=True) / max(len(non_null), 1)
    avg_length = non_null.astype(str).str.len().mean()

    if unique_ratio > 0.9 and avg_length > 6:
        return "identifier_like"

    if non_null.nunique(dropna=True) <= 20:
        return "categorical"

    if avg_length > 30:
        return "free_text"

    return "categorical"


def possible_user_values_for_type() -> list[str]:
    return [
        "identifier",
        "direct_identifier",
        "quasi_identifier",
        "sensitive_attribute",
        "categorical",
        "numeric",
        "datetime",
        "boolean",
        "free_text",
        "technical",
        "drop",
    ]


def sample_values(series: pd.Series, limit: int = 10) -> list[Any]:
    return series.dropna().astype(str).value_counts().head(limit).index.tolist()


def profile_column(df: pd.DataFrame, column_name: str) -> dict[str, Any]:
    series = df[column_name]
    non_null = series.dropna()

    return {
        "name": column_name,
        "inferred_type": infer_semantic_type(series),
        "nullable": bool(series.isna().any()),
        "non_null_count": int(non_null.shape[0]),
        "unique_count": int(non_null.nunique(dropna=True)),
        "sample_values": sample_values(series),
        "allowed_type_values": possible_user_values_for_type(),
        "suggested_role": "direct_identifier" if "id" in column_name.lower() else "keep",
    }


def suggest_join_key(
    graduates_df: pd.DataFrame,
    mobility_df: pd.DataFrame,
    explicit_key: str | None,
) -> str | None:
    if explicit_key:
        return explicit_key

    graduate_columns = {column.lower(): column for column in graduates_df.columns}
    mobility_columns = {column.lower(): column for column in mobility_df.columns}

    # Prefer obvious shared identifier names first.
    for candidate in ("student_id", "enrollment_number", "studentid", "id"):
        if candidate in graduate_columns and candidate in mobility_columns:
            return graduate_columns[candidate]

    shared_columns = [
        graduate_columns[column_name]
        for column_name in graduate_columns
        if column_name in mobility_columns and column_name not in {"source_file", "source_year"}
    ]
    if shared_columns:
        return shared_columns[0]

    def sampled_value_lengths(df: pd.DataFrame, column_name: str, sample_size: int = 10) -> list[int]:
        non_null = df[column_name].dropna().astype(str).str.strip()
        non_empty = non_null[non_null != ""]
        if non_empty.empty:
            return []

        sample_count = min(sample_size, len(non_empty))
        sampled = non_empty.sample(n=sample_count, random_state=42) if len(non_empty) > sample_count else non_empty
        return sampled.str.len().tolist()

    best_match: tuple[str, float] | None = None

    for graduate_column in graduates_df.columns:
        if graduate_column in {"source_file", "source_year"}:
            continue

        graduate_type = infer_semantic_type(graduates_df[graduate_column])
        if graduate_type not in {"identifier_like", "categorical", "numeric"}:
            continue

        graduate_lengths = sampled_value_lengths(graduates_df, graduate_column)
        if not graduate_lengths:
            continue

        graduate_average = sum(graduate_lengths) / len(graduate_lengths)

        for mobility_column in mobility_df.columns:
            if mobility_column in {"source_file", "source_year"}:
                continue

            mobility_lengths = sampled_value_lengths(mobility_df, mobility_column)
            if not mobility_lengths:
                continue

            mobility_average = sum(mobility_lengths) / len(mobility_lengths)
            average_difference = abs(graduate_average - mobility_average)

            if average_difference > 1:
                continue

            exact_length_matches = sum(
                1
                for graduate_length in graduate_lengths
                for mobility_length in mobility_lengths
                if graduate_length == mobility_length
            )
            total_pairs = len(graduate_lengths) * len(mobility_lengths)
            similarity_score = exact_length_matches / total_pairs if total_pairs else 0.0

            if similarity_score < 0.6:
                continue

            candidate_score = similarity_score - (average_difference * 0.1)
            if best_match is None or candidate_score > best_match[1]:
                best_match = (graduate_column, candidate_score)

    if best_match is not None:
        return best_match[0]

    return None


def suggest_aggregations(df: pd.DataFrame, join_key: str | None) -> dict[str, dict[str, Any]]:
    suggestions: dict[str, dict[str, Any]] = {}

    for column in df.columns:
        if column == join_key:
            continue

        inferred_type = infer_semantic_type(df[column])
        allowed = ["first"]
        selected = "first"

        if inferred_type == "numeric":
            allowed = ["first", "min", "max", "mean", "sum", "count"]
        elif inferred_type == "datetime":
            allowed = ["first", "min", "max"]
            selected = "min"
        elif inferred_type in {"categorical", "boolean"}:
            allowed = ["first", "mode", "nunique", "count"]

        suggestions[column] = {
            "selected": selected,
            "allowed": allowed,
            "keep_after_merge": False,
        }

    if join_key:
        suggestions["mobility_record_count"] = {
            "selected": "count",
            "source_column": join_key,
            "allowed": ["count"],
            "keep_after_merge": True,
        }

    return suggestions


def suggest_qi_candidates(df: pd.DataFrame, join_key: str | None) -> list[list[str]]:
    def content_suitability_score(series: pd.Series, inferred_type: str) -> float:
        non_null = series.dropna()
        row_count = max(len(series), 1)
        unique_count = non_null.nunique(dropna=True)

        if unique_count <= 1:
            return -1.0

        completeness_score = len(non_null) / row_count
        unique_ratio = unique_count / max(len(non_null), 1)

        # Prefer columns that are neither constant nor close to unique identifiers.
        if unique_ratio >= 0.9:
            cardinality_score = 0.0
        elif unique_ratio >= 0.5:
            cardinality_score = 0.5
        elif unique_ratio >= 0.1:
            cardinality_score = 1.0
        else:
            cardinality_score = 0.8

        type_score = 1.0 if inferred_type in {"categorical", "datetime"} else 0.7

        return (completeness_score * 2.0) + cardinality_score + type_score

    candidate_columns: list[tuple[str, float]] = []
    for column in df.columns:
        if column == join_key or column in {"source_file", "source_year"}:
            continue

        series = df[column]
        inferred_type = infer_semantic_type(series)
        unique_count = series.dropna().nunique(dropna=True)

        if inferred_type not in {"categorical", "datetime", "numeric"}:
            continue
        if not (1 < unique_count <= 50):
            continue

        score = content_suitability_score(series, inferred_type)
        if score < 0:
            continue

        candidate_columns.append((column, score))

    candidate_columns = [
        column_name
        for column_name, _ in sorted(
            candidate_columns,
            key=lambda item: (-item[1], item[0]),
        )[:8]
    ]

    ranked_combinations: list[tuple[int, int, int, int, list[str]]] = []
    for combination_size in (2, 3):
        for column_combination in combinations(candidate_columns, combination_size):
            valid_rows = df[list(column_combination)].dropna()
            if valid_rows.empty:
                continue

            groups = valid_rows.groupby(list(column_combination)).size()
            equivalence_class_count = int(groups.shape[0])
            if equivalence_class_count < 2:
                continue

            max_k = int(groups.min())
            average_group_size = float(groups.mean())
            content_score = sum(
                content_suitability_score(
                    df[column_name],
                    infer_semantic_type(df[column_name]),
                )
                for column_name in column_combination
            )

            ranked_combinations.append(
                (
                    max_k,
                    round(content_score, 4),
                    round(average_group_size, 4),
                    -combination_size,
                    equivalence_class_count,
                    list(column_combination),
                )
            )

    ranked_combinations.sort(
        key=lambda item: (-item[0], -item[1], -item[2], -item[3], item[4], item[5]),
    )

    return [columns for _, _, _, _, _, columns in ranked_combinations[:10]]
