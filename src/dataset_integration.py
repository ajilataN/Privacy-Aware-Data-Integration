import pandas as pd


def integrate_datasets(
    graduates_df: pd.DataFrame,
    mobility_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge graduates with aggregated mobility data.
    Result: one row per graduate.
    """

    merged_df = graduates_df.merge(
        mobility_summary_df,
        on="enrollment_number",
        how="left"
    )

    # Fill missing mobility data for graduates that had no mobility
    if "mobility_count" in merged_df.columns:
        merged_df["mobility_count"] = merged_df["mobility_count"].fillna(0).astype(int)

    if "had_mobility" in merged_df.columns:
        merged_df["had_mobility"] = merged_df["had_mobility"].fillna(0).astype(int)

    for col in ["first_mobility_year", "last_mobility_year"]:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col]

    return merged_df
