import pandas as pd


def clean_mobility_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the mobility dataset.
    """

    df = df.copy()

    # fix type of enrollment number
    df["enrollment_number"] = df["enrollment_number"].astype(str)

    # filter students
    # might there be some other statuses for professionals 
    if "status" in df.columns:
        df = df[df["status"].str.lower() == "student"]

    text_columns = [
        "faculty",
        "mobility_direction",
        "mobility_programme",
        "mobility_type",
        "mobility_form",
        "country",
        "eu_noneu",
        "study_programme",
        "degree_level",
        "full_parttime",
        "short_long_term",
    ]

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Fix date type and extract year and month
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        df["mobility_year"] = df["date"].dt.year
        df["mobility_month"] = df["date"].dt.month

    # remove duplicates
    df = df.drop_duplicates()

    return df


def clean_graduates_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the graduates dataset.
    """

    df = df.copy()

    # fix type of enrollment number
    df["enrollment_number"] = df["enrollment_number"].astype(str)

    text_columns = ["faculty", "study_type", "degree_level"]

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Fix date type and extract year and month
    if "graduation_date" in df.columns:
        df["graduation_date"] = pd.to_datetime(df["graduation_date"], errors="coerce")

        df["graduation_year"] = df["graduation_date"].dt.year

    # remove duplicates
    df = df.drop_duplicates()

    return df


def aggregate_mobility_per_student(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates mobility records to student level.
    In case a student has multiple mobility records, we count them and also keep track of the first and last mobility year.
    """

    mobility_summary = (
        df.groupby("enrollment_number")
        .agg(
            mobility_count=("enrollment_number", "count"),
            first_mobility_year=("mobility_year", "min"),
            last_mobility_year=("mobility_year", "max"),
            eu_noneu=("eu_noneu","first"), # can be a boolean
            short_long_term=("short_long_term","first"), # can be a boolean or 3 values one for short one for long and one for both
        )
        .reset_index()
    )

    mobility_summary["had_mobility"] = 1

    return mobility_summary
