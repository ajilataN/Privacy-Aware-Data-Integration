import pandas as pd


DIRECT_IDENTIFIERS = [
    "enrollment_number",
    "last_name",
    "first_name",
]

TECHNICAL_COLUMNS = [
    "source_file",
    "source_year",
]

DEGREE_LEVEL_GENERALIZATION = {
    "Bachelor": "Undergraduate",
    "Master": "Graduate",
    "PhD": "Graduate",
}

FACULTY_GENERALIZATION = {
    "UP FHŠ": "Social Sciences",
    "UP FM": "Social Sciences",
    "UP FAMNIT": "STEM",
    "UP FVZ": "Health",
    "UP PEF": "Education",
    "UP IAM": "Arts",
    "UP FTS - TURISTICA": "Tourism",
}

def remove_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes direct identifiers and technical columns which are not in interest.
    """

    df = df.copy()

    columns_to_drop = DIRECT_IDENTIFIERS + TECHNICAL_COLUMNS

    df = df.drop(columns=[c for c in columns_to_drop if c in df.columns])

    return df

def compute_equivalence_classes(df: pd.DataFrame, quasi_identifiers: list):
    """
    Computes equivalence classes for the given quasi-identifiers.
    """

    groups = (
        df.groupby(quasi_identifiers)
        .size()
        .reset_index(name="group_size")
        .sort_values("group_size")
    )

    return groups

def compute_max_k(groups):
    """
    Computes the maximum possible k based on equivalence classes.
    """

    if groups.empty:
        return 0

    return groups["group_size"].min()

def generalize_degree_level(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generalizes degree level:
    Bachelor -> Undergraduate
    Master, PhD -> Graduate
    """

    df = df.copy()

    if "degree_level" in df.columns:
        df["degree_level"] = df["degree_level"].replace(DEGREE_LEVEL_GENERALIZATION)

    return df

def generalize_faculty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generalizes faculty into study fields.
    """

    df = df.copy()

    if "faculty" in df.columns:
        df["faculty"] = df["faculty"].replace(FACULTY_GENERALIZATION)

    return df