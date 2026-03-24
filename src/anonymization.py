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
    "UP FHŠ": "Society & Culture",
    "UP FM": "Business & Economics",
    "UP FAMNIT": "STEM",
    "UP FVZ": "Health Sciences",
    "UP PEF": "Society & Culture",
    "UP IAM": "STEM",
    "UP FTS - TURISTICA": "Business & Economics",
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


def suppress_small_equivalence_classes(
    df: pd.DataFrame,
    quasi_identifiers: list,
    min_k: int,
) -> pd.DataFrame:
    """
    Removes records that belong to equivalence classes smaller than min_k.
    """

    group_sizes = (
        df.groupby(quasi_identifiers)
        .size()
        .reset_index(name="group_size")
    )

    valid_groups = group_sizes[group_sizes["group_size"] >= min_k].drop(columns="group_size")

    if valid_groups.empty:
        return df.iloc[0:0].copy()

    return df.merge(valid_groups, on=quasi_identifiers, how="inner")

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

def compute_l_diversity(df: pd.DataFrame, quasi_identifiers: list, sensitive_attribute: str):
    """
    Computes L-diversity for each equivalence class.
    L-diversity = number of distinct sensitive values in each group.
    """

    l_groups = (
        df.groupby(quasi_identifiers)[sensitive_attribute]
        .nunique()
        .reset_index(name="l_diversity")
        .sort_values("l_diversity")
    )

    return l_groups

def compute_t_closeness(df: pd.DataFrame, quasi_identifiers: list, sensitive_attribute: str):
    """
    Computes T-closeness using the absolute difference between the global distribution and group distribution.
    """

    global_dist = df[sensitive_attribute].value_counts(normalize=True)

    results = []

    for group_values, group in df.groupby(quasi_identifiers):

        group_dist = group[sensitive_attribute].value_counts(normalize=True)
        aligned = global_dist.subtract(group_dist, fill_value=0).abs()

        t_distance = aligned.sum()

        if not isinstance(group_values, tuple):
            group_values = (group_values,)

        results.append((*group_values, t_distance))

    columns = quasi_identifiers + ["t_distance"]

    return pd.DataFrame(results, columns=columns)

