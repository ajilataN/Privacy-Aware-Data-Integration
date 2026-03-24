from src.loader import load_dataset
from src.preprocess import clean_mobility_data, clean_graduates_data, aggregate_mobility_per_student
from src.dataset_integration import integrate_datasets
from src.anonymization import (
    generalize_degree_level,
    generalize_faculty,
    remove_identifiers,
    compute_equivalence_classes,
    compute_max_k,
    compute_l_diversity,
    compute_t_closeness,
    suppress_small_equivalence_classes,
)

def main():
    mobility_path = "data/mobility"
    graduates_path = "data/graduates"

    mobility_years_to_include = 5
    graduates_years_to_include = 5
    target_year = None
    target_k = 3

    # 1. Load
    mobility_df = load_dataset(
        mobility_path,
        years_to_include=mobility_years_to_include,
        target_year=target_year,
    )
    graduates_df = load_dataset(
        graduates_path,
        years_to_include=graduates_years_to_include,
        target_year=target_year,
    )

    # 2. Preprocess
    mobility_df = clean_mobility_data(mobility_df)
    graduates_df = clean_graduates_data(graduates_df)

    # 3. Aggregate mobility per student
    mobility_summary_df = aggregate_mobility_per_student(mobility_df)

    # 4. Integrate
    merged_df = integrate_datasets(graduates_df, mobility_summary_df)
    merged_df.to_excel("output/merged_dataset.xlsx", index=False)

    anonymization_input = remove_identifiers(merged_df)

    QI = ["faculty", "degree_level", "graduation_year"]

    SENSITIVE_ATTRIBUTES = [
        "had_mobility",
        "mobility_count_bucket",
        "eu_noneu",
        "short_long_term",
    ]

    print("\n Before generalization")

    groups = compute_equivalence_classes(anonymization_input, QI)
    print(groups.describe())

    max_k = compute_max_k(groups)
    print("\nMaximum possible k:", max_k)


    # Apply generalization
    generalized_df = generalize_degree_level(anonymization_input)
    generalized_df = generalize_faculty(generalized_df)
    anonymized_df = suppress_small_equivalence_classes(generalized_df, QI, min_k=target_k)

    anonymized_df.to_excel("output/anonymized/anonymized_dataset.xlsx", index=False)

    print("\n After generalization")

    groups_generalized = compute_equivalence_classes(generalized_df, QI)
    print(groups_generalized.describe())

    max_k_generalized = compute_max_k(groups_generalized)
    print("\nMaximum possible k after generalization:", max_k_generalized)

    print(groups_generalized.sort_values("group_size").head(10))

    print(f"\nRows after suppressing groups smaller than k={target_k}: {len(anonymized_df)}")

    if anonymized_df.empty:
        print("\nNo rows remain after suppression. Lower target_k or regenerate a denser dataset.")
        return

    report_df = (
        anonymized_df.groupby("graduation_year")
        .agg(
            graduates=("had_mobility", "size"),
            graduates_with_mobility=("had_mobility", "sum"),
        )
        .reset_index()
    )
    report_df["mobility_share"] = (
        report_df["graduates_with_mobility"] / report_df["graduates"]
    ).round(3)
    report_df.to_excel("output/reports/mobility_report_summary.xlsx", index=False)


    for sensitive in SENSITIVE_ATTRIBUTES:

        print(f"\nEvaluating sensitive attribute: {sensitive}")

        l_groups = compute_l_diversity(anonymized_df, QI, sensitive)

        print("L-diversity statistics:")
        print(l_groups["l_diversity"].describe())

        print("Minimum L-diversity:", l_groups["l_diversity"].min())

        t_groups = compute_t_closeness(anonymized_df, QI, sensitive)

        print("T-closeness statistics:")
        print(t_groups["t_distance"].describe())

if __name__ == "__main__":
    main()
