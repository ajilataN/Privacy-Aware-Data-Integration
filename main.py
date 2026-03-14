from src.loader import load_dataset
from src.preprocess import clean_mobility_data, clean_graduates_data, aggregate_mobility_per_student
from src.dataset_integration import integrate_datasets
from src.anonymization import remove_identifiers, compute_equivalence_classes, compute_max_k


def main():
    mobility_path = "data/mobility"
    graduates_path = "data/graduates"

    years_to_include = 1
    target_year = None

    # 1. Load
    mobility_df = load_dataset(
        mobility_path,
        years_to_include=years_to_include,
        target_year=target_year,
    )
    graduates_df = load_dataset(
        graduates_path,
        years_to_include=years_to_include,
        target_year=target_year,
    )

    # 2. Preprocess
    mobility_df = clean_mobility_data(mobility_df)
    graduates_df = clean_graduates_data(graduates_df)

    # 3. Aggregate mobility per student
    mobility_summary_df = aggregate_mobility_per_student(mobility_df)

    # 4. Integrate
    merged_df = integrate_datasets(graduates_df, mobility_summary_df)

    anonymization_input = remove_identifiers(merged_df)

    QI = ["faculty", "study_type", "degree_level", "graduation_year"]
    groups = compute_equivalence_classes(anonymization_input, QI)
    print(groups.describe())

    max_k = compute_max_k(groups)
    print("\nMaximum possible k for this dataset:", max_k)

if __name__ == "__main__":
    main()
