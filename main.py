from src.loader import load_dataset
from src.preprocess import clean_mobility_data, clean_graduates_data, aggregate_mobility_per_student
from src.dataset_integration import integrate_datasets


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

    print("\nMerged dataset shape:", merged_df.shape)
    print("\nMerged dataset preview:")
    print(merged_df.head())


if __name__ == "__main__":
    main()
