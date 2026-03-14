from src.loader import load_dataset
from src.preprocess import clean_mobility_data, clean_graduates_data, aggregate_mobility_per_student


def main():

    mobility_path = "data/mobility"
    graduates_path = "data/graduates"

    years_to_include = 1
    target_year = None

    # load dataset
    mobility_df = load_dataset(
        mobility_path,
        years_to_include=years_to_include,
        target_year=target_year
    )

    graduates_df = load_dataset(
        graduates_path,
        years_to_include=years_to_include,
        target_year=target_year
    )

    print("\nMobility dataset preview:")
    print(mobility_df.head())

    print("\nGraduates dataset preview:")
    print(graduates_df.head())

    # preprocess
    mobility_df = clean_mobility_data(mobility_df)
    graduates_df = clean_graduates_data(graduates_df)

    print("\nAfter cleaning:")
    print("Mobility shape:", mobility_df.shape)
    print("Graduates shape:", graduates_df.shape)

    # aggregate mobility data to stdudent level
    mobility_summary = aggregate_mobility_per_student(mobility_df)

    print("\nMobility summary preview:")
    print(mobility_summary.head())


if __name__ == "__main__":
    main()