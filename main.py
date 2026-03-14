from src.loader import load_dataset


def main():

    mobility_path = "data/mobility"
    graduates_path = "data/graduates"

    years_to_include = 1
    target_year = None

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


if __name__ == "__main__":
    main()