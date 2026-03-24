from pathlib import Path
import pandas as pd


def list_excel_files(folder_path: str):
    """
    Returns all Excel files in the given folder.
    """
    folder = Path(folder_path)
    return sorted(
        file_path
        for file_path in folder.glob("*.xlsx")
        if not file_path.name.startswith("~$")
    )


def extract_year_from_filename(file_path: Path):
    """
    something_YYYY.xlsx
    """
    return int(file_path.stem.split("_")[-1])


def select_files(files, years_to_include=1, target_year=None):
    """
    Selects which files should be used based on parameters.
    """
    file_years = {extract_year_from_filename(f): f for f in files}

    available_years = sorted(file_years.keys())

    if target_year is None:
        target_year = max(available_years)

    selected_years = [
        year for year in available_years
        if target_year - years_to_include + 1 <= year <= target_year
    ]

    return [file_years[y] for y in selected_years if y in file_years]


def load_excel_files(files):
    """
    Load multiple Excel files and concatenate them.
    """
    dfs = []

    for f in files:
        df = pd.read_excel(f)
        df["source_file"] = f.name
        df["source_year"] = extract_year_from_filename(f)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def load_dataset(folder_path, years_to_include=1, target_year=None):
    """
    Dataset loading pipeline.
    """
    files = list_excel_files(folder_path)
    selected_files = select_files(files, years_to_include, target_year)

    # print(f"Loading {len(selected_files)} files from {folder_path}")

    df = load_excel_files(selected_files)

    # print(f"Loaded dataset shape: {df.shape}")

    return df
