from __future__ import annotations

from pathlib import Path

import pandas as pd


SUPPORTED_TABULAR_SUFFIXES = (".xlsx", ".xls", ".csv")


def list_tabular_files(path_like: str | Path) -> list[Path]:
    """
    Return supported tabular files from a file or folder path.
    """
    path = Path(path_like)

    if path.is_file():
        if path.suffix.lower() not in SUPPORTED_TABULAR_SUFFIXES or path.name.startswith("~$"):
            return []
        return [path]

    files: list[Path] = []
    for pattern in ("*.xlsx", "*.xls", "*.csv"):
        files.extend(
            file_path
            for file_path in path.glob(pattern)
            if not file_path.name.startswith("~$")
        )

    return sorted(files)


def read_tabular_file(
    file_path: Path,
    *,
    add_source_metadata: bool = True,
) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    if add_source_metadata:
        df["source_file"] = file_path.name

    return df


def load_any_dataset(
    path_like: str | Path,
    *,
    add_source_metadata: bool = True,
) -> pd.DataFrame:
    """
    Load a single CSV/Excel file or all supported files in a folder.
    """
    files = list_tabular_files(path_like)
    if not files:
        raise FileNotFoundError(f"No supported files found at {path_like}")

    dataframes = [
        read_tabular_file(file_path, add_source_metadata=add_source_metadata)
        for file_path in files
    ]
    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
