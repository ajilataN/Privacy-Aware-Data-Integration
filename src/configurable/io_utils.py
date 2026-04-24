from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(data: dict[str, Any], output_path: str | Path) -> Path:
    output = Path(output_path)
    ensure_parent_dir(output)
    output.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return output


def load_json(path_like: str | Path) -> dict[str, Any]:
    return json.loads(Path(path_like).read_text(encoding="utf-8"))


def write_tabular_dataset(df: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    ensure_parent_dir(output)

    suffix = output.suffix.lower()
    if suffix == ".csv":
        df.to_csv(output, index=False)
    else:
        df.to_excel(output, index=False)

    return output
