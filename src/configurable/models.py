from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class PipelineOutputs:
    merged_df: pd.DataFrame
    anonymized_df: pd.DataFrame
    privacy_metrics: dict[str, Any]
    refinement_payload: dict[str, Any]
