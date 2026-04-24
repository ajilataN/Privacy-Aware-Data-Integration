from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class PreparationOutputs:
    prepared_df: pd.DataFrame
    anonymization_config: dict[str, Any]


@dataclass
class AnonymizationOutputs:
    anonymized_df: pd.DataFrame
    privacy_metrics: dict[str, Any]
    refinement_payload: dict[str, Any]
