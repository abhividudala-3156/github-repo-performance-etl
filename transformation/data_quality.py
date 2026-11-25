"""Data-quality helpers and the data-quality report.

This module contains the reusable checks that the transformation stage needs
(missing values, duplicates, primary-key checks) and a small report class that
records the "before and after" record counts for every dataset.

Everything here is deliberately simple: pandas value counts, set operations and
a dataclass that is written to CSV at the end of the run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Low level checks
# ---------------------------------------------------------------------------


def count_duplicates(dataframe: pd.DataFrame, subset: Optional[List[str]] = None) -> int:
    """Return the number of duplicate rows (0 when the frame is empty)."""
    if dataframe is None or dataframe.empty:
        return 0
    return int(dataframe.duplicated(subset=subset).sum())


def missing_value_summary(dataframe: pd.DataFrame) -> Dict[str, int]:
    """Return {column: number_of_missing_values} for columns that have gaps."""
    if dataframe is None or dataframe.empty:
        return {}
    missing = dataframe.isna().sum()
    return {column: int(count) for column, count in missing.items() if count > 0}


def count_missing_values(dataframe: pd.DataFrame) -> int:
    """Return the total number of missing cells in a dataframe."""
    if dataframe is None or dataframe.empty:
        return 0
    return int(dataframe.isna().sum().sum())


def check_primary_key_unique(dataframe: pd.DataFrame, key_column: str) -> List[str]:
    """Return a list of problems found with a supposed primary key column."""
    problems: List[str] = []
    if dataframe is None or dataframe.empty:
        return problems
    if key_column not in dataframe.columns:
        problems.append(f"Column '{key_column}' is missing from the dataset.")
        return problems
    null_count = int(dataframe[key_column].isna().sum())
    if null_count:
        problems.append(f"{null_count} record(s) have an empty '{key_column}'.")
    duplicate_count = int(dataframe[key_column].duplicated().sum())
    if duplicate_count:
        problems.append(f"{duplicate_count} duplicate value(s) found in '{key_column}'.")
    return problems


def check_required_columns(dataframe: pd.DataFrame, required: List[str]) -> List[str]:
    """Return a problem string for every required column that is missing."""
    if dataframe is None:
        return ["Dataset is empty (None)."]
    return [
        f"Required column '{column}' is missing."
        for column in required
        if column not in dataframe.columns
    ]


# ---------------------------------------------------------------------------
# Report objects
# ---------------------------------------------------------------------------


@dataclass
class DatasetQuality:
    """Quality metrics for one dataset (table)."""

    dataset: str
    records_extracted: int = 0
    records_loaded: int = 0
    duplicates_removed: int = 0
    records_rejected: int = 0
    records_excluded: int = 0
    missing_values_filled: int = 0
    missing_values_after: Dict[str, int] = field(default_factory=dict)
    notes: str = ""


class DataQualityReport:
    """Collects one DatasetQuality entry per dataset and writes a CSV report."""

    COLUMNS = [
        "dataset",
        "records_extracted",
        "duplicates_removed",
        "records_rejected",
        "records_excluded",
        "records_loaded",
        "missing_values_filled",
        "missing_values_remaining",
        "notes",
    ]

    def __init__(self) -> None:
        self.entries: List[DatasetQuality] = []

    def add(self, entry: DatasetQuality) -> None:
        self.entries.append(entry)

    def add_dataset(self, dataset: str, **kwargs) -> None:
        self.entries.append(DatasetQuality(dataset=dataset, **kwargs))

    def to_dataframe(self) -> pd.DataFrame:
        rows = []
        for entry in self.entries:
            rows.append(
                {
                    "dataset": entry.dataset,
                    "records_extracted": entry.records_extracted,
                    "duplicates_removed": entry.duplicates_removed,
                    "records_rejected": entry.records_rejected,
                    "records_excluded": entry.records_excluded,
                    "records_loaded": entry.records_loaded,
                    "missing_values_filled": entry.missing_values_filled,
                    "missing_values_remaining": sum(entry.missing_values_after.values()),
                    "notes": entry.notes,
                }
            )
        return pd.DataFrame(rows, columns=self.COLUMNS)

    def save_csv(self, path: str) -> str:
        """Write the report to CSV (creates the folder if needed)."""
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self.to_dataframe().to_csv(path, index=False)
        return path

    def summary_text(self) -> str:
        """Readable multi-line summary for the console."""
        lines = []
        for entry in self.entries:
            lines.append(
                f"  {entry.dataset:<26} extracted={entry.records_extracted:<5} "
                f"duplicates_removed={entry.duplicates_removed:<4} "
                f"rejected={entry.records_rejected:<4} "
                f"excluded={entry.records_excluded:<4} "
                f"loaded={entry.records_loaded:<5} "
                f"missing_filled={entry.missing_values_filled}"
            )
        return "\n".join(lines) if lines else "  (no datasets processed)"

    @property
    def total_extracted(self) -> int:
        return sum(entry.records_extracted for entry in self.entries)

    @property
    def total_loaded(self) -> int:
        return sum(entry.records_loaded for entry in self.entries)


# ---------------------------------------------------------------------------
# Dataset level validation used by main.py before loading into PostgreSQL
# ---------------------------------------------------------------------------


def run_quality_checks(
    datasets: Dict[str, pd.DataFrame],
    primary_keys: Dict[str, str],
    required_columns: Optional[Dict[str, List[str]]] = None,
) -> List[str]:
    """Run basic data-quality checks and return a list of problem messages.

    An empty list means all checks passed.
    """
    problems: List[str] = []
    required_columns = required_columns or {}

    for name, dataframe in datasets.items():
        if dataframe is None:
            problems.append(f"[{name}] dataset is None.")
            continue
        if dataframe.empty:
            problems.append(f"[{name}] dataset is empty (0 records).")
            continue

        if name in primary_keys:
            for problem in check_primary_key_unique(dataframe, primary_keys[name]):
                problems.append(f"[{name}] {problem}")

        for problem in check_required_columns(dataframe, required_columns.get(name, [])):
            problems.append(f"[{name}] {problem}")

    return problems
