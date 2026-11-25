"""Static Matplotlib charts for the project (no dashboard, no web app).

Three charts are produced from the processed CSV files:
  1. reports/top_repositories_by_stars.png
  2. reports/commits_by_repository.png
  3. reports/open_vs_closed_issues.png

Run standalone:  python -m analysis.visualizations

Backend note
------------
The default backend is `Agg`, which writes PNG files without needing a display.
Agg needs one small compiled module (`_backend_agg`). On some Windows machines a
security policy (Smart App Control / WDAC) blocks that module and the charts
cannot be drawn. In that case this module automatically switches to the
pure-Python `SVG` backend and writes `reports/*.svg` instead - same three charts,
same data, only the file format changes. You can force a backend yourself with
the environment variable CHART_BACKEND=agg or CHART_BACKEND=svg.
"""

from __future__ import annotations

import importlib
import logging
import os
from typing import List, Optional

import matplotlib

logger = logging.getLogger(__name__)


def _select_backend() -> str:
    """Choose the Matplotlib backend and return the chart file extension.

    'agg' -> .png (fast, needs the compiled _backend_agg module)
    'svg' -> .svg (pure Python, always available)
    """
    requested = os.getenv("CHART_BACKEND", "").strip().lower()

    if requested == "svg":
        matplotlib.use("svg")
        return ".svg"

    try:
        matplotlib.use("Agg")
        # force the compiled module to load now, so a blocked DLL is caught here
        importlib.import_module("matplotlib.backends.backend_agg")
        return ".png"
    except Exception as error:  # blocked by a Windows security policy
        logger.warning(
            "PNG backend unavailable (%s). Falling back to the SVG backend.", error
        )
        matplotlib.use("svg")
        return ".svg"


FILE_EXTENSION = _select_backend()

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")

BAR_COLOUR = "#2E86AB"
SECOND_BAR_COLOUR = "#E07A5F"
TITLE_SIZE = 14


def _save_figure(figure, output_path: str) -> str:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    figure.tight_layout()
    figure.savefig(output_path, dpi=120, bbox_inches="tight")
    plt.close(figure)
    logger.info("Chart saved: %s", output_path)
    return output_path


def plot_top_repositories_by_stars(
    repositories: pd.DataFrame, output_path: Optional[str] = None, top_n: int = 10
) -> str:
    """Chart 1: repositories with the most stars."""
    output_path = output_path or os.path.join(
        REPORTS_DIR, f"top_repositories_by_stars{FILE_EXTENSION}"
    )
    if repositories is None or repositories.empty:
        raise ValueError("Cannot draw the stars chart: the repositories dataset is empty.")

    data = repositories.sort_values("stars", ascending=False).head(top_n)
    figure, axis = plt.subplots(figsize=(9, 5))
    axis.barh(data["full_name"][::-1], data["stars"][::-1], color=BAR_COLOUR)
    axis.set_title(f"Top {len(data)} repositories by stars", fontsize=TITLE_SIZE)
    axis.set_xlabel("Stars")
    axis.set_ylabel("Repository")
    for index, value in enumerate(data["stars"][::-1]):
        axis.text(value, index, f" {int(value):,}", va="center", fontsize=8)
    axis.grid(axis="x", linestyle="--", alpha=0.4)
    return _save_figure(figure, output_path)


def plot_commits_by_repository(
    summary: pd.DataFrame, output_path: Optional[str] = None
) -> str:
    """Chart 2: number of commits collected per repository."""
    output_path = output_path or os.path.join(
        REPORTS_DIR, f"commits_by_repository{FILE_EXTENSION}"
    )
    if summary is None or summary.empty:
        raise ValueError("Cannot draw the commits chart: the repository summary is empty.")

    data = summary.sort_values("commit_count", ascending=False)
    figure, axis = plt.subplots(figsize=(9, 5))
    axis.bar(data["full_name"], data["commit_count"].astype(int), color=BAR_COLOUR, width=0.55)
    axis.set_title("Commit counts by repository", fontsize=TITLE_SIZE)
    axis.set_ylabel("Commits collected")
    axis.set_xlabel("Repository")
    axis.tick_params(axis="x", labelrotation=15)
    for index, value in enumerate(data["commit_count"].astype(int)):
        axis.text(index, value, str(value), ha="center", va="bottom", fontsize=9)
    axis.grid(axis="y", linestyle="--", alpha=0.4)
    return _save_figure(figure, output_path)


def plot_open_vs_closed_issues(
    summary: pd.DataFrame, output_path: Optional[str] = None
) -> str:
    """Chart 3: grouped bars of open and closed issues per repository."""
    output_path = output_path or os.path.join(
        REPORTS_DIR, f"open_vs_closed_issues{FILE_EXTENSION}"
    )
    if summary is None or summary.empty:
        raise ValueError("Cannot draw the issues chart: the repository summary is empty.")

    data = summary.sort_values("open_issues", ascending=False)
    positions = range(len(data))
    width = 0.38

    figure, axis = plt.subplots(figsize=(9, 5))
    axis.bar(
        [position - width / 2 for position in positions],
        data["open_issues"].astype(int),
        width=width,
        label="Open issues",
        color=SECOND_BAR_COLOUR,
    )
    axis.bar(
        [position + width / 2 for position in positions],
        data["closed_issues"].astype(int),
        width=width,
        label="Closed issues",
        color=BAR_COLOUR,
    )
    axis.set_xticks(list(positions))
    axis.set_xticklabels(data["full_name"], rotation=15)
    axis.set_title("Open versus closed issues per repository", fontsize=TITLE_SIZE)
    axis.set_ylabel("Number of issues")
    axis.legend()
    axis.grid(axis="y", linestyle="--", alpha=0.4)
    return _save_figure(figure, output_path)


def generate_all_charts(
    processed_dir: str = PROCESSED_DIR, reports_dir: str = REPORTS_DIR
) -> List[str]:
    """Read the processed CSVs and create all three charts.

    Raises:
        FileNotFoundError: when the processed CSV files are missing.
    """
    repositories_path = os.path.join(processed_dir, "repositories.csv")
    summary_path = os.path.join(processed_dir, "repository_summary.csv")
    for path in (repositories_path, summary_path):
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Processed file not found: {path}. Run the transformation stage first."
            )

    repositories = pd.read_csv(repositories_path)
    summary = pd.read_csv(summary_path)

    return [
        plot_top_repositories_by_stars(
            repositories,
            os.path.join(reports_dir, f"top_repositories_by_stars{FILE_EXTENSION}"),
        ),
        plot_commits_by_repository(
            summary, os.path.join(reports_dir, f"commits_by_repository{FILE_EXTENSION}")
        ),
        plot_open_vs_closed_issues(
            summary, os.path.join(reports_dir, f"open_vs_closed_issues{FILE_EXTENSION}")
        ),
    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    for chart in generate_all_charts():
        print(f"Created {chart}")
