"""Main entry point of the GitHub Repository Performance Analytics ETL pipeline.

Run the whole pipeline with:

    python main.py

Useful options (see `python main.py --help`):

    python main.py --skip-db          # run extraction + transformation only
    python main.py --max-pages 1      # fewer API pages per endpoint (faster)
    python main.py --repos psf/requests,numpy/numpy

Pipeline stages (the numbers are printed in the console):
    1  read repository configuration
    2  extract repository details
    3  extract commits
    4  extract contributors
    5  extract issues
    6  extract pull requests
    7  save raw JSON
    8  clean and transform
    9  data-quality checks
    10 save processed CSV files
    11 save rejected records
    12 connect to PostgreSQL
    13 create tables
    14 load data
    15 generate charts
    16 print the execution summary
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:  # makes "python main.py" work from any folder
    sys.path.insert(0, PROJECT_ROOT)

import pandas as pd  # noqa: E402

from analysis.visualizations import generate_all_charts  # noqa: E402
from extraction.github_extractor import (  # noqa: E402
    GITHUB_API_BASE,
    GitHubExtractionError,
    GitHubExtractor,
    save_raw_responses,
    split_full_name,
)
from transformation.cleaning import TransformationResult, transform_all  # noqa: E402
from transformation.data_quality import (  # noqa: E402
    DataQualityReport,
    DatasetQuality,
    missing_value_summary,
    run_quality_checks,
)

logger = logging.getLogger("etl")

# --- Project paths ---------------------------------------------------------
PATHS = {
    "config": os.path.join(PROJECT_ROOT, "config", "repositories.json"),
    "raw": os.path.join(PROJECT_ROOT, "data", "raw"),
    "processed": os.path.join(PROJECT_ROOT, "data", "processed"),
    "rejected": os.path.join(PROJECT_ROOT, "data", "rejected"),
    "reports": os.path.join(PROJECT_ROOT, "reports"),
    "logs": os.path.join(PROJECT_ROOT, "logs"),
}

# Tables written to data/processed/*.csv and loaded into PostgreSQL
TABLE_NAMES = [
    "repositories",
    "contributors",
    "repository_contributors",
    "commits",
    "issues",
    "pull_requests",
]

PRIMARY_KEYS = {
    "repositories": "repo_id",
    "contributors": "contributor_id",
    "commits": "commit_sha",
    "issues": "issue_id",
    "pull_requests": "pr_id",
}

REQUIRED_COLUMNS = {
    "repositories": ["repo_id", "full_name", "owner_login", "stars"],
    "contributors": ["contributor_id", "username"],
    "repository_contributors": ["repo_id", "contributor_id", "contributions"],
    "commits": ["commit_sha", "repo_id", "authored_at"],
    "issues": ["issue_id", "repo_id", "state", "is_closed"],
    "pull_requests": ["pr_id", "repo_id", "merged"],
}

TOTAL_STAGES = 16


# ---------------------------------------------------------------------------
# Logging and small console helpers
# ---------------------------------------------------------------------------
def setup_logging(log_level: str = "INFO") -> None:
    """Log to the console and to logs/etl.log."""
    os.makedirs(PATHS["logs"], exist_ok=True)
    log_file = os.path.join(PATHS["logs"], "etl.log")

    formatter = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()

    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(log_level)
    console.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    root.addHandler(console)

    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    logger.debug("Logging started, log file: %s", log_file)


def stage(number: int, message: str) -> None:
    """Print a numbered stage header."""
    print(f"\n[{number}/{TOTAL_STAGES}] {message}")


def ok(message: str = "done") -> None:
    print(f"      -> {message}")


# ---------------------------------------------------------------------------
# Stage 1: configuration
# ---------------------------------------------------------------------------
def load_configuration(config_path: str) -> Dict[str, Any]:
    """Read config/repositories.json and validate it."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, encoding="utf-8") as handle:
        config = json.load(handle)

    repositories = config.get("repositories")
    if not isinstance(repositories, list) or not repositories:
        raise ValueError(
            "config/repositories.json must contain a non-empty list under the key "
            "'repositories', for example: [\"pandas-dev/pandas\"]"
        )

    for name in repositories:
        split_full_name(name)  # raises GitHubExtractionError when the format is wrong

    extraction = config.get("extraction", {})
    config["extraction"] = {
        "per_page": int(extraction.get("per_page", 100)),
        "max_pages": int(extraction.get("max_pages", 3)),
        "timeout_seconds": int(extraction.get("timeout_seconds", 30)),
        "request_delay_seconds": float(extraction.get("request_delay_seconds", 0.5)),
    }
    return config


# ---------------------------------------------------------------------------
# Stages 2-6: extraction
# ---------------------------------------------------------------------------
ENDPOINT_STAGES: List[Tuple[str, str]] = [
    ("repository", "Extracting repository details (/repos/{owner}/{repo})"),
    ("commits", "Extracting commits (/repos/{owner}/{repo}/commits)"),
    ("contributors", "Extracting contributors (/repos/{owner}/{repo}/contributors)"),
    ("issues", "Extracting issues (/repos/{owner}/{repo}/issues)"),
    ("pull_requests", "Extracting pull requests (/repos/{owner}/{repo}/pulls)"),
]


def _empty_payload(full_name: str) -> Dict[str, Any]:
    owner, repo = split_full_name(full_name)
    return {
        "metadata": {
            "requested_repository": full_name,
            "owner": owner,
            "repo": repo,
            "extraction_timestamp_utc": None,
            "api_base": GITHUB_API_BASE,
            "endpoints": {
                "repository": f"/repos/{owner}/{repo}",
                "commits": f"/repos/{owner}/{repo}/commits",
                "contributors": f"/repos/{owner}/{repo}/contributors",
                "issues": f"/repos/{owner}/{repo}/issues",
                "pull_requests": f"/repos/{owner}/{repo}/pulls",
            },
            "records": {},
        },
        "repository": None,
        "commits": [],
        "contributors": [],
        "issues": [],
        "pull_requests": [],
    }


def extract_payloads(
    extractor: GitHubExtractor,
    repository_names: List[str],
    first_stage_number: int = 2,
) -> Dict[str, Any]:
    """Run stages 2-6: call the five endpoints for every configured repository."""
    started_at = datetime.now(timezone.utc)
    payloads = [_empty_payload(name) for name in repository_names]
    failures: List[Dict[str, str]] = []

    for offset, (key, label) in enumerate(ENDPOINT_STAGES):
        stage(first_stage_number + offset, label)
        total_records = 0
        for payload, full_name in zip(payloads, repository_names):
            if payload.get("_failed"):
                continue
            owner, repo = split_full_name(full_name)
            try:
                if key == "repository":
                    data = extractor.get_repository(owner, repo)
                    payload["repository"] = data
                    total_records += 1
                else:
                    data = getattr(extractor, f"get_{key}")(owner, repo)
                    payload[key] = data
                    total_records += len(data)
            except GitHubExtractionError as exc:
                logger.error("Extraction failed for %s: %s", full_name, exc)
                payload["_failed"] = True
                payload["_error"] = str(exc)
                failures.append({"repository": full_name, "stage": key, "error": str(exc)})
        ok(f"{total_records} record(s) collected")

    # Drop repositories that failed, they cannot be loaded (no id / no data).
    successful = []
    for payload in payloads:
        if payload.get("_failed"):
            continue
        if not payload.get("repository"):
            failures.append(
                {
                    "repository": payload["metadata"]["requested_repository"],
                    "stage": "repository",
                    "error": "repository details were not extracted",
                }
            )
            continue
        payload.pop("_failed", None)
        payload.pop("_error", None)
        payload["metadata"]["extraction_timestamp_utc"] = started_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        payload["metadata"]["records"] = {
            "commits": len(payload["commits"]),
            "contributors": len(payload["contributors"]),
            "issues_endpoint_records": len(payload["issues"]),
            "pull_requests_inside_issues": sum(
                1 for issue in payload["issues"] if "pull_request" in issue
            ),
            "pull_requests": len(payload["pull_requests"]),
        }
        successful.append(payload)

    if not successful:
        raise GitHubExtractionError(
            "No repository could be extracted. Errors:\n"
            + "\n".join(f"  - {item['repository']}: {item['error']}" for item in failures)
        )

    for payload in successful:
        metadata = payload["metadata"]
        print(
            f"      {metadata['requested_repository']:<28} commits={metadata['records']['commits']:<5} "
            f"contributors={metadata['records']['contributors']:<5} "
            f"issues={metadata['records']['issues_endpoint_records']:<5} "
            f"pull_requests={metadata['records']['pull_requests']}"
        )

    return {
        "extraction_timestamp_utc": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "requested_repositories": list(repository_names),
        "total_api_requests": extractor.request_count,
        "repositories": successful,
        "failures": failures,
    }


# ---------------------------------------------------------------------------
# Stage 9: data-quality report
# ---------------------------------------------------------------------------
def build_quality_report(transformation: TransformationResult) -> DataQualityReport:
    """Turn the cleaning statistics into a data-quality report."""
    report = DataQualityReport()
    for result in transformation.results:
        report.add(
            DatasetQuality(
                dataset=result.dataset,
                records_extracted=result.records_extracted,
                records_loaded=result.records_loaded,
                duplicates_removed=result.duplicates_removed,
                records_rejected=result.records_rejected,
                records_excluded=result.records_excluded,
                missing_values_filled=result.missing_values_filled,
                missing_values_after=missing_value_summary(result.data),
                notes=result.notes,
            )
        )

    # Tables that are derived rather than cleaned directly
    for name in ["repository_contributors", "repository_summary"]:
        dataframe = transformation.tables.get(name, pd.DataFrame())
        report.add(
            DatasetQuality(
                dataset=name,
                records_extracted=len(dataframe),
                records_loaded=len(dataframe),
                missing_values_after=missing_value_summary(dataframe),
                notes="Derived table calculated from the cleaned datasets.",
            )
        )
    return report


# ---------------------------------------------------------------------------
# Stages 10-11: writing files
# ---------------------------------------------------------------------------
def save_processed_csvs(tables: Dict[str, pd.DataFrame], processed_dir: str) -> List[str]:
    """Write every processed table to data/processed/ as CSV."""
    os.makedirs(processed_dir, exist_ok=True)
    written = []
    for name, dataframe in tables.items():
        path = os.path.join(processed_dir, f"{name}.csv")
        dataframe.to_csv(path, index=False, encoding="utf-8")
        written.append(path)
        print(f"      {os.path.basename(path):<32} {len(dataframe):>5} row(s)")
    return written


def save_rejected_records(rejected: Dict[str, pd.DataFrame], rejected_dir: str) -> List[str]:
    """Write rejected records (with rejection_reason) to data/rejected/."""
    os.makedirs(rejected_dir, exist_ok=True)
    written = []
    for name, dataframe in rejected.items():
        if dataframe is None or dataframe.empty:
            continue
        path = os.path.join(rejected_dir, f"{name}_rejected.csv")
        dataframe.to_csv(path, index=False, encoding="utf-8")
        written.append(path)
        print(f"      {os.path.basename(path):<32} {len(dataframe):>5} rejected row(s)")
    if not written:
        print("      No invalid records were rejected in this run.")
    return written


# ---------------------------------------------------------------------------
# Stages 12-14: PostgreSQL
# ---------------------------------------------------------------------------
def run_database_stages(tables: Dict[str, pd.DataFrame]) -> Dict[str, int]:
    """Connect, create the tables and load the data (stages 12-14)."""
    # imported here so that --skip-db also works without SQLAlchemy installed
    from database.connection import check_connection, describe_settings, get_engine
    from database.loader import create_tables, load_all, table_row_counts

    stage(12, "Connecting to PostgreSQL ...")
    engine = get_engine()
    version = check_connection(engine)
    ok(f"connected: {version.split(',')[0]}")
    logger.info("Database settings: %s", describe_settings())

    stage(13, "Creating the required tables (database/schema.sql) ...")
    statements = create_tables(engine)
    ok(f"{statements} SQL statement(s) executed (tables already existing are skipped)")

    stage(14, "Loading the processed data into PostgreSQL ...")
    # only the six database tables are loaded (repository_summary is a CSV report)
    database_tables = {name: tables[name] for name in TABLE_NAMES if name in tables}
    loaded = load_all(engine, database_tables)
    for table_name, rows in loaded.items():
        print(f"      {table_name:<26} {rows:>5} row(s) inserted or updated")

    counts = table_row_counts(engine)
    ok("rows now stored: " + ", ".join(f"{name}={count}" for name, count in counts.items()))
    return counts


# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
def print_summary(
    statuses: List[Tuple[str, str, str]],
    tables: Dict[str, pd.DataFrame],
    elapsed: float,
    db_counts: Optional[Dict[str, int]],
    skip_db: bool,
) -> None:
    stage(16, "Pipeline execution summary")
    print()
    for name, status, detail in statuses:
        print(f"      {status:<9} {name:<44} {detail}")
    print()
    print(f"      Total records processed : {sum(len(frame) for frame in tables.values())}")
    for name in TABLE_NAMES:
        dataframe = tables.get(name)
        if dataframe is not None:
            print(f"        {name:<24} {len(dataframe):>6} row(s)")
    if db_counts:
        print("      PostgreSQL row counts  : " + ", ".join(f"{k}={v}" for k, v in db_counts.items()))
    elif skip_db:
        print("      PostgreSQL              : SKIPPED (--skip-db was used)")
    print(f"      Elapsed time            : {elapsed:.1f} seconds")
    print()
    print("      Output locations:")
    print(f"        raw JSON     : {PATHS['raw']}")
    print(f"        CSV files    : {PATHS['processed']}")
    print(f"        rejected     : {PATHS['rejected']}")
    print(f"        charts       : {PATHS['reports']}")
    print(f"        log file     : {os.path.join(PATHS['logs'], 'etl.log')}")
    print(f"        SQL queries  : {os.path.join(PROJECT_ROOT, 'analysis', 'business_queries.sql')}")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GitHub Repository Performance Analytics - ETL pipeline"
    )
    parser.add_argument(
        "--config",
        default=PATHS["config"],
        help="path to the repository configuration file (default: config/repositories.json)",
    )
    parser.add_argument(
        "--repos",
        default=None,
        help="comma separated repositories that override the config file, "
        "for example: --repos psf/requests,numpy/numpy",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="maximum API pages per endpoint (overrides config/repositories.json)",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="skip the PostgreSQL stages (useful to test extraction and transformation)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="console log level (default: INFO)",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)
    started = time.time()

    statuses: List[Tuple[str, str, str]] = []
    tables: Dict[str, pd.DataFrame] = {}
    db_counts: Optional[Dict[str, int]] = None

    def record(name: str, status: str, detail: str = "") -> None:
        statuses.append((name, status, detail))

    print("=" * 78)
    print(" GITHUB REPOSITORY PERFORMANCE ANALYTICS - ETL PIPELINE")
    print("=" * 78)

    # ---------------- stage 1 : configuration ----------------------------
    stage(1, "Reading repository configuration ...")
    try:
        config = load_configuration(args.config)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        logger.error("Configuration error: %s", exc)
        record("Read repository configuration", "FAILED", str(exc))
        return 1

    repository_names = (
        [name.strip() for name in args.repos.split(",") if name.strip()]
        if args.repos
        else config["repositories"]
    )
    settings = config["extraction"]
    max_pages = args.max_pages if args.max_pages is not None else settings["max_pages"]
    token = os.getenv("GITHUB_TOKEN") or None

    ok(f"{len(repository_names)} repository(ies): {', '.join(repository_names)}")
    ok(
        f"per_page={settings['per_page']} max_pages={max_pages} "
        f"timeout={settings['timeout_seconds']}s "
        f"token={'yes' if token else 'no (60 requests/hour limit)'}"
    )
    record("Read repository configuration", "OK", f"{len(repository_names)} repositories")

    # ---------------- stages 2-6 : extraction ----------------------------
    extractor = GitHubExtractor(
        token=token,
        timeout=settings["timeout_seconds"],
        per_page=settings["per_page"],
        max_pages=max_pages,
        request_delay=settings["request_delay_seconds"],
    )
    try:
        payload = extract_payloads(extractor, repository_names)
    except GitHubExtractionError as exc:
        logger.error("Extraction failed: %s", exc)
        for offset, (_, label) in enumerate(ENDPOINT_STAGES):
            record(label.split(" (")[0], "FAILED", "extraction aborted")
        return 1

    extracted = len(payload["repositories"])
    record("Extract repository details", "OK", f"{extracted} repository(ies)")
    record("Extract commits", "OK", f"{sum(len(p['commits']) for p in payload['repositories'])} records")
    record("Extract contributors", "OK", f"{sum(len(p['contributors']) for p in payload['repositories'])} records")
    record("Extract issues", "OK", f"{sum(len(p['issues']) for p in payload['repositories'])} records (pull requests included)")
    record("Extract pull requests", "OK", f"{sum(len(p['pull_requests']) for p in payload['repositories'])} records")
    if payload["failures"]:
        record("Extraction warnings", "WARNING", f"{len(payload['failures'])} repository(ies) failed")

    # ---------------- stage 7 : raw JSON ---------------------------------
    stage(7, "Saving raw API responses to data/raw/ ...")
    try:
        raw_files = save_raw_responses(payload, PATHS["raw"])
    except OSError as exc:
        logger.error("Could not write the raw JSON files: %s", exc)
        record("Save raw JSON", "FAILED", str(exc))
        return 1
    ok(f"{len(raw_files)} JSON file(s) written ({payload['total_api_requests']} API request(s))")
    record("Save raw JSON", "OK", f"{len(raw_files)} files")

    # ---------------- stage 8 : transformation ---------------------------
    stage(8, "Cleaning and transforming the extracted datasets with Pandas ...")
    try:
        transformation = transform_all(payload)
        tables = transformation.tables
    except Exception as exc:  # transformation bugs must stop the pipeline
        logger.exception("Transformation failed")
        record("Clean and transform", "FAILED", str(exc))
        return 1

    for name in TABLE_NAMES:
        dataframe = tables.get(name, pd.DataFrame())
        print(f"      {name:<26} {len(dataframe):>6} clean row(s)")
    record("Clean and transform", "OK", f"{sum(len(tables.get(n, [])) for n in TABLE_NAMES)} rows")

    # ---------------- stage 9 : data quality -----------------------------
    stage(9, "Performing data-quality checks ...")
    quality_report = build_quality_report(transformation)
    print(quality_report.summary_text())
    problems = run_quality_checks(
        {name: tables.get(name, pd.DataFrame()) for name in TABLE_NAMES},
        PRIMARY_KEYS,
        REQUIRED_COLUMNS,
    )
    if problems:
        print(f"      {len(problems)} data-quality warning(s):")
        for problem in problems:
            print(f"        - {problem}")
            logger.warning(problem)
        record("Data-quality checks", "WARNING", f"{len(problems)} warning(s)")
    else:
        ok("all checks passed (unique keys, required columns, no empty datasets)")
        record("Data-quality checks", "OK", "all checks passed")

    # ---------------- stage 10 : processed CSV ---------------------------
    stage(10, "Saving processed CSV files to data/processed/ ...")
    try:
        processed_files = save_processed_csvs(tables, PATHS["processed"])
        quality_path = quality_report.save_csv(
            os.path.join(PATHS["processed"], "data_quality_report.csv")
        )
        processed_files.append(quality_path)
    except OSError as exc:
        logger.error("Could not write the processed CSV files: %s", exc)
        record("Save processed CSV files", "FAILED", str(exc))
        return 1
    ok(f"{len(processed_files)} CSV file(s) written")
    record("Save processed CSV files", "OK", f"{len(processed_files)} files")

    # ---------------- stage 11 : rejected records ------------------------
    stage(11, "Saving rejected records to data/rejected/ ...")
    try:
        rejected_files = save_rejected_records(transformation.rejected, PATHS["rejected"])
    except OSError as exc:
        logger.error("Could not write the rejected records: %s", exc)
        record("Save rejected records", "FAILED", str(exc))
        return 1
    total_rejected = sum(len(frame) for frame in transformation.rejected.values() if frame is not None)
    record("Save rejected records", "OK", f"{total_rejected} rejected row(s)")

    # ---------------- stages 12-14 : PostgreSQL --------------------------
    if args.skip_db:
        stage(12, "Connecting to PostgreSQL ...")
        print("      -> SKIPPED because --skip-db was used")
        for name in ["Connect to PostgreSQL", "Create tables", "Load data"]:
            record(name, "SKIPPED", "--skip-db")
    else:
        try:
            db_counts = run_database_stages(tables)
        except Exception as exc:  # DatabaseConnectionError and friends
            print(f"\n      [FAILED] {exc}")
            logger.error("Database stage failed: %s", exc)
            record("Connect to PostgreSQL / create tables / load", "FAILED", str(exc).splitlines()[0])
            print("\n      The data WAS cleaned and saved to CSV, but it was NOT loaded into PostgreSQL.")
            print("      The pipeline therefore reports FAILURE. Fix the connection settings in .env")
            print("      (see README section 'Configuring your PostgreSQL credentials') and run again.")
            print_summary(statuses, tables, time.time() - started, None, args.skip_db)
            return 1
        record("Connect to PostgreSQL", "OK", "connection verified")
        record("Create tables", "OK", "schema.sql applied")
        record("Load data", "OK", f"{sum(db_counts.values())} row(s) in the database")

    # ---------------- stage 15 : charts ----------------------------------
    stage(15, "Generating static charts in reports/ ...")
    try:
        charts = generate_all_charts(PATHS["processed"], PATHS["reports"])
        for chart in charts:
            print(f"      {os.path.basename(chart)}")
        record("Generate charts", "OK", f"{len(charts)} PNG file(s)")
    except Exception as exc:  # charts are not essential for the data load
        logger.warning("Chart generation failed: %s", exc)
        print(f"      [WARNING] charts could not be generated: {exc}")
        record("Generate charts", "WARNING", str(exc))

    # ---------------- stage 16 : summary --------------------------------
    elapsed = time.time() - started
    print_summary(statuses, tables, elapsed, db_counts, args.skip_db)

    failed = [name for name, status, _ in statuses if status == "FAILED"]
    if failed:
        print("\n      RESULT: PIPELINE FAILED")
        return 1

    print("\n      RESULT: PIPELINE COMPLETED SUCCESSFULLY")
    print("      Next step: run the queries in analysis/business_queries.sql inside pgAdmin.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
