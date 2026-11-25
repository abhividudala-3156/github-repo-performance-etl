"""Data cleaning and transformation (the TRANSFORM stage of the ETL pipeline).

Every function in this module takes the raw JSON records that the extractor
saved and returns tidy pandas DataFrames that match the PostgreSQL schema.

Cleaning rules applied (documented for the report)
--------------------------------------------------
1.  Duplicate records are removed using the natural key of each dataset
    (repo_id, commit sha, issue id, pull-request id, contributor id).
2.  Column names are standardised to lower_case_with_underscores.
3.  Timestamps are converted to pandas datetime (UTC, stored as naive UTC so
    they fit a PostgreSQL TIMESTAMP column).
4.  Count columns are converted to nullable integers (Int64).
5.  Contributor usernames are trimmed and lower-cased (GitHub logins are
    case-insensitive, so this avoids "Torvalds" and "torvalds" being counted
    as two different people).
6.  Repository names are validated against the owner/repository pattern and
    the numeric GitHub ids must be positive.
7.  Issues are classified as Open / Closed.
8.  Pull requests are classified as Open / Closed / Merged.
9.  Pull requests that the /issues endpoint returns are detected and moved out
    of the issues dataset (they are NOT counted as ordinary issues).
10. Missing values are NOT simply dropped. Each column has an explicit rule:
    text columns get a readable placeholder, numeric columns get 0, and only
    records that are unusable (no id, no date) are rejected and written to
    data/rejected/ with a rejection reason.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from extraction.github_extractor import is_pull_request_record
from transformation.data_quality import count_duplicates, count_missing_values, missing_value_summary

logger = logging.getLogger(__name__)

# owner/repository: letters, digits, dot, underscore and hyphen only
REPOSITORY_NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
VALID_ISSUE_STATES = {"open", "closed"}
VALID_PR_STATES = {"open", "closed"}

PLACEHOLDER_DESCRIPTION = "No description provided"
PLACEHOLDER_LANGUAGE = "Unknown"
PLACEHOLDER_LICENSE = "No license"
PLACEHOLDER_AUTHOR = "unknown"
PLACEHOLDER_TITLE = "Untitled"


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------
@dataclass
class CleanResult:
    """Outcome of cleaning one dataset."""

    dataset: str
    data: pd.DataFrame
    rejected: pd.DataFrame
    records_extracted: int = 0
    duplicates_removed: int = 0
    records_rejected: int = 0
    records_excluded: int = 0
    missing_values_filled: int = 0
    notes: str = ""

    @property
    def records_loaded(self) -> int:
        return len(self.data)

    def missing_after(self) -> Dict[str, int]:
        return missing_value_summary(self.data)


@dataclass
class TransformationResult:
    """All processed tables plus the per-dataset cleaning statistics."""

    tables: Dict[str, pd.DataFrame] = field(default_factory=dict)
    results: List[CleanResult] = field(default_factory=list)
    rejected: Dict[str, pd.DataFrame] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------
def standardize_column_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Convert column names to lower_case_with_underscores."""
    renamed = {}
    for column in dataframe.columns:
        new_name = str(column).strip().lower()
        new_name = re.sub(r"[^0-9a-z]+", "_", new_name).strip("_")
        renamed[column] = new_name
    return dataframe.rename(columns=renamed)


def standardize_username(value: Any) -> Optional[str]:
    """Trim, drop a leading '@' and lower-case a GitHub login."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    username = str(value).strip().lstrip("@").lower()
    return username or None


def to_datetime_column(series: pd.Series) -> pd.Series:
    """Convert an ISO-8601 text column to naive UTC datetime64."""
    converted = pd.to_datetime(series, errors="coerce", utc=True, format="ISO8601")
    try:
        return converted.dt.tz_localize(None)
    except (AttributeError, TypeError):  # all values were NaT
        return pd.to_datetime(converted, errors="coerce")


def to_int_column(series: pd.Series) -> pd.Series:
    """Convert a column to nullable integers (invalid values become <NA>)."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def is_valid_repository_name(full_name: Any) -> bool:
    """True when the value looks like 'owner/repository'."""
    return isinstance(full_name, str) and bool(REPOSITORY_NAME_PATTERN.match(full_name.strip()))


def _split_by_rejection_reason(dataframe: pd.DataFrame, reason_column: str = "_rejection_reason"):
    """Split a dataframe into (valid rows, rejected rows) using a reason column."""
    rejected_mask = dataframe[reason_column].fillna("") != ""
    rejected = dataframe.loc[rejected_mask].copy()
    rejected = rejected.rename(columns={reason_column: "rejection_reason"})
    valid = dataframe.loc[~rejected_mask].copy()
    valid = valid.drop(columns=[reason_column], errors="ignore")
    return valid.reset_index(drop=True), rejected.reset_index(drop=True)


def _empty_rejected(columns: List[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns) + ["rejection_reason"])


def _as_records(records: Any) -> List[Dict[str, Any]]:
    """Keep only dictionary records (protects against a malformed API answer)."""
    if not isinstance(records, list):
        return []
    return [record for record in records if isinstance(record, dict)]


# ---------------------------------------------------------------------------
# 1. Repositories
# ---------------------------------------------------------------------------
def clean_repositories(
    records: List[Dict[str, Any]],
    extracted_at: Optional[str] = None,
) -> CleanResult:
    """Clean the /repos/{owner}/{repo} responses (one record per repository)."""
    rows = []
    for record in _as_records(records):
        owner = record.get("owner") or {}
        license_info = record.get("license") or {}
        rows.append(
            {
                "repo_id": record.get("id"),
                "full_name": record.get("full_name"),
                "owner_login": owner.get("login"),
                "repo_name": record.get("name"),
                "description": record.get("description"),
                "language": record.get("language"),
                "stars": record.get("stargazers_count"),
                "forks": record.get("forks_count"),
                "watchers": record.get("subscribers_count", record.get("watchers_count")),
                "open_issues_count": record.get("open_issues_count"),
                "size_kb": record.get("size"),
                "default_branch": record.get("default_branch"),
                "license_name": license_info.get("spdx_id") or license_info.get("name"),
                "is_fork": record.get("fork", False),
                "created_at": record.get("created_at"),
                "updated_at": record.get("updated_at"),
                "pushed_at": record.get("pushed_at"),
                "html_url": record.get("html_url"),
                "extracted_at": extracted_at,
            }
        )

    if not rows:
        return CleanResult(
            dataset="repositories",
            data=pd.DataFrame(),
            rejected=_empty_rejected(["full_name"]),
            notes="No repository records were available.",
        )

    dataframe = standardize_column_names(pd.DataFrame(rows))

    # --- duplicate removal -------------------------------------------------
    before = len(dataframe)
    dataframe = dataframe.drop_duplicates(subset=["repo_id"], keep="first").reset_index(drop=True)
    duplicates_removed = before - len(dataframe)

    # --- type conversion ---------------------------------------------------
    dataframe["repo_id"] = to_int_column(dataframe["repo_id"])
    for column in ["stars", "forks", "watchers", "open_issues_count", "size_kb"]:
        dataframe[column] = to_int_column(dataframe[column])
    for column in ["created_at", "updated_at", "pushed_at"]:
        dataframe[column] = to_datetime_column(dataframe[column])

    # --- validation --------------------------------------------------------
    dataframe["_rejection_reason"] = ""
    dataframe.loc[dataframe["repo_id"].isna() | (dataframe["repo_id"] <= 0), "_rejection_reason"] = (
        "missing or invalid repository id"
    )
    dataframe.loc[
        (dataframe["_rejection_reason"] == "")
        & ~dataframe["full_name"].apply(is_valid_repository_name),
        "_rejection_reason",
    ] = "invalid repository name (expected owner/repository)"
    dataframe.loc[
        (dataframe["_rejection_reason"] == "") & dataframe["repo_name"].isna(), "_rejection_reason"
    ] = "missing repository name"

    valid, rejected = _split_by_rejection_reason(dataframe)

    # --- missing value handling -------------------------------------------
    missing_before = count_missing_values(valid)
    valid["description"] = valid["description"].fillna(PLACEHOLDER_DESCRIPTION)
    valid["language"] = valid["language"].fillna(PLACEHOLDER_LANGUAGE)
    valid["license_name"] = valid["license_name"].fillna(PLACEHOLDER_LICENSE)
    valid["owner_login"] = valid["owner_login"].fillna(
        valid["full_name"].str.split("/").str[0]
    )
    for column in ["stars", "forks", "watchers", "open_issues_count", "size_kb"]:
        valid[column] = valid[column].fillna(0).astype("Int64")
    valid["is_fork"] = valid["is_fork"].fillna(False).astype(bool)
    valid["created_at"] = valid["created_at"].fillna(valid["updated_at"])
    missing_after = count_missing_values(valid)

    valid = valid[
        [
            "repo_id",
            "full_name",
            "owner_login",
            "repo_name",
            "description",
            "language",
            "stars",
            "forks",
            "watchers",
            "open_issues_count",
            "size_kb",
            "default_branch",
            "license_name",
            "is_fork",
            "created_at",
            "updated_at",
            "pushed_at",
            "html_url",
            "extracted_at",
        ]
    ]

    return CleanResult(
        dataset="repositories",
        data=valid,
        rejected=rejected,
        records_extracted=len(rows),
        duplicates_removed=duplicates_removed,
        records_rejected=len(rejected),
        missing_values_filled=max(missing_before - missing_after, 0),
        notes="Text placeholders and zero counts used for missing values; "
        "created_at filled from updated_at when empty.",
    )


# ---------------------------------------------------------------------------
# 2. Commits
# ---------------------------------------------------------------------------
def clean_commits(records: List[Dict[str, Any]], repo_id: int) -> CleanResult:
    """Clean the /commits responses of one repository."""
    rows = []
    for record in _as_records(records):
        commit_info = record.get("commit") or {}
        author_info = commit_info.get("author") or {}
        user = record.get("author") or {}
        message = (commit_info.get("message") or "").strip()
        rows.append(
            {
                "commit_sha": record.get("sha"),
                "repo_id": repo_id,
                "author_username": standardize_username(user.get("login")),
                "author_name": author_info.get("name"),
                "author_email": author_info.get("email"),
                "authored_at": author_info.get("date"),
                "commit_message": message.splitlines()[0][:250] if message else "No commit message",
                "comment_count": commit_info.get("comment_count", 0),
                "html_url": record.get("html_url"),
            }
        )

    columns = [
        "commit_sha",
        "repo_id",
        "author_username",
        "author_name",
        "author_email",
        "authored_at",
        "commit_message",
        "comment_count",
        "html_url",
    ]

    if not rows:
        return CleanResult(
            dataset="commits",
            data=pd.DataFrame(columns=columns),
            rejected=_empty_rejected(columns),
            notes="No commit records were returned by the API.",
        )

    dataframe = standardize_column_names(pd.DataFrame(rows))

    before = len(dataframe)
    dataframe = dataframe.drop_duplicates(subset=["commit_sha"], keep="first").reset_index(drop=True)
    duplicates_removed = before - len(dataframe)

    dataframe["repo_id"] = to_int_column(dataframe["repo_id"])
    dataframe["comment_count"] = to_int_column(dataframe["comment_count"])
    dataframe["authored_at"] = to_datetime_column(dataframe["authored_at"])

    dataframe["_rejection_reason"] = ""
    dataframe.loc[
        dataframe["commit_sha"].isna() | (dataframe["commit_sha"].astype(str).str.len() < 7),
        "_rejection_reason",
    ] = "missing or invalid commit sha"
    dataframe.loc[
        (dataframe["_rejection_reason"] == "") & dataframe["repo_id"].isna(), "_rejection_reason"
    ] = "missing repository id"
    dataframe.loc[
        (dataframe["_rejection_reason"] == "") & dataframe["authored_at"].isna(), "_rejection_reason"
    ] = "missing or invalid commit date"

    valid, rejected = _split_by_rejection_reason(dataframe)

    missing_before = count_missing_values(valid)
    valid["author_username"] = valid["author_username"].fillna(PLACEHOLDER_AUTHOR)
    valid["author_name"] = valid["author_name"].fillna(PLACEHOLDER_AUTHOR)
    valid["author_email"] = valid["author_email"].fillna("not-provided@unknown")
    valid["comment_count"] = valid["comment_count"].fillna(0).astype("Int64")
    missing_after = count_missing_values(valid)

    valid = valid[columns]

    return CleanResult(
        dataset="commits",
        data=valid,
        rejected=rejected,
        records_extracted=len(rows),
        duplicates_removed=duplicates_removed,
        records_rejected=len(rejected),
        missing_values_filled=max(missing_before - missing_after, 0),
        notes="Commits without sha or authored_at are rejected; unknown authors "
        "are labelled 'unknown'.",
    )


# ---------------------------------------------------------------------------
# 3. Contributors
# ---------------------------------------------------------------------------
def clean_contributors(records: List[Dict[str, Any]], repo_id: int) -> CleanResult:
    """Clean the /contributors responses of one repository."""
    rows = []
    for record in _as_records(records):
        rows.append(
            {
                "contributor_id": record.get("id"),
                "repo_id": repo_id,
                "username": standardize_username(record.get("login")),
                "contributions": record.get("contributions"),
                "avatar_url": record.get("avatar_url"),
                "profile_url": record.get("html_url"),
                "account_type": record.get("type", "User"),
            }
        )

    columns = [
        "contributor_id",
        "repo_id",
        "username",
        "contributions",
        "avatar_url",
        "profile_url",
        "account_type",
    ]

    if not rows:
        return CleanResult(
            dataset="contributors",
            data=pd.DataFrame(columns=columns),
            rejected=_empty_rejected(columns),
            notes="No contributor records were returned by the API.",
        )

    dataframe = standardize_column_names(pd.DataFrame(rows))

    before = len(dataframe)
    dataframe = dataframe.drop_duplicates(subset=["contributor_id"], keep="first").reset_index(drop=True)
    duplicates_removed = before - len(dataframe)

    dataframe["contributor_id"] = to_int_column(dataframe["contributor_id"])
    dataframe["repo_id"] = to_int_column(dataframe["repo_id"])
    dataframe["contributions"] = to_int_column(dataframe["contributions"])

    dataframe["_rejection_reason"] = ""
    dataframe.loc[
        dataframe["contributor_id"].isna() | (dataframe["contributor_id"] <= 0), "_rejection_reason"
    ] = "anonymous contributor without a valid account id"
    dataframe.loc[
        (dataframe["_rejection_reason"] == "") & dataframe["username"].isna(), "_rejection_reason"
    ] = "missing contributor username"

    valid, rejected = _split_by_rejection_reason(dataframe)

    missing_before = count_missing_values(valid)
    valid["contributions"] = valid["contributions"].fillna(0).astype("Int64")
    valid["account_type"] = valid["account_type"].fillna("User")
    missing_after = count_missing_values(valid)

    valid = valid[columns]

    return CleanResult(
        dataset="contributors",
        data=valid,
        rejected=rejected,
        records_extracted=len(rows),
        duplicates_removed=duplicates_removed,
        records_rejected=len(rejected),
        missing_values_filled=max(missing_before - missing_after, 0),
        notes="Usernames trimmed and lower-cased; anonymous contributors rejected.",
    )


def build_contributors_table(contributors: pd.DataFrame, extracted_at: Optional[str] = None) -> pd.DataFrame:
    """Build the global `contributors` table (one row per GitHub account)."""
    columns = ["contributor_id", "username", "avatar_url", "profile_url", "account_type", "extracted_at"]
    if contributors is None or contributors.empty:
        return pd.DataFrame(columns=columns)

    unique = (
        contributors.sort_values("contributions", ascending=False)
        .drop_duplicates(subset=["contributor_id"], keep="first")
        .copy()
    )
    unique["extracted_at"] = extracted_at
    return unique[columns].reset_index(drop=True)


def build_repository_contributors(contributors: pd.DataFrame) -> pd.DataFrame:
    """Build the `repository_contributors` link table (repo <-> contributor)."""
    columns = ["repo_id", "contributor_id", "contributions"]
    if contributors is None or contributors.empty:
        return pd.DataFrame(columns=columns)

    link = (
        contributors.groupby(["repo_id", "contributor_id"], as_index=False)["contributions"]
        .sum()
        .sort_values(["repo_id", "contributions"], ascending=[True, False])
    )
    link["contributions"] = link["contributions"].astype("Int64")
    return link[columns].reset_index(drop=True)


# ---------------------------------------------------------------------------
# 4. Issues
# ---------------------------------------------------------------------------
def clean_issues(records: List[Dict[str, Any]], repo_id: int) -> CleanResult:
    """Clean the /issues responses of one repository.

    Records that GitHub returns with a 'pull_request' key are pull requests, not
    issues. They are excluded here so that issue counts stay correct.
    """
    rows = []
    excluded = 0
    for record in _as_records(records):
        if is_pull_request_record(record):
            excluded += 1  # handled by the pull_requests dataset instead
            continue
        labels = record.get("labels") or []
        label_names = [
            label.get("name") if isinstance(label, dict) else str(label) for label in labels
        ]
        user = record.get("user") or {}
        rows.append(
            {
                "issue_id": record.get("id"),
                "repo_id": repo_id,
                "issue_number": record.get("number"),
                "title": record.get("title"),
                "state": record.get("state"),
                "author_username": standardize_username(user.get("login")),
                "labels": ",".join([name for name in label_names if name]),
                "comments": record.get("comments", 0),
                "created_at": record.get("created_at"),
                "closed_at": record.get("closed_at"),
                "is_pull_request": False,
                "html_url": record.get("html_url"),
            }
        )

    columns = [
        "issue_id",
        "repo_id",
        "issue_number",
        "title",
        "state",
        "state_category",
        "is_closed",
        "author_username",
        "labels",
        "comments",
        "created_at",
        "closed_at",
        "is_pull_request",
        "html_url",
    ]

    if not rows:
        return CleanResult(
            dataset="issues",
            data=pd.DataFrame(columns=columns),
            rejected=_empty_rejected(columns),
            records_extracted=len(_as_records(records)),
            records_excluded=excluded,
            notes="No issue records were available (pull requests excluded).",
        )

    dataframe = standardize_column_names(pd.DataFrame(rows))

    before = len(dataframe)
    dataframe = dataframe.drop_duplicates(subset=["issue_id"], keep="first").reset_index(drop=True)
    duplicates_removed = before - len(dataframe)

    dataframe["issue_id"] = to_int_column(dataframe["issue_id"])
    dataframe["repo_id"] = to_int_column(dataframe["repo_id"])
    dataframe["issue_number"] = to_int_column(dataframe["issue_number"])
    dataframe["comments"] = to_int_column(dataframe["comments"])
    dataframe["created_at"] = to_datetime_column(dataframe["created_at"])
    dataframe["closed_at"] = to_datetime_column(dataframe["closed_at"])
    dataframe["state"] = dataframe["state"].astype("string").str.strip().str.lower()

    dataframe["_rejection_reason"] = ""
    dataframe.loc[
        dataframe["issue_id"].isna() | (dataframe["issue_id"] <= 0), "_rejection_reason"
    ] = "missing or invalid issue id"
    dataframe.loc[
        (dataframe["_rejection_reason"] == "") & dataframe["issue_number"].isna(), "_rejection_reason"
    ] = "missing issue number"
    dataframe.loc[
        (dataframe["_rejection_reason"] == "") & dataframe["created_at"].isna(), "_rejection_reason"
    ] = "missing or invalid creation date"

    valid, rejected = _split_by_rejection_reason(dataframe)

    missing_before = count_missing_values(valid)
    valid["title"] = valid["title"].fillna(PLACEHOLDER_TITLE)
    valid["author_username"] = valid["author_username"].fillna(PLACEHOLDER_AUTHOR)
    valid["labels"] = valid["labels"].fillna("")
    valid["comments"] = valid["comments"].fillna(0).astype("Int64")

    # --- issue classification (open / closed) ------------------------------
    valid["state"] = valid["state"].fillna("open")
    invalid_state = ~valid["state"].isin(VALID_ISSUE_STATES)
    if invalid_state.any():
        logger.warning("%s issue(s) had an unknown state and were set to 'open'", int(invalid_state.sum()))
        valid.loc[invalid_state, "state"] = "open"
    valid["state_category"] = valid["state"].map({"open": "Open", "closed": "Closed"})
    valid["is_closed"] = valid["state"].eq("closed")
    missing_after = count_missing_values(valid)

    valid = valid[columns]

    return CleanResult(
        dataset="issues",
        data=valid,
        rejected=rejected,
        records_extracted=len(_as_records(records)),
        duplicates_removed=duplicates_removed,
        records_rejected=len(rejected),
        records_excluded=excluded,
        missing_values_filled=max(missing_before - missing_after, 0),
        notes="Pull requests returned by /issues were excluded; states classified as Open/Closed.",
    )


# ---------------------------------------------------------------------------
# 5. Pull requests
# ---------------------------------------------------------------------------
def clean_pull_requests(records: List[Dict[str, Any]], repo_id: int) -> CleanResult:
    """Clean the /pulls responses of one repository."""
    rows = []
    for record in _as_records(records):
        user = record.get("user") or {}
        rows.append(
            {
                "pr_id": record.get("id"),
                "repo_id": repo_id,
                "pr_number": record.get("number"),
                "title": record.get("title"),
                "state": record.get("state"),
                "merged_at": record.get("merged_at"),
                "author_username": standardize_username(user.get("login")),
                "created_at": record.get("created_at"),
                "closed_at": record.get("closed_at"),
                "is_draft": record.get("draft", False),
                "html_url": record.get("html_url"),
            }
        )

    columns = [
        "pr_id",
        "repo_id",
        "pr_number",
        "title",
        "state",
        "state_category",
        "merged",
        "merged_at",
        "author_username",
        "created_at",
        "closed_at",
        "is_draft",
        "html_url",
    ]

    if not rows:
        return CleanResult(
            dataset="pull_requests",
            data=pd.DataFrame(columns=columns),
            rejected=_empty_rejected(columns),
            notes="No pull request records were returned by the API.",
        )

    dataframe = standardize_column_names(pd.DataFrame(rows))

    before = len(dataframe)
    dataframe = dataframe.drop_duplicates(subset=["pr_id"], keep="first").reset_index(drop=True)
    duplicates_removed = before - len(dataframe)

    dataframe["pr_id"] = to_int_column(dataframe["pr_id"])
    dataframe["repo_id"] = to_int_column(dataframe["repo_id"])
    dataframe["pr_number"] = to_int_column(dataframe["pr_number"])
    dataframe["created_at"] = to_datetime_column(dataframe["created_at"])
    dataframe["closed_at"] = to_datetime_column(dataframe["closed_at"])
    dataframe["merged_at"] = to_datetime_column(dataframe["merged_at"])
    dataframe["state"] = dataframe["state"].astype("string").str.strip().str.lower()

    dataframe["_rejection_reason"] = ""
    dataframe.loc[dataframe["pr_id"].isna() | (dataframe["pr_id"] <= 0), "_rejection_reason"] = (
        "missing or invalid pull request id"
    )
    dataframe.loc[
        (dataframe["_rejection_reason"] == "") & dataframe["pr_number"].isna(), "_rejection_reason"
    ] = "missing pull request number"
    dataframe.loc[
        (dataframe["_rejection_reason"] == "") & dataframe["created_at"].isna(), "_rejection_reason"
    ] = "missing or invalid creation date"

    valid, rejected = _split_by_rejection_reason(dataframe)

    missing_before = count_missing_values(valid)
    valid["title"] = valid["title"].fillna(PLACEHOLDER_TITLE)
    valid["author_username"] = valid["author_username"].fillna(PLACEHOLDER_AUTHOR)
    valid["is_draft"] = valid["is_draft"].fillna(False).astype(bool)

    # --- pull request classification (open / closed / merged) --------------
    valid["state"] = valid["state"].fillna("open")
    invalid_state = ~valid["state"].isin(VALID_PR_STATES)
    if invalid_state.any():
        logger.warning("%s pull request(s) had an unknown state and were set to 'open'", int(invalid_state.sum()))
        valid.loc[invalid_state, "state"] = "open"
    valid["merged"] = valid["merged_at"].notna()
    valid["state_category"] = np.select(
        [valid["merged"], valid["state"].eq("open")],
        ["Merged", "Open"],
        default="Closed",
    )
    missing_after = count_missing_values(valid)

    valid = valid[columns]

    return CleanResult(
        dataset="pull_requests",
        data=valid,
        rejected=rejected,
        records_extracted=len(_as_records(records)),
        duplicates_removed=duplicates_removed,
        records_rejected=len(rejected),
        missing_values_filled=max(missing_before - missing_after, 0),
        notes="A pull request is 'Merged' when merged_at is present, otherwise Open/Closed.",
    )


# ---------------------------------------------------------------------------
# 6. Repository level aggregates (answers the transformation questions)
# ---------------------------------------------------------------------------
def build_repository_summary(
    repositories: pd.DataFrame,
    commits: pd.DataFrame,
    contributors: pd.DataFrame,
    issues: pd.DataFrame,
    pull_requests: pd.DataFrame,
) -> pd.DataFrame:
    """One row per repository with the calculated metrics.

    Metrics: commit count, contributor count, open/closed issue counts and the
    pull-request merge count and percentage.
    """
    base_columns = [
        "repo_id",
        "full_name",
        "language",
        "stars",
        "forks",
        "commit_count",
        "contributor_count",
        "total_issues",
        "open_issues",
        "closed_issues",
        "total_pull_requests",
        "merged_pull_requests",
        "merge_percentage",
    ]
    if repositories is None or repositories.empty:
        return pd.DataFrame(columns=base_columns)

    summary = repositories[["repo_id", "full_name", "language", "stars", "forks"]].copy()

    def _count_per_repo(dataframe: pd.DataFrame, name: str) -> pd.DataFrame:
        if dataframe is None or dataframe.empty or "repo_id" not in dataframe.columns:
            return pd.DataFrame(columns=["repo_id", name])
        counts = dataframe.groupby("repo_id").size().reset_index(name=name)
        return counts

    commit_counts = _count_per_repo(commits, "commit_count")
    contributor_counts = _count_per_repo(
        contributors.drop_duplicates(subset=["repo_id", "contributor_id"])
        if contributors is not None and not contributors.empty
        else contributors,
        "contributor_count",
    )

    if issues is not None and not issues.empty:
        issue_counts = (
            issues.groupby("repo_id")
            .agg(
                total_issues=("issue_id", "count"),
                open_issues=("is_closed", lambda values: int((~values).sum())),
                closed_issues=("is_closed", lambda values: int(values.sum())),
            )
            .reset_index()
        )
    else:
        issue_counts = pd.DataFrame(columns=["repo_id", "total_issues", "open_issues", "closed_issues"])

    if pull_requests is not None and not pull_requests.empty:
        pr_counts = (
            pull_requests.groupby("repo_id")
            .agg(
                total_pull_requests=("pr_id", "count"),
                merged_pull_requests=("merged", lambda values: int(values.sum())),
            )
            .reset_index()
        )
    else:
        pr_counts = pd.DataFrame(columns=["repo_id", "total_pull_requests", "merged_pull_requests"])

    for frame in [commit_counts, contributor_counts, issue_counts, pr_counts]:
        summary = summary.merge(frame, on="repo_id", how="left")

    for column in [
        "commit_count",
        "contributor_count",
        "total_issues",
        "open_issues",
        "closed_issues",
        "total_pull_requests",
        "merged_pull_requests",
    ]:
        summary[column] = to_int_column(summary[column]).fillna(0).astype("Int64")

    summary["merge_percentage"] = np.where(
        summary["total_pull_requests"] > 0,
        (summary["merged_pull_requests"] / summary["total_pull_requests"] * 100).round(2),
        0.0,
    )

    return summary[base_columns].sort_values("stars", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 7. Orchestration used by main.py
# ---------------------------------------------------------------------------
def transform_all(raw_payload: Dict[str, Any]) -> TransformationResult:
    """Clean every dataset of the extracted payload.

    `raw_payload` is the dictionary returned by GitHubExtractor.extract_all().
    """
    result = TransformationResult()
    repository_payloads = raw_payload.get("repositories", []) if isinstance(raw_payload, dict) else []

    repository_records = []
    commit_frames, contributor_frames, issue_frames, pr_frames = [], [], [], []
    rejected_frames: Dict[str, List[pd.DataFrame]] = {
        "commits": [],
        "contributors": [],
        "issues": [],
        "pull_requests": [],
    }

    for payload in repository_payloads:
        metadata = payload.get("metadata", {})
        extracted_at = metadata.get("extraction_timestamp_utc")
        repository = payload.get("repository") or {}

        repository_records.append(repository)

        repo_id = repository.get("id")
        if repo_id is None:
            logger.warning("Skipping %s: no repository id in the response", metadata.get("requested_repository"))
            continue

        commit_result = clean_commits(payload.get("commits", []), repo_id)
        contributor_result = clean_contributors(payload.get("contributors", []), repo_id)
        issue_result = clean_issues(payload.get("issues", []), repo_id)
        pr_result = clean_pull_requests(payload.get("pull_requests", []), repo_id)

        commit_frames.append(commit_result.data)
        contributor_frames.append(contributor_result.data)
        issue_frames.append(issue_result.data)
        pr_frames.append(pr_result.data)

        result.results.extend([commit_result, contributor_result, issue_result, pr_result])
        # rejected records are collected per repository and combined at the end
        rejected_frames["commits"].append(commit_result.rejected)
        rejected_frames["contributors"].append(contributor_result.rejected)
        rejected_frames["issues"].append(issue_result.rejected)
        rejected_frames["pull_requests"].append(pr_result.rejected)

    # --- repositories -------------------------------------------------------
    repository_result = clean_repositories(
        repository_records,
        extracted_at=(repository_payloads[0].get("metadata", {}).get("extraction_timestamp_utc") if repository_payloads else None),
    )
    result.results.insert(0, repository_result)
    result.rejected["repositories"] = repository_result.rejected
    repositories_df = repository_result.data

    # combine the rejected records of every repository into one frame per dataset
    for dataset_name, frames in rejected_frames.items():
        non_empty = [frame for frame in frames if frame is not None and not frame.empty]
        if non_empty:
            result.rejected[dataset_name] = pd.concat(non_empty, ignore_index=True)
        elif frames:
            result.rejected[dataset_name] = frames[0]

    # --- combine per repository frames -------------------------------------
    def _concat(frames: List[pd.DataFrame], columns: List[str]) -> pd.DataFrame:
        non_empty = [frame for frame in frames if frame is not None and not frame.empty]
        if not non_empty:
            return pd.DataFrame(columns=columns)
        return pd.concat(non_empty, ignore_index=True)

    commits_df = _concat(commit_frames, ["commit_sha", "repo_id"])
    contributors_df = _concat(contributor_frames, ["contributor_id", "repo_id", "username"])
    issues_df = _concat(issue_frames, ["issue_id", "repo_id"])
    pull_requests_df = _concat(pr_frames, ["pr_id", "repo_id"])

    # keep only rows whose repository survived cleaning (foreign-key safety)
    valid_repo_ids = set(repositories_df["repo_id"].dropna().astype(int)) if not repositories_df.empty else set()
    if valid_repo_ids:
        for frame in (commits_df, contributors_df, issues_df, pull_requests_df):
            if not frame.empty:
                frame.drop(frame[~frame["repo_id"].astype("Int64").isin(valid_repo_ids)].index, inplace=True)

    # --- global contributor tables ----------------------------------------
    extracted_at = repository_payloads[0].get("metadata", {}).get("extraction_timestamp_utc") if repository_payloads else None
    contributors_table = build_contributors_table(contributors_df, extracted_at=extracted_at)
    repository_contributors = build_repository_contributors(contributors_df)

    duplicate_contributors = max(len(contributors_df) - len(contributors_table), 0)
    result.results.append(
        CleanResult(
            dataset="contributors (unique accounts)",
            data=contributors_table,
            rejected=_empty_rejected(["contributor_id"]),
            records_extracted=len(contributors_df),
            duplicates_removed=duplicate_contributors,
            notes="Same account appears once per repository; de-duplicated here.",
        )
    )

    summary = build_repository_summary(
        repositories_df, commits_df, contributors_df, issues_df, pull_requests_df
    )

    result.tables = {
        "repositories": repositories_df,
        "contributors": contributors_table,
        "repository_contributors": repository_contributors,
        "commits": commits_df.reset_index(drop=True),
        "issues": issues_df.reset_index(drop=True),
        "pull_requests": pull_requests_df.reset_index(drop=True),
        "repository_summary": summary,
    }
    return result
