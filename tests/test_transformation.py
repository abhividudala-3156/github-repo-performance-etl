"""Tests for the transformation (cleaning) and data-quality modules.

These tests use small in-memory dictionaries that look exactly like GitHub API
responses, so no network access and no database are required.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from transformation.cleaning import (
    build_repository_summary,
    clean_commits,
    clean_contributors,
    clean_issues,
    clean_pull_requests,
    clean_repositories,
    standardize_column_names,
    standardize_username,
    to_datetime_column,
    transform_all,
)
from transformation.data_quality import (
    DataQualityReport,
    DatasetQuality,
    count_duplicates,
    missing_value_summary,
    run_quality_checks,
)


# ---------------------------------------------------------------------------
# Raw sample records (same shape as the GitHub API)
# ---------------------------------------------------------------------------
def repo_record(repo_id=101, full_name="psf/requests", stars=54000, description="HTTP library"):
    owner, name = full_name.split("/")
    return {
        "id": repo_id,
        "name": name,
        "full_name": full_name,
        "owner": {"login": owner},
        "description": description,
        "language": "Python",
        "stargazers_count": stars,
        "forks_count": 9800,
        "watchers_count": stars,
        "subscribers_count": 1500,
        "open_issues_count": 300,
        "size": 30000,
        "default_branch": "main",
        "license": {"spdx_id": "Apache-2.0"},
        "fork": False,
        "created_at": "2011-02-13T18:38:17Z",
        "updated_at": "2025-01-01T10:00:00Z",
        "pushed_at": "2025-01-02T11:00:00Z",
        "html_url": "https://github.com/" + full_name,
    }


def commit_record(sha="a" * 40, date="2025-01-05T10:00:00Z", login="KennethReitz"):
    return {
        "sha": sha,
        "commit": {
            "author": {"name": "Kenneth Reitz", "email": "me@example.com", "date": date},
            "message": "Fix connection pool\n\nLong description",
            "comment_count": 2,
        },
        "author": {"login": login} if login else None,
        "html_url": "https://github.com/psf/requests/commit/" + str(sha),
    }


def contributor_record(contributor_id=7, login="KennethReitz", contributions=100):
    return {
        "id": contributor_id,
        "login": login,
        "contributions": contributions,
        "avatar_url": "https://avatars.githubusercontent.com/u/7",
        "html_url": "https://github.com/" + login,
        "type": "User",
    }


def issue_record(issue_id=11, number=1, state="open", is_pr=False):
    record = {
        "id": issue_id,
        "number": number,
        "title": "Something is broken",
        "state": state,
        "user": {"login": "Reporter"},
        "labels": [{"name": "bug"}],
        "comments": 3,
        "created_at": "2025-01-03T09:00:00Z",
        "closed_at": "2025-01-09T09:00:00Z" if state == "closed" else None,
        "html_url": "https://github.com/psf/requests/issues/1",
    }
    if is_pr:
        record["pull_request"] = {"url": "https://api.github.com/repos/psf/requests/pulls/1"}
    return record


def pr_record(pr_id=21, number=2, state="closed", merged=True):
    return {
        "id": pr_id,
        "number": number,
        "title": "Improve docs",
        "state": state,
        "merged_at": "2025-01-07T09:00:00Z" if merged else None,
        "user": {"login": "Contributor"},
        "created_at": "2025-01-04T09:00:00Z",
        "closed_at": "2025-01-07T09:00:00Z" if state == "closed" else None,
        "draft": False,
        "html_url": "https://github.com/psf/requests/pull/2",
    }


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------
def test_standardize_column_names():
    dataframe = pd.DataFrame({"Repo ID": [1], "Full Name ": ["a/b"], "STARS": [5]})
    cleaned = standardize_column_names(dataframe)
    assert list(cleaned.columns) == ["repo_id", "full_name", "stars"]


def test_standardize_username():
    assert standardize_username("  KennethReitz ") == "kennethreitz"
    assert standardize_username("@psf") == "psf"
    assert standardize_username(None) is None
    assert standardize_username("   ") is None


def test_to_datetime_column_parses_and_coerces():
    series = pd.Series(["2025-01-05T10:00:00Z", "not-a-date", None])
    converted = to_datetime_column(series)

    assert converted.dtype.kind == "M"                  # datetime64
    assert converted.iloc[0] == pd.Timestamp("2025-01-05 10:00:00")
    assert pd.isna(converted.iloc[1])                    # invalid -> NaT
    assert pd.isna(converted.iloc[2])


# ---------------------------------------------------------------------------
# Repositories
# ---------------------------------------------------------------------------
def test_clean_repositories_removes_duplicates_and_fills_missing():
    records = [
        repo_record(description=None),
        repo_record(description=None),  # exact duplicate -> removed
    ]
    result = clean_repositories(records, extracted_at="2025-01-10T00:00:00Z")

    assert result.records_extracted == 2
    assert result.duplicates_removed == 1
    assert len(result.data) == 1
    assert result.data.loc[0, "description"] == "No description provided"
    assert result.data.loc[0, "stars"] == 54000
    assert result.data.loc[0, "repo_id"] == 101
    assert str(result.data["created_at"].dtype).startswith("datetime64")


def test_clean_repositories_rejects_invalid_names_and_ids():
    records = [
        repo_record(repo_id=1, full_name="psf/requests"),
        {**repo_record(repo_id=2), "full_name": "not a valid name"},   # invalid name
        {**repo_record(repo_id=None, full_name="owner/repo")},        # missing id
    ]
    result = clean_repositories(records)

    assert len(result.data) == 1
    assert result.records_rejected == 2
    assert "rejection_reason" in result.rejected.columns
    assert set(result.rejected["rejection_reason"]) == {
        "invalid repository name (expected owner/repository)",
        "missing or invalid repository id",
    }


def test_clean_repositories_handles_empty_input():
    result = clean_repositories([])
    assert result.data.empty
    assert result.records_extracted == 0


# ---------------------------------------------------------------------------
# Commits
# ---------------------------------------------------------------------------
def test_clean_commits_removes_duplicates_and_converts_types():
    records = [commit_record(), commit_record(), commit_record(sha="b" * 40)]
    result = clean_commits(records, repo_id=101)

    assert result.duplicates_removed == 1
    assert len(result.data) == 2
    assert result.data["comment_count"].tolist() == [2, 2]
    assert result.data.loc[0, "author_username"] == "kennethreitz"
    assert result.data.loc[0, "commit_message"] == "Fix connection pool"   # first line only


def test_clean_commits_rejects_records_without_date_or_sha():
    records = [
        commit_record(),
        commit_record(sha="c" * 40, date=None),        # no date -> rejected
        {"sha": None, "commit": {"author": {"date": "2025-01-05T10:00:00Z"}}},
    ]
    result = clean_commits(records, repo_id=101)

    assert len(result.data) == 1
    assert result.records_rejected == 2
    assert "missing or invalid commit date" in set(result.rejected["rejection_reason"])


def test_clean_commits_fills_unknown_author():
    result = clean_commits([commit_record(login=None)], repo_id=101)
    assert result.data.loc[0, "author_username"] == "unknown"


# ---------------------------------------------------------------------------
# Contributors
# ---------------------------------------------------------------------------
def test_clean_contributors_standardises_usernames_and_rejects_anonymous():
    records = [
        contributor_record(login="KennethReitz"),
        contributor_record(login="kennethreitz"),          # same id -> duplicate
        {"id": None, "login": None, "contributions": 5},   # anonymous -> rejected
    ]
    result = clean_contributors(records, repo_id=101)

    assert len(result.data) == 1
    assert result.data.loc[0, "username"] == "kennethreitz"
    assert result.duplicates_removed == 1
    assert result.records_rejected == 1


# ---------------------------------------------------------------------------
# Issues
# ---------------------------------------------------------------------------
def test_clean_issues_excludes_pull_requests_from_the_issues_endpoint():
    records = [
        issue_record(issue_id=11, number=1, state="open"),
        issue_record(issue_id=12, number=2, state="closed"),
        issue_record(issue_id=13, number=3, state="open", is_pr=True),  # a pull request!
    ]
    result = clean_issues(records, repo_id=101)

    assert len(result.data) == 2
    assert result.records_excluded == 1
    assert result.data["is_pull_request"].sum() == 0
    assert 13 not in result.data["issue_id"].tolist()


def test_clean_issues_classifies_open_and_closed():
    records = [
        issue_record(issue_id=11, number=1, state="open"),
        issue_record(issue_id=12, number=2, state="closed"),
    ]
    result = clean_issues(records, repo_id=101)
    data = result.data.set_index("issue_id")

    assert data.loc[11, "state_category"] == "Open"
    assert bool(data.loc[11, "is_closed"]) is False
    assert data.loc[12, "state_category"] == "Closed"
    assert bool(data.loc[12, "is_closed"]) is True
    assert data.loc[11, "labels"] == "bug"


def test_clean_issues_rejects_records_without_id():
    result = clean_issues([{"id": None, "number": None, "state": "open"}], repo_id=101)
    assert result.data.empty
    assert result.records_rejected == 1


# ---------------------------------------------------------------------------
# Pull requests
# ---------------------------------------------------------------------------
def test_clean_pull_requests_classifies_open_closed_and_merged():
    records = [
        pr_record(pr_id=21, number=1, state="open", merged=False),
        pr_record(pr_id=22, number=2, state="closed", merged=False),
        pr_record(pr_id=23, number=3, state="closed", merged=True),
    ]
    result = clean_pull_requests(records, repo_id=101)
    data = result.data.set_index("pr_id")

    assert data.loc[21, "state_category"] == "Open"
    assert data.loc[22, "state_category"] == "Closed"
    assert data.loc[23, "state_category"] == "Merged"
    assert bool(data.loc[23, "merged"]) is True
    assert bool(data.loc[22, "merged"]) is False


def test_clean_pull_requests_removes_duplicates():
    records = [pr_record(pr_id=21), pr_record(pr_id=21)]
    result = clean_pull_requests(records, repo_id=101)
    assert len(result.data) == 1
    assert result.duplicates_removed == 1


# ---------------------------------------------------------------------------
# Repository summary (the calculated metrics)
# ---------------------------------------------------------------------------
def test_build_repository_summary_calculates_all_metrics():
    repositories = clean_repositories([repo_record()]).data
    commits = clean_commits([commit_record(), commit_record(sha="b" * 40)], repo_id=101).data
    contributors = pd.concat(
        [
            clean_contributors([contributor_record()], repo_id=101).data,
            clean_contributors([contributor_record(contributor_id=8, login="psf")], repo_id=101).data,
        ],
        ignore_index=True,
    )
    issues = clean_issues(
        [
            issue_record(issue_id=11, number=1, state="open"),
            issue_record(issue_id=12, number=2, state="closed"),
            issue_record(issue_id=13, number=3, state="closed"),
        ],
        repo_id=101,
    ).data
    pull_requests = clean_pull_requests(
        [
            pr_record(pr_id=21, number=1, state="closed", merged=True),
            pr_record(pr_id=22, number=2, state="open", merged=False),
            pr_record(pr_id=23, number=3, state="closed", merged=True),
            pr_record(pr_id=24, number=4, state="closed", merged=False),
        ],
        repo_id=101,
    ).data

    summary = build_repository_summary(repositories, commits, contributors, issues, pull_requests)
    row = summary.iloc[0]

    assert row["commit_count"] == 2
    assert row["contributor_count"] == 2
    assert row["total_issues"] == 3
    assert row["open_issues"] == 1
    assert row["closed_issues"] == 2
    assert row["total_pull_requests"] == 4
    assert row["merged_pull_requests"] == 2
    assert row["merge_percentage"] == 50.0


def test_build_repository_summary_handles_repository_without_pull_requests():
    repositories = clean_repositories([repo_record()]).data
    summary = build_repository_summary(
        repositories, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    )
    row = summary.iloc[0]

    assert row["commit_count"] == 0
    assert row["merge_percentage"] == 0.0


# ---------------------------------------------------------------------------
# Full transformation
# ---------------------------------------------------------------------------
def build_raw_payload():
    return {
        "extraction_timestamp_utc": "2025-01-10T00:00:00Z",
        "repositories": [
            {
                "metadata": {
                    "requested_repository": "psf/requests",
                    "extraction_timestamp_utc": "2025-01-10T00:00:00Z",
                },
                "repository": repo_record(),
                "commits": [commit_record(), commit_record(sha="b" * 40)],
                "contributors": [
                    contributor_record(),
                    contributor_record(contributor_id=8, login="PSF", contributions=50),
                ],
                "issues": [issue_record(), issue_record(issue_id=13, number=3, is_pr=True)],
                "pull_requests": [pr_record()],
            },
            {
                "metadata": {
                    "requested_repository": "numpy/numpy",
                    "extraction_timestamp_utc": "2025-01-10T00:00:00Z",
                },
                "repository": repo_record(repo_id=202, full_name="numpy/numpy", stars=26000),
                "commits": [commit_record(sha="d" * 40)],
                "contributors": [contributor_record(contributor_id=7, contributions=10)],
                "issues": [issue_record(issue_id=31, number=1, state="closed")],
                "pull_requests": [pr_record(pr_id=41, number=1, merged=False, state="open")],
            },
        ],
    }


def test_transform_all_builds_every_table():
    result = transform_all(build_raw_payload())

    for table in [
        "repositories",
        "contributors",
        "repository_contributors",
        "commits",
        "issues",
        "pull_requests",
        "repository_summary",
    ]:
        assert table in result.tables
        assert isinstance(result.tables[table], pd.DataFrame)

    assert len(result.tables["repositories"]) == 2
    assert len(result.tables["commits"]) == 3
    # contributor 7 works in both repositories -> 1 unique row, 2 links
    assert len(result.tables["contributors"]) == 2
    assert len(result.tables["repository_contributors"]) == 3
    # pull requests returned by /issues must not appear in the issues table
    assert len(result.tables["issues"]) == 2
    assert len(result.tables["pull_requests"]) == 2
    assert len(result.tables["repository_summary"]) == 2


def test_transform_all_keeps_only_rows_of_valid_repositories():
    payload = build_raw_payload()
    payload["repositories"][1]["repository"] = {"id": None, "full_name": "numpy/numpy"}

    result = transform_all(payload)

    assert len(result.tables["repositories"]) == 1
    assert result.tables["commits"]["repo_id"].nunique() == 1


def test_transform_all_reports_rejected_records():
    payload = build_raw_payload()
    payload["repositories"][0]["commits"].append(commit_record(sha="e" * 40, date=None))

    result = transform_all(payload)

    assert "commits" in result.rejected
    assert len(result.rejected["commits"]) == 1
    assert result.rejected["commits"].iloc[0]["rejection_reason"] == "missing or invalid commit date"


# ---------------------------------------------------------------------------
# Data-quality helpers
# ---------------------------------------------------------------------------
def test_count_duplicates_and_missing_summary():
    dataframe = pd.DataFrame({"a": [1, 1, 2], "b": [None, None, "x"]})
    assert count_duplicates(dataframe) == 1
    assert missing_value_summary(dataframe) == {"b": 2}


def test_run_quality_checks_passes_on_clean_data():
    tables = {
        "repositories": clean_repositories([repo_record()]).data,
        "commits": clean_commits([commit_record()], repo_id=101).data,
    }
    problems = run_quality_checks(
        tables,
        primary_keys={"repositories": "repo_id", "commits": "commit_sha"},
        required_columns={"repositories": ["repo_id", "full_name"]},
    )
    assert problems == []


def test_run_quality_checks_detects_duplicates_and_missing_columns():
    duplicated = pd.DataFrame({"repo_id": [1, 1], "stars": [1, 2]})
    problems = run_quality_checks(
        {"repositories": duplicated},
        primary_keys={"repositories": "repo_id"},
        required_columns={"repositories": ["repo_id", "full_name"]},
    )

    assert any("duplicate" in problem.lower() for problem in problems)
    assert any("full_name" in problem for problem in problems)


def test_data_quality_report_dataframe_and_summary():
    report = DataQualityReport()
    report.add(
        DatasetQuality(
            dataset="commits",
            records_extracted=10,
            records_loaded=9,
            duplicates_removed=1,
            records_rejected=0,
            missing_values_after={"author_email": 2},
        )
    )
    dataframe = report.to_dataframe()

    assert list(dataframe.columns) == DataQualityReport.COLUMNS
    assert dataframe.loc[0, "records_loaded"] == 9
    assert dataframe.loc[0, "missing_values_remaining"] == 2
    assert "commits" in report.summary_text()
