"""Tests for the extraction stage.

All tests use MOCKED HTTP responses - no internet connection and no GitHub
token are needed to run them.
"""

from __future__ import annotations

import json
import os

import pytest
import requests

from extraction.github_extractor import (
    GitHubExtractionError,
    GitHubExtractor,
    GitHubRateLimitError,
    is_pull_request_record,
    load_raw_responses,
    save_raw_responses,
    split_full_name,
)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------
class FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, payload, status_code=200, headers=None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers if headers is not None else {"X-RateLimit-Remaining": "4999"}
        self.text = json.dumps(payload) if payload is not None else ""

    def json(self):
        if self._payload is None:
            raise ValueError("Response body is not JSON")
        return self._payload


class FakeSession:
    """Minimal stand-in for requests.Session.

    `handler(url, params)` returns a FakeResponse or raises an exception.
    """

    def __init__(self, handler):
        self.handler = handler
        self.headers = {}
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params or {}, "timeout": timeout})
        result = self.handler(url, params or {})
        if isinstance(result, Exception):
            raise result
        return result


def make_extractor(handler, **kwargs):
    session = FakeSession(handler)
    extractor = GitHubExtractor(session=session, request_delay=0, **kwargs)
    return extractor, session


# --- sample API payloads ---------------------------------------------------
def sample_repository(repo_id=101, full_name="psf/requests"):
    owner, name = full_name.split("/")
    return {
        "id": repo_id,
        "name": name,
        "full_name": full_name,
        "owner": {"login": owner},
        "description": "A simple, yet elegant HTTP library.",
        "language": "Python",
        "stargazers_count": 54000,
        "forks_count": 9800,
        "watchers_count": 54000,
        "subscribers_count": 1500,
        "open_issues_count": 300,
        "size": 30000,
        "default_branch": "main",
        "license": {"spdx_id": "Apache-2.0", "name": "Apache License 2.0"},
        "fork": False,
        "created_at": "2011-02-13T18:38:17Z",
        "updated_at": "2025-01-01T10:00:00Z",
        "pushed_at": "2025-01-02T11:00:00Z",
        "html_url": "https://github.com/psf/requests",
    }


def sample_commit(sha="a" * 40, date="2025-01-05T10:00:00Z"):
    return {
        "sha": sha,
        "commit": {
            "author": {"name": "Kenneth Reitz", "email": "me@kennethreitz.org", "date": date},
            "message": "Fix connection pool\n\nMore details here",
            "comment_count": 0,
        },
        "author": {"login": "kennethreitz"},
        "html_url": "https://github.com/psf/requests/commit/" + sha,
    }


def sample_contributor(contributor_id=7, login="KennethReitz", contributions=100):
    return {
        "id": contributor_id,
        "login": login,
        "contributions": contributions,
        "avatar_url": "https://avatars.githubusercontent.com/u/7",
        "html_url": "https://github.com/" + login,
        "type": "User",
    }


def sample_issue(issue_id=11, number=1, state="open", is_pr=False):
    record = {
        "id": issue_id,
        "number": number,
        "title": "Something is broken",
        "state": state,
        "user": {"login": "reporter"},
        "labels": [{"name": "bug"}, {"name": "help wanted"}],
        "comments": 3,
        "created_at": "2025-01-03T09:00:00Z",
        "closed_at": "2025-01-09T09:00:00Z" if state == "closed" else None,
        "html_url": "https://github.com/psf/requests/issues/1",
    }
    if is_pr:
        record["pull_request"] = {"url": "https://api.github.com/repos/psf/requests/pulls/1"}
    return record


def sample_pull_request(pr_id=21, number=2, state="closed", merged=True):
    return {
        "id": pr_id,
        "number": number,
        "title": "Improve documentation",
        "state": state,
        "merged_at": "2025-01-07T09:00:00Z" if merged else None,
        "user": {"login": "contributor"},
        "created_at": "2025-01-04T09:00:00Z",
        "closed_at": "2025-01-07T09:00:00Z" if state == "closed" else None,
        "draft": False,
        "html_url": "https://github.com/psf/requests/pull/2",
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_split_full_name_accepts_owner_repo():
    assert split_full_name("pandas-dev/pandas") == ("pandas-dev", "pandas")
    assert split_full_name(" psf / requests ") == ("psf", "requests")


@pytest.mark.parametrize("value", ["pandas", "owner/repo/extra", "", "/requests", "psf/"])
def test_split_full_name_rejects_bad_values(value):
    with pytest.raises(GitHubExtractionError):
        split_full_name(value)


def test_is_pull_request_record_detects_pull_requests():
    assert is_pull_request_record(sample_issue(is_pr=True)) is True
    assert is_pull_request_record(sample_issue(is_pr=False)) is False


def test_get_repository_returns_json_and_uses_timeout():
    extractor, session = make_extractor(lambda url, params: FakeResponse(sample_repository()))

    data = extractor.get_repository("psf", "requests")

    assert data["full_name"] == "psf/requests"
    assert session.calls[0]["timeout"] == 30          # request timeout is set
    assert session.calls[0]["url"].endswith("/repos/psf/requests")


def test_pagination_follows_pages_until_short_page():
    pages = {
        1: [sample_commit(sha="1" * 40), sample_commit(sha="2" * 40)],
        2: [sample_commit(sha="3" * 40)],
    }

    def handler(url, params):
        return FakeResponse(pages[params["page"]])

    extractor, session = make_extractor(handler, per_page=2, max_pages=5)
    commits = extractor.get_commits("psf", "requests")

    assert len(commits) == 3
    assert len(session.calls) == 2          # stopped after the short page
    assert session.calls[0]["params"]["per_page"] == 2


def test_pagination_stops_at_max_pages():
    def handler(url, params):
        return FakeResponse([sample_commit(sha=str(params["page"]) * 40)])

    extractor, session = make_extractor(handler, per_page=1, max_pages=3)
    commits = extractor.get_commits("psf", "requests")

    assert len(commits) == 3
    assert len(session.calls) == 3


def test_timeout_raises_extraction_error():
    extractor, _ = make_extractor(lambda url, params: requests.exceptions.Timeout())

    with pytest.raises(GitHubExtractionError) as error:
        extractor.get_repository("psf", "requests")
    assert "timed out" in str(error.value).lower()


def test_http_404_raises_extraction_error():
    extractor, _ = make_extractor(
        lambda url, params: FakeResponse({"message": "Not Found"}, status_code=404)
    )

    with pytest.raises(GitHubExtractionError) as error:
        extractor.get_repository("psf", "does-not-exist")
    assert "404" in str(error.value)


def test_rate_limit_raises_dedicated_error():
    extractor, _ = make_extractor(
        lambda url, params: FakeResponse(
            {"message": "API rate limit exceeded"},
            status_code=403,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1795601453"},
        )
    )

    with pytest.raises(GitHubRateLimitError) as error:
        extractor.get_repository("psf", "requests")
    assert "rate limit" in str(error.value).lower()


def test_token_is_sent_as_bearer_header():
    extractor, session = make_extractor(
        lambda url, params: FakeResponse(sample_repository()), token="fake-token"
    )
    assert session.headers["Authorization"] == "Bearer fake-token"


def test_extract_repository_collects_all_five_datasets():
    def handler(url, params):
        if url.endswith("/commits"):
            return FakeResponse([sample_commit()])
        if url.endswith("/contributors"):
            return FakeResponse([sample_contributor()])
        if url.endswith("/issues"):
            return FakeResponse([sample_issue(), sample_issue(issue_id=12, number=2, is_pr=True)])
        if url.endswith("/pulls"):
            return FakeResponse([sample_pull_request()])
        return FakeResponse(sample_repository())

    extractor, _ = make_extractor(handler)
    payload = extractor.extract_repository("psf/requests")

    assert payload["repository"]["full_name"] == "psf/requests"
    assert len(payload["commits"]) == 1
    assert len(payload["contributors"]) == 1
    assert len(payload["issues"]) == 2
    assert len(payload["pull_requests"]) == 1
    # the pull request inside /issues is detected and counted
    assert payload["metadata"]["records"]["pull_requests_inside_issues"] == 1
    assert payload["metadata"]["extraction_timestamp_utc"] is not None


def test_extract_all_reports_failures_but_keeps_good_repositories():
    def handler(url, params):
        if "broken" in url:
            return FakeResponse({"message": "Not Found"}, status_code=404)
        if url.endswith("/commits"):
            return FakeResponse([sample_commit()])
        if url.endswith("/contributors"):
            return FakeResponse([])
        if url.endswith("/issues"):
            return FakeResponse([])
        if url.endswith("/pulls"):
            return FakeResponse([])
        return FakeResponse(sample_repository())

    extractor, _ = make_extractor(handler)
    payload = extractor.extract_all(["psf/requests", "owner/broken"])

    assert len(payload["repositories"]) == 1
    assert len(payload["failures"]) == 1
    assert payload["failures"][0]["repository"] == "owner/broken"


def test_extract_all_raises_when_every_repository_fails():
    extractor, _ = make_extractor(
        lambda url, params: FakeResponse({"message": "Not Found"}, status_code=404)
    )

    with pytest.raises(GitHubExtractionError):
        extractor.extract_all(["owner/broken"])


def test_save_and_load_raw_responses(tmp_path):
    def handler(url, params):
        if url.endswith("/commits"):
            return FakeResponse([sample_commit()])
        if url.endswith("/contributors"):
            return FakeResponse([sample_contributor()])
        if url.endswith("/issues"):
            return FakeResponse([sample_issue()])
        if url.endswith("/pulls"):
            return FakeResponse([sample_pull_request()])
        return FakeResponse(sample_repository())

    extractor, _ = make_extractor(handler)
    payload = extractor.extract_all(["psf/requests"])

    written = save_raw_responses(payload, str(tmp_path))
    names = sorted(os.path.basename(path) for path in written)

    assert "psf__requests.json" in names
    assert "extraction_manifest.json" in names

    with open(tmp_path / "psf__requests.json", encoding="utf-8") as handle:
        saved = json.load(handle)
    assert saved["metadata"]["extraction_timestamp_utc"] == payload["extraction_timestamp_utc"]
    assert len(saved["commits"]) == 1

    reloaded = load_raw_responses(str(tmp_path))
    assert len(reloaded["repositories"]) == 1
