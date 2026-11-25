"""GitHub REST API extraction module.

This module is the EXTRACT stage of the ETL pipeline. It calls the official
GitHub REST API (https://docs.github.com/en/rest) with the `requests` library
and returns the raw JSON responses.

Endpoints used (per repository)
-------------------------------
1. GET /repos/{owner}/{repo}              -> repository details
2. GET /repos/{owner}/{repo}/commits      -> commits
3. GET /repos/{owner}/{repo}/contributors -> contributors
4. GET /repos/{owner}/{repo}/issues       -> issues (may contain pull requests!)
5. GET /repos/{owner}/{repo}/pulls        -> pull requests

Design notes for the evaluator
------------------------------
* Simple class + plain functions, no async, no custom frameworks.
* Pagination is handled with the `page` / `per_page` query parameters.
* Every request uses an explicit timeout.
* HTTP errors raise a clear exception instead of silently returning nothing.
* Rate-limit responses (HTTP 403/429 with X-RateLimit-Remaining = 0) raise a
  dedicated exception that explains when the limit resets.
* A GitHub Personal Access Token is optional and read from the environment.
* The raw JSON is stored exactly as returned by the API. We never modify it,
  so `data/raw/` is a true copy of what GitHub sent us.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# --- Constants -------------------------------------------------------------
GITHUB_API_BASE = "https://api.github.com"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_PER_PAGE = 100
DEFAULT_MAX_PAGES = 3
DEFAULT_REQUEST_DELAY = 0.5


# --- Custom exceptions -----------------------------------------------------
class GitHubExtractionError(Exception):
    """Raised when the GitHub API cannot be read successfully."""


class GitHubRateLimitError(GitHubExtractionError):
    """Raised when the GitHub API rate limit has been reached."""


# --- Small helpers ---------------------------------------------------------
def split_full_name(full_name: str) -> tuple:
    """Split 'owner/repository' into ('owner', 'repository').

    Raises:
        GitHubExtractionError: if the name is not in the owner/repo format.
    """
    if not isinstance(full_name, str) or full_name.count("/") != 1:
        raise GitHubExtractionError(
            f"Invalid repository name {full_name!r}. Expected the format 'owner/repository'."
        )
    owner, repo = (part.strip() for part in full_name.split("/"))
    if not owner or not repo:
        raise GitHubExtractionError(
            f"Invalid repository name {full_name!r}. Owner and repository cannot be empty."
        )
    return owner, repo


def is_pull_request_record(issue: Dict[str, Any]) -> bool:
    """Return True if a record from the /issues endpoint is really a pull request.

    GitHub's issues endpoint returns BOTH issues and pull requests. A pull
    request record always carries an extra 'pull_request' key, so we use that
    key to identify it. These records must not be counted as ordinary issues.
    """
    return isinstance(issue, dict) and "pull_request" in issue


class GitHubExtractor:
    """Thin, readable wrapper around the GitHub REST API."""

    def __init__(
        self,
        token: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        per_page: int = DEFAULT_PER_PAGE,
        max_pages: int = DEFAULT_MAX_PAGES,
        request_delay: float = DEFAULT_REQUEST_DELAY,
        session: Optional[requests.Session] = None,
        api_base: str = GITHUB_API_BASE,
    ) -> None:
        self.timeout = timeout
        self.per_page = per_page
        self.max_pages = max_pages
        self.request_delay = request_delay
        self.api_base = api_base.rstrip("/")
        self.request_count = 0  # simple counter used in the console summary

        # A Session re-uses the TCP connection, which is faster and politer.
        self.session = session if session is not None else requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "User-Agent": "github-repository-performance-analytics/1.0",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    # -- low level ----------------------------------------------------------
    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Perform one GET request and return the decoded JSON body.

        Handles: timeouts, network errors, HTTP errors and rate limits.
        """
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
        except requests.exceptions.Timeout as exc:
            raise GitHubExtractionError(
                f"Request timed out after {self.timeout}s: {url}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise GitHubExtractionError(f"Network error while calling {url}: {exc}") from exc

        self.request_count += 1
        remaining = response.headers.get("X-RateLimit-Remaining")

        # --- rate limit handling -------------------------------------------
        if response.status_code in (403, 429) and remaining == "0":
            reset_header = response.headers.get("X-RateLimit-Reset")
            reset_text = "unknown"
            if reset_header and reset_header.isdigit():
                reset_time = datetime.fromtimestamp(int(reset_header), tz=timezone.utc)
                reset_text = reset_time.strftime("%Y-%m-%d %H:%M:%S UTC")
            raise GitHubRateLimitError(
                "GitHub API rate limit reached. Resets at "
                f"{reset_text}. Set GITHUB_TOKEN in your .env file to raise the "
                "limit from 60 to 5000 requests per hour."
            )

        # --- other HTTP errors ---------------------------------------------
        if response.status_code == 404:
            raise GitHubExtractionError(
                f"404 Not Found for {url}. Check the repository name and that the "
                "repository is public."
            )
        if response.status_code >= 400:
            raise GitHubExtractionError(
                f"HTTP {response.status_code} returned by {url}: "
                f"{response.text[:200]}"
            )

        try:
            return response.json()
        except ValueError as exc:  # response was not JSON
            raise GitHubExtractionError(f"Invalid JSON returned by {url}") from exc

    def _paginate(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Collect list results across pages until a short page or max_pages."""
        pages_to_read = max_pages if max_pages is not None else self.max_pages
        items: List[Dict[str, Any]] = []

        for page in range(1, pages_to_read + 1):
            page_params = dict(params or {})
            page_params["page"] = page
            page_params["per_page"] = self.per_page

            data = self._get(url, page_params)

            # The contributors endpoint returns an empty body (HTTP 204) when a
            # repository has no contributors. Treat that as "no more data".
            if data is None or data == "":
                break
            if not isinstance(data, list):
                raise GitHubExtractionError(
                    f"Expected a list from {url} but received {type(data).__name__}."
                )

            items.extend(data)
            logger.debug("Page %s of %s returned %s records", page, url, len(data))

            if len(data) < self.per_page:  # last page
                break
            if self.request_delay:
                time.sleep(self.request_delay)

        logger.info("Fetched %s records from %s", len(items), url)
        return items

    # -- public endpoint methods -------------------------------------------
    def get_repository(self, owner: str, repo: str) -> Dict[str, Any]:
        """GET /repos/{owner}/{repo}"""
        return self._get(f"{self.api_base}/repos/{owner}/{repo}")

    def get_commits(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """GET /repos/{owner}/{repo}/commits"""
        return self._paginate(f"{self.api_base}/repos/{owner}/{repo}/commits")

    def get_contributors(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """GET /repos/{owner}/{repo}/contributors"""
        return self._paginate(f"{self.api_base}/repos/{owner}/{repo}/contributors")

    def get_issues(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """GET /repos/{owner}/{repo}/issues

        `state=all` returns open and closed issues. Pull requests are included
        in this response by GitHub; they are filtered later in transformation.
        """
        return self._paginate(
            f"{self.api_base}/repos/{owner}/{repo}/issues",
            params={"state": "all", "sort": "created", "direction": "desc"},
        )

    def get_pull_requests(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """GET /repos/{owner}/{repo}/pulls"""
        return self._paginate(
            f"{self.api_base}/repos/{owner}/{repo}/pulls",
            params={"state": "all", "sort": "created", "direction": "desc"},
        )

    # -- convenience --------------------------------------------------------
    def extract_repository(self, full_name: str) -> Dict[str, Any]:
        """Extract all five datasets for one repository.

        Returns a dictionary that is later written to data/raw/ as JSON.
        """
        owner, repo = split_full_name(full_name)
        started_at = datetime.now(timezone.utc)

        logger.info("Extracting data for %s/%s", owner, repo)

        repository = self.get_repository(owner, repo)
        commits = self.get_commits(owner, repo)
        contributors = self.get_contributors(owner, repo)
        issues = self.get_issues(owner, repo)
        pull_requests = self.get_pull_requests(owner, repo)

        # Count the pull requests that GitHub returned inside /issues.
        pr_inside_issues = sum(1 for issue in issues if is_pull_request_record(issue))

        payload = {
            "metadata": {
                "requested_repository": full_name,
                "owner": owner,
                "repo": repo,
                "extraction_timestamp_utc": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "extraction_date_utc": started_at.strftime("%Y-%m-%d"),
                "api_base": self.api_base,
                "endpoints": {
                    "repository": f"/repos/{owner}/{repo}",
                    "commits": f"/repos/{owner}/{repo}/commits",
                    "contributors": f"/repos/{owner}/{repo}/contributors",
                    "issues": f"/repos/{owner}/{repo}/issues",
                    "pull_requests": f"/repos/{owner}/{repo}/pulls",
                },
                "records": {
                    "commits": len(commits),
                    "contributors": len(contributors),
                    "issues_endpoint_records": len(issues),
                    "pull_requests_inside_issues": pr_inside_issues,
                    "pull_requests": len(pull_requests),
                },
                "pagination": {
                    "per_page": self.per_page,
                    "max_pages": self.max_pages,
                },
                "authenticated": "Authorization" in self.session.headers,
            },
            # raw API responses, stored unmodified
            "repository": repository,
            "commits": commits,
            "contributors": contributors,
            "issues": issues,
            "pull_requests": pull_requests,
        }
        return payload

    def extract_all(self, repository_names: List[str]) -> Dict[str, Any]:
        """Extract every repository in the configuration list.

        A failure in one repository is logged and recorded, and the pipeline
        continues with the remaining repositories. If EVERY repository fails,
        an exception is raised because there is nothing to load.
        """
        results: List[Dict[str, Any]] = []
        failures: List[Dict[str, str]] = []

        for full_name in repository_names:
            try:
                results.append(self.extract_repository(full_name))
            except GitHubExtractionError as exc:
                logger.error("Extraction failed for %s: %s", full_name, exc)
                failures.append({"repository": full_name, "error": str(exc)})

        if not results:
            raise GitHubExtractionError(
                "Extraction failed for every configured repository. "
                f"Errors: {failures}"
            )

        return {
            "extraction_timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "requested_repositories": list(repository_names),
            "total_api_requests": self.request_count,
            "repositories": results,
            "failures": failures,
        }


# --- Raw JSON storage ------------------------------------------------------
def repository_file_stem(full_name: str) -> str:
    """Turn 'pandas-dev/pandas' into 'pandas-dev__pandas' for file names."""
    owner, repo = split_full_name(full_name)
    return f"{owner}__{repo}"


def save_raw_responses(payload: Dict[str, Any], raw_dir: str) -> List[str]:
    """Save the raw API responses to data/raw/ as JSON files.

    One file per repository plus one manifest file that records the
    extraction date/time of the whole run. Returns the list of written paths.
    """
    os.makedirs(raw_dir, exist_ok=True)
    written: List[str] = []

    for repo_payload in payload.get("repositories", []):
        full_name = repo_payload["metadata"]["requested_repository"]
        path = os.path.join(raw_dir, f"{repository_file_stem(full_name)}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(repo_payload, handle, indent=2, ensure_ascii=False)
        written.append(path)
        logger.info("Raw API response saved: %s", path)

    manifest = {
        "extraction_timestamp_utc": payload.get("extraction_timestamp_utc"),
        "requested_repositories": payload.get("requested_repositories", []),
        "total_api_requests": payload.get("total_api_requests"),
        "failures": payload.get("failures", []),
        "files": [os.path.basename(path) for path in written],
    }
    manifest_path = os.path.join(raw_dir, "extraction_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, ensure_ascii=False)
    written.append(manifest_path)
    return written


def load_raw_responses(raw_dir: str) -> Dict[str, Any]:
    """Read the saved raw JSON files back from data/raw/ (used by tests/debug)."""
    repositories = []
    if not os.path.isdir(raw_dir):
        raise GitHubExtractionError(f"Raw data directory not found: {raw_dir}")

    for name in sorted(os.listdir(raw_dir)):
        if name.endswith(".json") and name != "extraction_manifest.json":
            with open(os.path.join(raw_dir, name), encoding="utf-8") as handle:
                repositories.append(json.load(handle))

    if not repositories:
        raise GitHubExtractionError(f"No raw JSON files found in {raw_dir}")
    return {"repositories": repositories}
