# Project Report
## GitHub Repository Performance Analytics

**A Python ETL pipeline for GitHub repository data**

| | |
|---|---|
| **Project title** | GitHub Repository Performance Analytics |
| **Domain** | Data Engineering / Data Analytics |
| **Student** | Abhinay V |
| **Course** | B.Tech – individual assignment |
| **Technologies** | Python 3.12, Requests, Pandas, NumPy, PostgreSQL, pgAdmin, SQLAlchemy, psycopg2, Matplotlib, python-dotenv, Pytest |
| **Report date** | 25 November 2026 |

---

## Abstract

Open-source projects publish a large amount of information through the GitHub REST API, but that
information arrives as nested JSON spread over many paginated responses and cannot answer business
questions directly. This project implements a complete ETL pipeline: it extracts repository,
commit, contributor, issue and pull-request data for three public repositories, cleans and
transforms it with Pandas, loads it into a local PostgreSQL database in six related tables, and
answers fourteen business questions using SQL, including a common table expression and window
functions. Three static charts summarise the results. The pipeline is repeatable (an
`INSERT … ON CONFLICT` strategy prevents duplicate rows), reports failure honestly when a stage
cannot complete, and is covered by 58 automated tests that run without internet access or a
database server.

---

## 1. Introduction

Data engineering is about moving data from a source where it is *stored* to a place where it can be
*analysed*. The three classical stages are **Extract** (collect), **Transform** (clean and
reshape) and **Load** (store in a database). This project applies that pattern to a real, public,
free data source: the GitHub REST API.

The project is deliberately kept at an academic, basic-to-intermediate level. There is no
framework, no orchestration tool and no cloud service. Every step is a readable Python function
that a student can explain line by line.

## 2. Problem statement

Given several public GitHub repositories, produce a database that can answer questions such as:

1. Which repository is the most popular?
2. Which repository is being developed most actively?
3. Who are the most important contributors?
4. Which projects have a growing maintenance backlog?
5. What percentage of proposed changes are actually merged?

The API alone cannot answer these: it returns individual records, not aggregates, and its data
contains duplicates, missing fields, mixed types, text timestamps, and pull requests hidden inside
the issues endpoint. A structured, cleaned, relational store is required.

## 3. Objectives

| # | Objective | Status |
|---|-----------|--------|
| 1 | Extract real data from the GitHub REST API with Requests | Completed |
| 2 | Handle pagination, timeouts, HTTP errors, rate limits, optional token | Completed |
| 3 | Save the untouched API responses as JSON with the extraction timestamp | Completed |
| 4 | Clean the data with Pandas/NumPy (duplicates, nulls, names, types, dates) | Completed |
| 5 | Classify issues (open/closed) and pull requests (open/closed/merged) | Completed |
| 6 | Calculate repository level metrics (commits, contributors, issues, merge %) | Completed |
| 7 | Keep rejected records with reasons and report data quality | Completed |
| 8 | Design and load a PostgreSQL schema with keys, constraints and indexes | Completed |
| 9 | Make loading idempotent (no duplicates on re-run) | Completed |
| 10 | Answer 10+ business questions with SQL (CTE + window function) | Completed (14 queries) |
| 11 | Produce three static Matplotlib charts | Completed |
| 12 | Test the pipeline with Pytest using mocks | Completed (58 tests) |

## 4. Literature and technology review

| Technology | Role in this project | Why it was chosen |
|------------|----------------------|-------------------|
| GitHub REST API v3 | Data source | Official, free, documented, JSON, no scraping |
| Requests | HTTP client | Simple, industry standard, easy timeout/header control |
| Pandas | Cleaning and transformation | Table operations (`drop_duplicates`, `fillna`, `to_datetime`) in one line |
| NumPy | Numeric helpers | `np.select` for classification, dtype handling |
| PostgreSQL | Target database | Free, ACID, rich SQL (CTE, window functions) |
| pgAdmin | Database GUI | Standard companion tool, lets the evaluator inspect tables |
| SQLAlchemy + psycopg2 | Connectivity | Safe parameter binding, dialect-aware `INSERT … ON CONFLICT` |
| Matplotlib | Charts | Simple static PNG output, no dashboard framework |
| python-dotenv | Configuration | Keeps credentials out of source code and out of Git |
| Pytest | Testing | Concise tests, fixtures, monkeypatching |

## 5. Methodology

The project follows the classical ETL architecture with a **raw layer** (JSON), a **processed
layer** (CSV) and a **serving layer** (PostgreSQL).

```
GitHub REST API
   │  Requests + pagination + retries-free error handling
   ▼
data/raw/*.json          (immutable evidence of what the API returned)
   │  Pandas: dedupe, type conversion, validation, classification, aggregation
   ▼
data/processed/*.csv     (six tables + repository_summary + quality report)
data/rejected/*.csv      (invalid records + rejection_reason)
   │  SQLAlchemy + psycopg2: CREATE TABLE IF NOT EXISTS + INSERT … ON CONFLICT
   ▼
PostgreSQL "github_analytics"  ──►  14 business SQL queries (pgAdmin)
   │
   ▼
reports/*.png (Matplotlib)
```

## 6. System design

### 6.1 Modules

| Module | Responsibility |
|--------|----------------|
| `extraction/github_extractor.py` | `GitHubExtractor` class, five endpoint methods, pagination, error and rate-limit handling, raw JSON storage |
| `transformation/cleaning.py` | `clean_*` functions per dataset, classification, metrics, `transform_all()` |
| `transformation/data_quality.py` | quality checks, `DataQualityReport`, `run_quality_checks()` |
| `database/connection.py` | reads `.env`, builds the SQLAlchemy URL, tests the connection, hides the password in logs |
| `database/schema.sql` | the single definition of the schema (6 tables, keys, unique constraints, indexes) |
| `database/loader.py` | executes `schema.sql`, reflects the tables, validates DataFrames, upserts data |
| `analysis/business_queries.sql` | 14 documented business queries |
| `analysis/visualizations.py` | three Matplotlib charts |
| `main.py` | runs the 16 stages, prints progress, returns exit code 0/1 |

### 6.2 Database schema

```
repositories (repo_id PK, full_name UNIQUE, stars, forks, language, created_at, ...)
     │
     ├──< commits (commit_sha PK, repo_id FK, authored_at, author_username, ...)
     ├──< issues (issue_id PK, repo_id FK, state, is_closed, UNIQUE(repo_id, issue_number))
     ├──< pull_requests (pr_id PK, repo_id FK, merged, merged_at, UNIQUE(repo_id, pr_number))
     └──< repository_contributors (repo_id FK, contributor_id FK, contributions,
                                   PRIMARY KEY (repo_id, contributor_id))
                                        │
contributors (contributor_id PK, username UNIQUE, account_type, ...) ─┘
```

Design decisions:

* **`commit_sha` as primary key** – a commit hash is globally unique, so it identifies a commit
  without a surrogate key.
* **Composite primary key on `repository_contributors`** – it *is* the natural key of the
  many-to-many relationship and prevents duplicate links.
* **`UNIQUE (repo_id, issue_number)`** – issue numbers are unique inside a repository but not
  across repositories.
* **`ON DELETE CASCADE`** – deleting a repository removes its dependent rows, keeping referential
  integrity.
* **Indexes** on the columns used by the `GROUP BY`/`JOIN` in the business queries.

## 7. Implementation

### 7.1 Extraction

`GitHubExtractor` keeps one `requests.Session`, sets the required headers
(`Accept: application/vnd.github+json`, a `User-Agent`, the API version) and adds
`Authorization: Bearer <token>` when `GITHUB_TOKEN` is present.

Pagination loop (simplified):

```python
for page in range(1, max_pages + 1):
    data = self._get(url, {**params, "page": page, "per_page": self.per_page})
    items.extend(data)
    if len(data) < self.per_page:
        break
```

Error handling is centralised in `_get()`:

```python
except requests.exceptions.Timeout:
    raise GitHubExtractionError(f"Request timed out after {self.timeout}s: {url}")
if response.status_code in (403, 429) and remaining == "0":
    raise GitHubRateLimitError(...)      # includes the reset time
if response.status_code == 404:
    raise GitHubExtractionError("404 Not Found ... check the repository name")
if response.status_code >= 400:
    raise GitHubExtractionError(f"HTTP {response.status_code} ...")
```

The important data trap: **`/issues` also returns pull requests**. They are recognised by the
presence of the `pull_request` key and excluded from the issue counts (87 of the 100 issue records
for `pandas-dev/pandas` in the verification run were actually pull requests).

### 7.2 Transformation

Thirteen documented operations are implemented (see `README.md` §10 for the full table). Two
examples:

```python
# 1. duplicate removal on the natural key
dataframe = dataframe.drop_duplicates(subset=["commit_sha"], keep="first")

# 2. type conversion: ISO text -> datetime64 (naive UTC), counts -> nullable Int64
dataframe["authored_at"] = to_datetime_column(dataframe["authored_at"])
dataframe["comment_count"] = to_int_column(dataframe["comment_count"])

# 3. classification of pull requests
valid["merged"] = valid["merged_at"].notna()
valid["state_category"] = np.select(
    [valid["merged"], valid["state"].eq("open")], ["Merged", "Open"], default="Closed"
)
```

Missing values are never deleted wholesale. Each column has an explicit rule (placeholder text,
`0`, or filled from a related column). Records are rejected **only** when they are unusable — no
id, no date, or a malformed repository name — and each rejection carries a reason.

### 7.3 Loading

```python
insert_statement = pg_insert(table)
update_columns = {c: insert_statement.excluded[c] for c in dataframe.columns if c not in keys}
statement = insert_statement.on_conflict_do_update(index_elements=keys, set_=update_columns)
for chunk in _chunks(records, 500):
    connection.execute(statement, chunk)
```

The tables are created from `database/schema.sql` — the same file that is documented and can be
pasted into pgAdmin — so there is exactly one definition of the schema. Table objects are
reflected from the live database, and `validate_dataframes_against_schema()` refuses to load when
a DataFrame column does not exist in the table.

### 7.4 Analysis

Fourteen SQL queries answer the required questions plus four additional ones. Concepts used:
`WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`/`LEFT JOIN`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`,
`COUNT(DISTINCT …)`, `CASE WHEN`, `ROUND`, `DATE_TRUNC`, casts, a CTE (`WITH`), a sub-query and
the window functions `SUM() OVER`, `RANK() OVER` and `ROW_NUMBER() OVER`.

## 8. Results

### 8.1 Extraction (real API, 25 November 2026)

| Repository | Commits | Contributors | Issue-endpoint records | …of which pull requests | Pull requests |
|------------|---------|--------------|------------------------|--------------------------|---------------|
| pandas-dev/pandas | 100 | 100 | 100 | 87 | 100 |
| numpy/numpy | 100 | 100 | 100 | 55 | 100 |
| psf/requests | 100 | 100 | 100 | 69 | 100 |

15 API requests were used (3 repositories × 5 endpoints × 1 page), well inside the
unauthenticated limit of 60 requests/hour.

### 8.2 Transformation

| Dataset | Extracted | Duplicates removed | Rejected | Excluded | Loaded |
|---------|-----------|--------------------|----------|----------|--------|
| repositories | 3 | 0 | 0 | 0 | 3 |
| commits | 300 | 0 | 0 | 0 | 300 |
| contributors | 300 | 0 | 0 | 0 | 300 |
| contributors (unique accounts) | 300 | 7 | 0 | 0 | 293 |
| issues | 300 | 0 | 0 | 211 | 89 |
| pull_requests | 300 | 0 | 0 | 0 | 300 |

The 7 "duplicates" among contributors are the same GitHub accounts appearing in more than one
repository; they are merged into single rows in `contributors` and kept as separate rows in
`repository_contributors`. The 211 excluded records are pull requests that the issues endpoint
returned.

### 8.3 Repository metrics

| Repository | Stars | Commits | Contributors | Issues (open/closed) | Pull requests (merged/total) | Merge % |
|------------|-------|---------|--------------|----------------------|------------------------------|---------|
| psf/requests | 54,757 | 100 | 100 | 26 / 19 | 29 / 100 | 29.0 |
| pandas-dev/pandas | 50,627 | 100 | 100 | 10 / 3 | 23 / 100 | 23.0 |
| numpy/numpy | 33,473 | 100 | 100 | 14 / 17 | 50 / 100 | 50.0 |

### 8.4 Loading

Loaded into PostgreSQL 16.2 in a verification environment:

```
repositories=3, contributors=293, repository_contributors=300,
commits=300, issues=89, pull_requests=300
```

Loading the same CSV files a second time left every row count unchanged, which proves the
`ON CONFLICT DO UPDATE` strategy works and no duplicates are created.

### 8.5 Business analysis – key findings

* `psf/requests` is the most starred project but has the largest open-issue backlog (26) and the
  lowest closure rate (42 %).
* `numpy/numpy` merges half of the sampled pull requests; `pandas-dev/pandas` merges only 23 %
  and keeps 69 pull requests open, i.e. it is the hardest project to get a change into.
* Contributor concentration is high: `charris` alone accounts for 7,301 of numpy's 35,440
  contributions.
* Only `psf/requests` exceeds the average number of open issues (16.67), so it is the single
  project needing more maintainer capacity.
* Four human contributors (`gfyoung`, `marcogorelli`, `bashtage`, `shoyer`) work across two of the
  three projects, while `dependabot[bot]` appears in all three.

## 9. Testing

`pytest -v` → **58 tests, all passing** (`58 passed in 3.87s`).

| File | Tests | Coverage of requirements |
|------|-------|--------------------------|
| `tests/test_extraction.py` | 18 | name validation, pull-request detection, response handling, pagination, `max_pages`, timeout, 404, rate limit, token header, five-dataset extraction, partial failure, raw JSON save/load |
| `tests/test_transformation.py` | 24 | column standardisation, username standardisation, datetime conversion, duplicate removal, missing-value handling, rejection rules, issue classification, pull-request classification, metrics, quality checks, `transform_all()` |
| `tests/test_database.py` | 16 | URL building and password encoding, settings from environment, password hiding, connection success and failure messages, schema parsing, schema ↔ loader consistency, table creation, pandas → Python type conversion, column validation |

No test needs the internet or a PostgreSQL server: HTTP responses are replaced by `FakeSession`
objects and the database engine by `FakeEngine` objects. The behaviour that *does* need a real
server — creating the tables in your own database and the upsert — is verified by running
`python main.py` against the local server, as described in `README.md` §17.

## 10. Limitations

* Only the first `max_pages × per_page` records per endpoint are collected; the metric
  "commit count" therefore means *commits collected in this run*.
* GitHub returns at most the top 500 contributors per repository.
* Commit authors that GitHub cannot link to an account are stored as `unknown`.
* Star history is not tracked over time — each run stores a snapshot.
* Without a token the rate limit is 60 requests/hour.

## 11. Future scope

* Incremental extraction using a `since` watermark stored in the database.
* Add releases, commit comments, languages and topics datasets.
* Schedule the pipeline (Task Scheduler or Airflow) and keep a star-history dimension table.
* Add SQL views or `dbt` models on top of the tables for the business metrics.
* Add a small Streamlit dashboard reading the same PostgreSQL tables.

## 12. Conclusion

The project demonstrates a complete, working ETL pipeline on real public data. It extracts data
from five GitHub REST API endpoints with pagination, timeouts and rate-limit handling, cleans and
transforms it into six well-designed relational tables, loads them into a local PostgreSQL
database without creating duplicates on repeated runs, and answers fourteen business questions
with SQL that uses joins, aggregates, a CTE and window functions. The code is modular, documented,
tested with 58 unit tests, and safe to publish on GitHub because all credentials live in a
git-ignored `.env` file.

## 13. Assessment mapping (3 + 7 + 5 marks)

| Marks | Component | What was delivered |
|-------|-----------|--------------------|
| **3** | **Data Extraction** | `extraction/github_extractor.py`: all five required endpoints, `requests.Session`, pagination with `page`/`per_page`, explicit timeouts, HTTP error handling, rate-limit detection with reset time, optional `GITHUB_TOKEN`, raw JSON stored in `data/raw/` with extraction timestamp and manifest, pull requests inside `/issues` detected, configurable repository list |
| **7** | **Transformation and Cleaning** | `transformation/cleaning.py` + `data_quality.py`: duplicate removal, documented missing-value rules, column-name standardisation, datetime and integer conversion, username standardisation, repository/ID validation, issue classification (Open/Closed), pull-request classification (Open/Closed/Merged), commit and contributor counts, open/closed issue counts, merge count and percentage, invalid-record detection with rejection reasons in `data/rejected/`, data-quality report with before/after counts in `data/processed/data_quality_report.csv` |
| **5** | **Loading and Business Analysis** | `database/schema.sql` (6 tables, PK/FK, unique constraints, indexes), `database/loader.py` (automatic table creation, duplicate-free upsert, schema validation, honest failure), `database/connection.py` (`.env` based credentials, connection test), `analysis/business_queries.sql` (14 queries with CTE, window functions, joins, aggregates), `analysis/visualizations.py` (3 charts), plus documentation and 58 passing tests |

## 14. References

1. GitHub REST API documentation – <https://docs.github.com/en/rest>
2. GitHub rate limits – <https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api>
3. Pandas documentation – <https://pandas.pydata.org/docs/>
4. SQLAlchemy 2.0 documentation – <https://docs.sqlalchemy.org/en/20/>
5. PostgreSQL 16 documentation – <https://www.postgresql.org/docs/16/>
6. Requests documentation – <https://requests.readthedocs.io/>
7. Matplotlib documentation – <https://matplotlib.org/stable/>
8. Pytest documentation – <https://docs.pytest.org/>
