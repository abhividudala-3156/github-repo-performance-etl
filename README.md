# GitHub Repository Performance Analytics

An end-to-end **ETL (Extract – Transform – Load)** project in Python that collects public
GitHub repository data from the official **GitHub REST API**, cleans and transforms it with
**Pandas**, loads it into a local **PostgreSQL** database (managed with **pgAdmin**) and
answers business questions with **SQL**.

> B.Tech university assignment – individual project.
> Basic-to-intermediate level: plain Python classes and functions, no frameworks,
> no cloud services, no Docker.

---

## Table of contents

1. [Project introduction](#1-project-introduction)
2. [Business problem](#2-business-problem)
3. [Project objectives](#3-project-objectives)
4. [Why GitHub data is useful](#4-why-github-data-is-useful)
5. [ETL architecture](#5-etl-architecture)
6. [Technologies used](#6-technologies-used)
7. [Project folder structure](#7-project-folder-structure)
8. [GitHub API endpoints](#8-github-api-endpoints)
9. [Extraction explained](#9-extraction-explained)
10. [Transformation and cleaning explained](#10-transformation-and-cleaning-explained)
11. [PostgreSQL database design](#11-postgresql-database-design)
12. [Business SQL queries](#12-business-sql-queries)
13. [Creating the database in pgAdmin](#13-creating-the-database-in-pgadmin)
14. [Configuring your PostgreSQL credentials](#14-configuring-your-postgresql-credentials)
15. [Installing the dependencies](#15-installing-the-dependencies)
16. [Running the project](#16-running-the-project)
17. [Testing instructions](#17-testing-instructions)
18. [Expected output files](#18-expected-output-files)
19. [Limitations and future improvements](#19-limitations-and-future-improvements)

---

## 1. Project introduction

Open-source repositories publish a lot of information: stars, forks, commits, contributors,
issues and pull requests. That information is available as JSON through the GitHub REST API,
but it is **not ready for analysis**: it is spread over many API responses, it contains
duplicates, missing values, mixed data types, timestamps in text form, and — an important
trap — GitHub's *issues* endpoint also returns *pull requests*.

This project builds a small but complete data pipeline that turns those raw API responses into
six clean relational tables and answers 14 business questions with SQL.

```
GitHub REST API  ->  Python + Requests  ->  JSON in data/raw/
      ->  Pandas cleaning  ->  CSV in data/processed/  (+ rejected records)
      ->  PostgreSQL (6 tables)  ->  SQL business queries in pgAdmin
      ->  Matplotlib charts in reports/
```

## 2. Business problem

A company (or a student research group) wants to know which open-source projects are healthy
and which are struggling, so it can decide where to invest engineering time, where to hire
contributors from, and which projects to depend on.

The raw API cannot answer that. Questions such as *"which repository has the largest
maintenance backlog?"*, *"what percentage of proposed changes are actually merged?"* or
*"who are the top contributors across all these projects?"* need a **structured database** and
**aggregate SQL queries**. Building that database from the API is exactly the ETL work this
project performs.

## 3. Project objectives

| # | Objective |
|---|-----------|
| 1 | Extract real data for several repositories from the official GitHub REST API using Python Requests |
| 2 | Handle pagination, timeouts, HTTP errors, rate limits and an optional access token |
| 3 | Store the untouched API responses as JSON, together with the extraction date and time |
| 4 | Clean and transform the data with Pandas/NumPy (duplicates, missing values, types, dates, validation) |
| 5 | Classify issues (open/closed) and pull requests (open/closed/merged) correctly |
| 6 | Calculate repository level metrics (commits, contributors, issues, merge percentage) |
| 7 | Keep rejected records with a reason and produce a data-quality report |
| 8 | Load the cleaned data into a local PostgreSQL database with a proper relational schema |
| 9 | Make the load repeatable (no duplicate rows when the pipeline runs again) |
| 10 | Answer 10+ business questions with SQL, including a CTE and a window function |
| 11 | Produce three static charts with Matplotlib |
| 12 | Test the pipeline with Pytest using mocked API responses and a mocked database |

## 4. Why GitHub data is useful

* **Objective and public** – no licence problems, no private data, no scraping of HTML.
* **Rich and relational** – repositories, commits, contributors, issues and pull requests form
  natural one-to-many and many-to-many relationships, which makes it a realistic exercise in
  database design.
* **Dirty in realistic ways** – missing descriptions, `null` authors, duplicate records across
  pages, pull requests hidden inside the issues endpoint, timestamps in ISO-8601 text. Perfect
  for demonstrating data cleaning.
* **Business relevant** – stars, commit activity, issue backlog and merge ratio are exactly the
  metrics engineering managers use to judge open-source projects.

## 5. ETL architecture

```
                        config/repositories.json
                                  |
                                  v
  +------------------+    +----------------------+    +---------------------+
  |  EXTRACT         |    |  TRANSFORM           |    |  LOAD               |
  |  Requests        |--->|  Pandas / NumPy      |--->|  SQLAlchemy         |
  |  GitHub REST API |    |  cleaning + quality  |    |  psycopg2-binary    |
  +------------------+    +----------------------+    +---------------------+
        |                          |                          |
        v                          v                          v
   data/raw/*.json          data/processed/*.csv        PostgreSQL
   (+ manifest)             data/rejected/*.csv         (6 tables)
                            data_quality_report.csv           |
                                                              v
                                                   analysis/business_queries.sql
                                                   (pgAdmin Query Tool)

                                        +------------------------------+
                                        |  REPORT: Matplotlib charts   |
                                        |  reports/*.png               |
                                        +------------------------------+
```

The 16 pipeline stages are described in [ETL_WORKFLOW.md](ETL_WORKFLOW.md).

## 6. Technologies used

| Technology | Purpose |
|------------|---------|
| Python 3.12+ | Main programming language |
| GitHub REST API | Data source |
| Requests | API communication |
| Pandas | Data cleaning and transformation |
| NumPy | Numerical operations |
| PostgreSQL | Relational database (installed locally) |
| pgAdmin | Database management (GUI) |
| SQLAlchemy | Database connectivity |
| psycopg2-binary | PostgreSQL driver used by SQLAlchemy |
| Matplotlib | Static charts |
| python-dotenv | Reads credentials from the local `.env` file |
| Pytest | Unit tests |

No cloud database, no Docker, no paid service, no web framework.

## 7. Project folder structure

```
github-repository-performance-analytics/
│
├── config/
│   └── repositories.json          # which repositories to extract + extraction settings
│
├── data/
│   ├── raw/                       # untouched API responses (JSON) + extraction_manifest.json
│   ├── processed/                 # cleaned CSV files + data_quality_report.csv
│   └── rejected/                  # rejected records with a rejection_reason column
│
├── extraction/
│   ├── __init__.py
│   └── github_extractor.py        # GitHubExtractor: pagination, timeouts, errors, rate limits
│
├── transformation/
│   ├── __init__.py
│   ├── cleaning.py                # all cleaning rules, classifications and metrics
│   └── data_quality.py            # quality checks + data-quality report
│
├── database/
│   ├── __init__.py
│   ├── connection.py              # DB settings (.env), engine, connection test
│   ├── schema.sql                 # complete PostgreSQL schema (tables, keys, indexes)
│   └── loader.py                  # create tables + upsert DataFrames (no duplicates)
│
├── analysis/
│   ├── __init__.py
│   ├── business_queries.sql       # 14 business questions in SQL
│   └── visualizations.py          # three Matplotlib charts
│
├── reports/                       # generated PNG charts
│
├── tests/
│   ├── test_extraction.py         # mocked GitHub API responses
│   ├── test_transformation.py     # cleaning, classification, metrics, quality checks
│   └── test_database.py           # connection URL, schema, loader helpers (no real DB needed)
│
├── logs/                          # etl.log (created when the pipeline runs)
│
├── .env.example                   # credential template (copy to .env)
├── .gitignore                     # keeps .env, logs and generated data out of Git
├── requirements.txt
├── pytest.ini
├── main.py                        # the ETL entry point
├── README.md
├── PROJECT_REPORT.md
├── ETL_WORKFLOW.md
└── BUSINESS_QUESTIONS.md
```

## 8. GitHub API endpoints

All endpoints are documented at <https://docs.github.com/en/rest>.

| # | Endpoint | What it gives us | Used for table |
|---|----------|------------------|----------------|
| 1 | `GET /repos/{owner}/{repo}` | repository details (stars, forks, language, dates…) | `repositories` |
| 2 | `GET /repos/{owner}/{repo}/commits` | commit list with author and date | `commits` |
| 3 | `GET /repos/{owner}/{repo}/contributors` | contributors and their contribution counts | `contributors`, `repository_contributors` |
| 4 | `GET /repos/{owner}/{repo}/issues?state=all` | issues **and pull requests** | `issues` |
| 5 | `GET /repos/{owner}/{repo}/pulls?state=all` | pull requests with `merged_at` | `pull_requests` |

Initial repositories (change them in `config/repositories.json`):

* `pandas-dev/pandas`
* `numpy/numpy`
* `psf/requests`

## 9. Extraction explained

Code: `extraction/github_extractor.py`

* **`GitHubExtractor` class** – one `requests.Session` for all calls (connection reuse).
* **Pagination** – `_paginate()` requests `page=1,2,3…` with `per_page` (default 100) and stops
  when a page returns fewer than `per_page` records or when `max_pages` is reached.
* **Timeouts** – every request passes `timeout=30` (configurable) and a timeout raises a clear
  `GitHubExtractionError`.
* **HTTP errors** – 404 gets an explicit "check the repository name" message, any other
  `>= 400` raises `GitHubExtractionError` with the status code and response body.
* **Rate limits** – a `403`/`429` with `X-RateLimit-Remaining: 0` raises `GitHubRateLimitError`
  that prints when the limit resets and reminds you to set `GITHUB_TOKEN`.
* **Optional token** – `GITHUB_TOKEN` from `.env` is sent as a `Bearer` header
  (60 requests/hour without it, 5000 with it).
* **Pull requests inside issues** – GitHub returns pull requests from the `/issues` endpoint.
  They always carry an extra `pull_request` key; `is_pull_request_record()` detects them, the
  extractor *counts* them in the metadata, and the transformation stage *excludes* them from the
  `issues` table.
* **Raw storage** – `save_raw_responses()` writes one JSON file per repository
  (`owner__repo.json`) plus `extraction_manifest.json`, which records the extraction
  timestamp, the request count and any failures. The API responses are stored **unmodified**.
* **One failing repository does not kill the run** – the error is logged, recorded in
  `failures` and the other repositories continue. If *every* repository fails, the pipeline stops
  with an error.

Console example (real run):

```
[2/16] Extracting repository details (/repos/{owner}/{repo})
      -> 3 record(s) collected
[5/16] Extracting issues (/repos/{owner}/{repo}/issues)
      -> 300 record(s) collected
      pandas-dev/pandas            commits=100   contributors=100   issues=100   pull_requests=100
```

## 10. Transformation and cleaning explained

Code: `transformation/cleaning.py` and `transformation/data_quality.py`

The 13 required cleaning operations and where they live:

| # | Requirement | Implementation |
|---|-------------|----------------|
| 1 | Remove duplicate records | `drop_duplicates()` on the natural key of each dataset (`repo_id`, `commit_sha`, `issue_id`, `pr_id`, `contributor_id`) |
| 2 | Handle missing and null values | explicit per-column rules (see table below) |
| 3 | Standardize column names | `standardize_column_names()` → `lower_case_with_underscores` |
| 4 | Convert timestamps | `to_datetime_column()` → ISO-8601 text → `datetime64` (naive UTC) |
| 5 | Convert numerical columns | `to_int_column()` → nullable `Int64` |
| 6 | Standardize contributor usernames | `standardize_username()` → trimmed, `@` removed, lower-cased |
| 7 | Validate repository names and IDs | regex `owner/repository` + positive numeric GitHub id |
| 8 | Classify issues | `state_category` = Open / Closed, `is_closed` boolean |
| 9 | Classify pull requests | `state_category` = Open / Closed / **Merged** (merged when `merged_at` is present) |
| 10 | Commit count per repository | `build_repository_summary()` |
| 11 | Contributor count per repository | `build_repository_summary()` |
| 12 | Open and closed issue counts | `build_repository_summary()` |
| 13 | Merge count and percentage | `build_repository_summary()` (`merge_percentage`) |

**Missing-value rules (nothing is deleted just because a field is empty):**

| Dataset | Column | Rule |
|---------|--------|------|
| repositories | `description` | `"No description provided"` |
| repositories | `language` | `"Unknown"` |
| repositories | `license_name` | `"No license"` |
| repositories | `stars`, `forks`, `watchers`, `open_issues_count`, `size_kb` | `0` |
| repositories | `created_at` | filled from `updated_at` when empty |
| commits | `author_username`, `author_name` | `"unknown"` |
| commits | `author_email` | `"not-provided@unknown"` |
| commits | `comment_count` | `0` |
| issues | `title` | `"Untitled"` |
| issues | `labels` | empty string |
| issues | `state` | `"open"` |
| pull_requests | `title` | `"Untitled"` |

**Invalid-record detection** – a record is *rejected* (not silently dropped) only when it cannot
be used at all:

| Dataset | Rejection reason |
|---------|------------------|
| repositories | `missing or invalid repository id`, `invalid repository name (expected owner/repository)`, `missing repository name` |
| commits | `missing or invalid commit sha`, `missing repository id`, `missing or invalid commit date` |
| contributors | `anonymous contributor without a valid account id`, `missing contributor username` |
| issues | `missing or invalid issue id`, `missing issue number`, `missing or invalid creation date` |
| pull_requests | `missing or invalid pull request id`, `missing pull request number`, `missing or invalid creation date` |

Rejected rows are written to `data/rejected/<dataset>_rejected.csv` with a
`rejection_reason` column, and the before/after counts of every dataset are written to
`data/processed/data_quality_report.csv`:

```
dataset                        records_extracted  duplicates_removed  records_rejected  records_excluded  records_loaded
repositories                   3                  0                   0                 0                 3
commits                        300                0                   0                 0                 300
issues                         300                0                   0                 211               89
pull_requests                  300                0                   0                 0                 300
contributors (unique accounts) 300                7                   0                 0                 293
```

`records_excluded` for issues = pull requests that GitHub returned from `/issues`
(they are counted in the `pull_requests` table instead).

## 11. PostgreSQL database design

Code: `database/schema.sql` (single source of truth), applied automatically by
`database/loader.py`.

```
repositories (1) ──< commits
     │ 1
     │
     ├──< issues
     │
     ├──< pull_requests
     │
     └──< repository_contributors >── (1) contributors
```

| Table | Primary key | Important columns | Notes |
|-------|-------------|-------------------|-------|
| `repositories` | `repo_id` | `full_name` (UNIQUE), `stars`, `forks`, `language`, `created_at` | one row per repository |
| `contributors` | `contributor_id` | `username` (UNIQUE), `account_type` | one row per GitHub account |
| `repository_contributors` | (`repo_id`, `contributor_id`) | `contributions` | many-to-many link with FK to both parents |
| `commits` | `commit_sha` | `repo_id` (FK), `authored_at`, `author_username` | one row per commit |
| `issues` | `issue_id` | `repo_id` (FK), `state`, `is_closed`, UNIQUE(`repo_id`,`issue_number`) | pull requests excluded |
| `pull_requests` | `pr_id` | `repo_id` (FK), `merged`, `merged_at`, UNIQUE(`repo_id`,`pr_number`) | merged flag drives the merge % |

Indexes are created on `commits(repo_id)`, `commits(authored_at)`, `issues(repo_id)`,
`issues(state)`, `pull_requests(repo_id)`, `pull_requests(merged)` and
`repository_contributors(contributor_id)`.

**Repeatable loading** – the loader inserts with
`INSERT ... ON CONFLICT (primary key) DO UPDATE`, so running `python main.py` twice updates the
existing rows instead of duplicating them (verified: two loads of the same data leave the row
counts unchanged).

**Honest failure** – if the connection fails you get a readable message and the pipeline exits
with code `1` and `RESULT: PIPELINE FAILED`; it never claims the data was loaded.

## 12. Business SQL queries

File: `analysis/business_queries.sql` – **14 queries**, each with a comment explaining the
business question. All 14 were executed against a real PostgreSQL 16 database.

| # | Business question | Main SQL concepts |
|---|-------------------|-------------------|
| 1 | Repositories with the most stars | `ORDER BY`, `LIMIT` |
| 2 | Highest commit activity | `LEFT JOIN`, `COUNT`, `COUNT(DISTINCT …)`, `MIN`/`MAX` |
| 3 | Top 10 contributors | `JOIN`, `SUM`, `GROUP BY`, `ORDER BY`, `LIMIT` |
| 4 | Contributors per repository | `LEFT JOIN`, `COUNT`, `AVG`, `MAX` |
| 5 | Monthly commit trend | `DATE_TRUNC`, `GROUP BY`, `SUM() OVER (…)` window function |
| 6 | Open vs closed issues per repository | `SUM(CASE WHEN …)`, `ROUND` |
| 7 | Repositories with the most open issues | `WHERE`, `GROUP BY`, `HAVING` |
| 8 | Pull-request merge percentage | `CASE`, `SUM`, division with `100.0` |
| 9 | Contributors working across several repositories | `HAVING COUNT(DISTINCT …) > 1` |
| 10 | Contributor ranking inside each repository | CTE + `RANK() OVER (PARTITION BY …)` |
| 11 | Average stars/forks per language | `GROUP BY`, `AVG`, `SUM` |
| 12 | Repositories above the average open-issue count | CTE + sub-query |
| 13 | Top contributor of each repository | CTE + `ROW_NUMBER() OVER (…)` |
| 14 | Monthly issue-resolution trend | `DATE_TRUNC`, `CASE`, `GROUP BY` |

How to run them: pgAdmin → right-click the `github_analytics` database → **Query Tool** →
paste a query → **F5**. See [BUSINESS_QUESTIONS.md](BUSINESS_QUESTIONS.md) for the business
meaning of each answer.

## 13. Creating the database in pgAdmin

You only create the **empty database**. The six tables are created by the Python program.

1. Open **pgAdmin 4** and enter your pgAdmin master password.
2. In the left panel expand **Servers → PostgreSQL 16 (localhost:5432)**.
   If asked, enter the `postgres` password you chose when installing PostgreSQL.
3. Right-click **Databases → Create → Database…**
4. In the **Database** field type exactly `github_analytics` and click **Save**.
5. (Optional) Right-click the new database → **Refresh**; it now appears in the tree.
6. Run the pipeline (see below). After it finishes, expand
   **github_analytics → Schemas → public → Tables** and right-click → **Refresh**.
   You should see: `commits`, `contributors`, `issues`, `pull_requests`,
   `repositories`, `repository_contributors`.
7. To check the data: right-click a table → **View/Edit Data → All Rows**.

## 14. Configuring your PostgreSQL credentials

1. In the project folder, copy the template (PowerShell):

   ```powershell
   Copy-Item .env.example .env
   ```

2. Open `.env` in Notepad/VS Code and fill in your own values:

   ```ini
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=github_analytics
   DB_USER=postgres
   DB_PASSWORD=your_real_postgres_password
   GITHUB_TOKEN=
   ```

3. Save the file. **Do not rename it and do not commit it** – `.env` is already in `.gitignore`.

4. Test the connection before running the whole pipeline:

   ```powershell
   python -m database.connection
   ```

   Expected output: `[OK] Connected to PostgreSQL: PostgreSQL 16.x ...`
   If it fails you get a list of things to check (service running, database exists, password).

`database/connection.py` is the only file that reads these values, and it hides the password in
all log output. No password is ever written into the Python code or the SQL file.

## 15. Installing the dependencies

Windows PowerShell, inside the project folder:

```powershell
python --version                  # must be 3.12 or newer
python -m venv venv               # optional but recommended
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If PowerShell blocks the activation script:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## 16. Running the project

Make sure PostgreSQL is running, the `github_analytics` database exists and `.env` is filled in.

```powershell
python main.py
```

The console prints 16 numbered stages and ends with either
`RESULT: PIPELINE COMPLETED SUCCESSFULLY` or `RESULT: PIPELINE FAILED`.

Useful options:

```powershell
python main.py --skip-db                 # extraction + transformation only (no PostgreSQL)
python main.py --max-pages 1             # faster run, fewer API pages per endpoint
python main.py --repos psf/requests      # only one repository
python main.py --log-level DEBUG         # verbose console output
python main.py --help                    # all options
```

To add or change repositories, edit `config/repositories.json`:

```json
{
  "repositories": ["pandas-dev/pandas", "numpy/numpy", "psf/requests"],
  "extraction": {
    "per_page": 100,
    "max_pages": 3,
    "timeout_seconds": 30,
    "request_delay_seconds": 0.5
  }
}
```

* `max_pages` limits how many pages (× `per_page` records) are collected per endpoint.
  With `per_page = 100` and `max_pages = 3` you get up to 300 records per endpoint per repository.
* Put a token in `.env` (`GITHUB_TOKEN=...`) to raise the rate limit from 60 to 5000 requests/hour.

Charts can also be regenerated on their own:

```powershell
python -m analysis.visualizations
```

## 17. Testing instructions

```powershell
pytest -v
```

The suite contains **58 tests** and needs **no internet connection and no PostgreSQL server**:
the GitHub API and the database engine are replaced with small fakes.

| File | What it tests |
|------|---------------|
| `tests/test_extraction.py` | repository-name validation, pull-request detection, JSON response handling, pagination, `max_pages`, timeouts, HTTP 404, rate-limit error, token header, all five datasets, partial failures, raw JSON save/load |
| `tests/test_transformation.py` | column-name standardisation, username standardisation, datetime conversion, duplicate removal, missing-value filling, invalid-record rejection, issue classification, pull-request classification, repository metrics, quality checks, full `transform_all()` |
| `tests/test_database.py` | connection URL building (including password encoding), settings from environment, password hiding, connection success/failure messages, schema.sql parsing, schema ↔ loader table/key consistency, `create_tables`, `dataframe_to_records` type conversion, schema/DataFrame column validation |

**Tests that need a real PostgreSQL database** (not part of the automatic suite):
actually creating the tables in *your* database and inserting rows. Those are covered by running
`python main.py` against your local server, and by `python -m database.connection`.

Verified results (this build):

```
58 passed in 3.87s
```

## 18. Expected output files

| Location | Files |
|----------|-------|
| `data/raw/` | `pandas-dev__pandas.json`, `numpy__numpy.json`, `psf__requests.json`, `extraction_manifest.json` |
| `data/processed/` | `repositories.csv`, `contributors.csv`, `repository_contributors.csv`, `commits.csv`, `issues.csv`, `pull_requests.csv`, `repository_summary.csv`, `data_quality_report.csv` |
| `data/rejected/` | `<dataset>_rejected.csv` (only when records were rejected) |
| `reports/` | `top_repositories_by_stars.png`, `commits_by_repository.png`, `open_vs_closed_issues.png` |
| `logs/` | `etl.log` |
| PostgreSQL | 6 tables with the same rows as the CSV files |

Example `repository_summary.csv` from a real run (`--max-pages 1`, 25 Nov 2026):

```
repo_id,full_name,language,stars,forks,commit_count,contributor_count,total_issues,open_issues,closed_issues,total_pull_requests,merged_pull_requests,merge_percentage
1362490,psf/requests,Python,54757,10886,100,100,45,26,19,100,29,29.0
858127,pandas-dev/pandas,Python,50627,20717,100,100,13,10,3,100,23,23.0
908607,numpy/numpy,Python,33473,13068,100,100,31,14,17,100,50,50.0
```

## 19. Limitations and future improvements

**Limitations**

* Only the first `max_pages × per_page` records per endpoint are collected (the pipeline is a
  teaching project, not a full history archive). Numbers such as "commit count" therefore mean
  *commits collected in this run*, and the README/CSV headers say so.
* `/contributors` returns at most the top 500 contributors of a repository (GitHub limit).
* Commit authors that GitHub cannot link to an account appear as `unknown`.
* The pipeline stores snapshots; it does not keep a history of how stars changed over time.
* GitHub's unauthenticated rate limit is only 60 requests/hour, so a token is strongly
  recommended for repeated runs.

**Future improvements**

* Collect the full history with a "since last run" watermark and incremental loads.
* Add `commit_comments`, `releases`, `languages` and `repository_topics` datasets.
* Add Airflow/Prefect scheduling and a slowly-changing-dimension for star history.
* Add a small Streamlit dashboard on top of the same PostgreSQL tables.
* Add `dbt` models or SQL views for the business queries.

---

## Quick start (all commands)

```powershell
# 1. install
pip install -r requirements.txt

# 2. create the database "github_analytics" in pgAdmin, then configure credentials
Copy-Item .env.example .env
notepad .env

# 3. check the connection
python -m database.connection

# 4. run the pipeline
python main.py

# 5. run the tests
pytest -v

# 6. run the business queries: pgAdmin -> Query Tool -> analysis/business_queries.sql
```
> On machines with an Application Control policy, run `python -m pytest -v`
> instead of `pytest -v`.
