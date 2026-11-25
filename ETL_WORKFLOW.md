# ETL Workflow – Stage by Stage

This document explains what happens inside the pipeline, in the order it happens, and points to
the function that implements each step. Run `python main.py` and the console prints the same
16 stage numbers.

```
 config/repositories.json
        │
        ▼
 ┌──────────────┐   data/raw/*.json        ┌─────────────────┐   data/processed/*.csv
 │ 1  CONFIG    │─────────────────────────▶│ 8  TRANSFORM    │──────────────────────┐
 │ 2-6 EXTRACT  │                          │ 9  QUALITY      │  data/rejected/*.csv │
 │ 7  RAW JSON  │                          │ 10-11 WRITE     │                      │
 └──────────────┘                          └─────────────────┘                      │
                                                                                     ▼
                                                        ┌────────────────────────────────┐
                                                        │ 12-14 LOAD into PostgreSQL     │
                                                        │ 15    CHARTS (Matplotlib)      │
                                                        │ 16    SUMMARY                  │
                                                        └────────────────────────────────┘
```

---

## Stage 1 – Read repository configuration

**Code:** `main.load_configuration()` → `config/repositories.json`

```json
{
  "repositories": ["pandas-dev/pandas", "numpy/numpy", "psf/requests"],
  "extraction": {"per_page": 100, "max_pages": 3, "timeout_seconds": 30, "request_delay_seconds": 0.5}
}
```

Validation performed here:

* the file must exist and be valid JSON,
* `repositories` must be a non-empty list,
* every name must have the `owner/repository` form (`split_full_name()`),
* the `extraction` block is filled with defaults when a key is missing.

Command-line overrides: `--repos`, `--max-pages`, `--config`.

---

## Stages 2–6 – Extract from the GitHub REST API

**Code:** `main.extract_payloads()` → `extraction/github_extractor.py`

| Stage | Method | Endpoint |
|-------|--------|----------|
| 2 | `get_repository()` | `/repos/{owner}/{repo}` |
| 3 | `get_commits()` | `/repos/{owner}/{repo}/commits` |
| 4 | `get_contributors()` | `/repos/{owner}/{repo}/contributors` |
| 5 | `get_issues()` | `/repos/{owner}/{repo}/issues?state=all` |
| 6 | `get_pull_requests()` | `/repos/{owner}/{repo}/pulls?state=all` |

For every repository and every endpoint:

1. `_paginate()` adds `page` and `per_page` parameters and calls `_get()`.
2. `_get()` performs **one** HTTP request with `timeout`, then inspects:
   * `requests.exceptions.Timeout` → `GitHubExtractionError("Request timed out …")`
   * `403/429` with `X-RateLimit-Remaining: 0` → `GitHubRateLimitError` (with reset time)
   * `404` → `GitHubExtractionError` ("check the repository name")
   * any other `>= 400` → `GitHubExtractionError` with status and body
   * a body that is not JSON → `GitHubExtractionError`
3. Pagination stops on a short page (`len(items) < per_page`) or at `max_pages`.
4. Between pages the extractor sleeps `request_delay_seconds` to stay polite.

Special handling:

* **Pull requests inside `/issues`** – GitHub returns both issues and pull requests from the
  issues endpoint. `is_pull_request_record()` checks for the extra `pull_request` key. The
  extractor counts these records in `metadata.records.pull_requests_inside_issues`; the
  transformation stage excludes them from the `issues` table.
* **Empty contributors** – the endpoint may answer `204 No Content`; `_paginate()` treats an
  empty body as "no more data".
* **Failures** – one broken repository is logged and added to `failures`; the run continues.
  If every repository fails, the pipeline stops with a clear error.

Result: a `payload` dictionary with one entry per repository
(`repository`, `commits`, `contributors`, `issues`, `pull_requests`, `metadata`).

---

## Stage 7 – Save the raw API responses

**Code:** `extraction.github_extractor.save_raw_responses()`

* `data/raw/pandas-dev__pandas.json` – the exact JSON the API returned (never modified),
  plus a `metadata` block with the extraction timestamp, endpoint list, record counts and
  pagination settings.
* `data/raw/extraction_manifest.json` – run-level summary: timestamp, requested repositories,
  number of API requests, list of files, failures.

Keeping the raw layer makes the pipeline **re-runnable offline** and lets an evaluator prove
that the CSV files really come from the API.

---

## Stage 8 – Clean and transform with Pandas

**Code:** `transformation.cleaning.transform_all()` → the `clean_*` functions

Per dataset, in this order:

1. **Build a DataFrame** from the JSON records with explicit, readable field mapping.
2. **Standardize column names** (`standardize_column_names`).
3. **Remove duplicates** on the natural key and count them.
4. **Convert types** – nullable `Int64` for counts, `datetime64` for timestamps
   (`to_int_column`, `to_datetime_column`).
5. **Detect invalid records** – set `_rejection_reason`, then split into valid and rejected
   frames (`_split_by_rejection_reason`).
6. **Fill missing values** with the documented rules.
7. **Classify** – issues Open/Closed; pull requests Open/Closed/Merged.
8. **Select the final column order** that matches the PostgreSQL schema.

Then the repository level aggregates are built:

```python
build_repository_summary(repositories, commits, contributors, issues, pull_requests)
# repo_id, full_name, language, stars, forks, commit_count, contributor_count,
# total_issues, open_issues, closed_issues, total_pull_requests,
# merged_pull_requests, merge_percentage
```

and the two contributor tables:

* `build_contributors_table()` – one row per GitHub account (de-duplicated across repositories)
* `build_repository_contributors()` – the many-to-many link with `contributions`

Foreign-key safety: rows whose `repo_id` did not survive repository cleaning are dropped, so the
loader can never violate a foreign key.

---

## Stage 9 – Data-quality checks

**Code:** `main.build_quality_report()`, `transformation.data_quality.run_quality_checks()`

* **Before/after counts** per dataset: extracted → duplicates removed → rejected → excluded →
  loaded.
* **Missing values** remaining per column (`missing_value_summary`).
* **Primary-key checks**: null values and duplicates in `repo_id`, `contributor_id`,
  `commit_sha`, `issue_id`, `pr_id`.
* **Required columns** exist.
* **Empty dataset** detection.

Warnings do not stop the pipeline; they are printed and written to
`data/processed/data_quality_report.csv`.

---

## Stages 10–11 – Write the processed and rejected CSV files

**Code:** `main.save_processed_csvs()`, `main.save_rejected_records()`

`data/processed/`: `repositories.csv`, `contributors.csv`, `repository_contributors.csv`,
`commits.csv`, `issues.csv`, `pull_requests.csv`, `repository_summary.csv`,
`data_quality_report.csv`.

`data/rejected/`: `<dataset>_rejected.csv` – only created when something was rejected. Every row
keeps its original values plus a `rejection_reason` column.

---

## Stages 12–14 – Load into PostgreSQL

**Code:** `database/connection.py`, `database/loader.py`

**Stage 12 – connect**

* `get_db_settings()` reads `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` from `.env`.
* `build_database_url()` → `postgresql+psycopg2://user:password@host:port/database`.
* `check_connection()` runs `SELECT version()`. On failure it raises
  `DatabaseConnectionError` explaining what to check (service running, database created,
  password correct) – and the pipeline stops with `RESULT: PIPELINE FAILED`.

**Stage 13 – create the tables**

* `create_tables()` reads `database/schema.sql`, strips `--` comment lines, splits on `;` and
  executes every statement. All statements are `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF
  NOT EXISTS`, so the stage is idempotent.

**Stage 14 – load the data**

* `reflect_tables()` reads the real table definitions back from PostgreSQL.
* `validate_dataframes_against_schema()` compares DataFrame columns with table columns and
  refuses to load on a mismatch (this catches typos in the transformation stage).
* `load_all()` inserts in foreign-key order:
  `repositories → contributors → repository_contributors → commits → issues → pull_requests`.
* `dataframe_to_records()` converts pandas values (`NaN`, `NaT`, `pd.NA`, `numpy.int64`,
  `Timestamp`) into plain Python values for psycopg2.
* Each table is written with `INSERT … ON CONFLICT (primary key) DO UPDATE` in chunks of 500
  rows, so a second run updates rows instead of duplicating them.
* `table_row_counts()` prints the row count of every table afterwards.

---

## Stage 15 – Charts

**Code:** `analysis/visualizations.py`

Matplotlib runs with the `Agg` backend (no window, works on Windows without a display):

1. `reports/top_repositories_by_stars.png`
2. `reports/commits_by_repository.png`
3. `reports/open_vs_closed_issues.png`

Charts are generated from the processed CSV files, so they also work with `--skip-db`.
A chart failure is a warning, not a pipeline failure (the data is already loaded).

---

## Stage 16 – Execution summary

Prints a status line per stage (`OK` / `WARNING` / `SKIPPED` / `FAILED`), the row counts of every
table, the PostgreSQL row counts, the elapsed time and the output locations, then either

```
RESULT: PIPELINE COMPLETED SUCCESSFULLY
```

or

```
RESULT: PIPELINE FAILED
```

and the process exit code is `0` or `1` accordingly, so the pipeline never claims success when
an essential stage failed.

---

## Where each requirement is implemented

| Requirement | File / function |
|-------------|-----------------|
| Requests, pagination, timeout, HTTP errors, rate limits, token | `extraction/github_extractor.py` |
| Raw JSON + extraction timestamp | `save_raw_responses()` |
| Duplicates, missing values, column names, dtypes, dates | `transformation/cleaning.py` |
| Username / repository-name validation | `standardize_username()`, `is_valid_repository_name()` |
| Issue and pull-request classification | `clean_issues()`, `clean_pull_requests()` |
| Commit/contributor/issue/merge metrics | `build_repository_summary()` |
| Rejected records with reasons | `_split_by_rejection_reason()` + `data/rejected/` |
| Data-quality summary and before/after counts | `transformation/data_quality.py`, `main.build_quality_report()` |
| PostgreSQL schema | `database/schema.sql` |
| Table creation + duplicate-free loading | `database/loader.py` |
| Business SQL (10+ queries, CTE, window function) | `analysis/business_queries.sql` |
| Charts | `analysis/visualizations.py` |
| Tests | `tests/test_extraction.py`, `tests/test_transformation.py`, `tests/test_database.py` |
