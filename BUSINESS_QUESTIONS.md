# Business Questions and SQL Analysis

All queries live in **`analysis/business_queries.sql`** and run against the six tables created
by the pipeline:

```
repositories (repo_id, full_name, owner_login, stars, forks, watchers, language, ...)
contributors (contributor_id, username, account_type, ...)
repository_contributors (repo_id, contributor_id, contributions)
commits (commit_sha, repo_id, author_username, authored_at, ...)
issues (issue_id, repo_id, issue_number, state, is_closed, created_at, closed_at, ...)
pull_requests (pr_id, repo_id, pr_number, state, merged, merged_at, ...)
```

> The sample results below are real output from a verification run of this project
> (`--max-pages 1`, three repositories: `pandas-dev/pandas`, `numpy/numpy`, `psf/requests`,
> extracted on 25 November 2026). Your numbers will differ because the API returns live data.

---

## Q1. Which repositories have the highest number of stars?

```sql
SELECT full_name, owner_login, language, stars, forks, watchers
FROM repositories
ORDER BY stars DESC, full_name ASC
LIMIT 10;
```

**SQL concepts:** `SELECT`, `ORDER BY` (descending), `LIMIT`.

**Business significance:** stars are the simplest public measure of popularity. A product team
deciding which open-source library to build on, or a marketing team looking for a partnership
target, starts here.

| full_name | language | stars | forks |
|-----------|----------|-------|-------|
| psf/requests | Python | 54,757 | 10,886 |
| pandas-dev/pandas | Python | 50,627 | 20,717 |
| numpy/numpy | Python | 33,473 | 13,068 |

---

## Q2. Which repositories have the highest commit activity?

```sql
SELECT r.full_name,
       COUNT(c.commit_sha)               AS commit_count,
       COUNT(DISTINCT c.author_username) AS distinct_commit_authors,
       MIN(c.authored_at)::date          AS first_commit_date,
       MAX(c.authored_at)::date          AS last_commit_date
FROM repositories r
LEFT JOIN commits c ON c.repo_id = r.repo_id
GROUP BY r.full_name
ORDER BY commit_count DESC, r.full_name;
```

**SQL concepts:** `LEFT JOIN`, `GROUP BY`, `COUNT`, `COUNT(DISTINCT …)`, `MIN`, `MAX`, cast
`::date`.

**Business significance:** commit activity shows whether a project is alive. `LEFT JOIN` keeps
repositories with zero commits in the result, so a dead project is visible instead of silently
disappearing.

Sample: all three repositories returned 100 commits in this run (the extraction limit), but the
date range is very different – `psf/requests` commits come from 2023-10-18 to 2026-10-26, while
`pandas-dev/pandas` commits are all from the last two weeks. That difference is the real signal.

---

## Q3. Who are the top 10 contributors?

```sql
SELECT c.username,
       SUM(rc.contributions)      AS total_contributions,
       COUNT(DISTINCT rc.repo_id) AS repositories_contributed_to
FROM contributors c
JOIN repository_contributors rc ON rc.contributor_id = c.contributor_id
GROUP BY c.username
ORDER BY total_contributions DESC, c.username ASC
LIMIT 10;
```

**SQL concepts:** inner `JOIN`, `SUM`, `COUNT(DISTINCT …)`, `GROUP BY`, `ORDER BY`, `LIMIT`.

**Business significance:** identifies the people who carry the ecosystem. Useful for hiring,
for community recognition and for knowing whom to contact when a project must be fixed.

| username | total_contributions | repositories |
|----------|--------------------|--------------|
| charris | 7,301 | 1 |
| jbrockmendel | 6,670 | 1 |
| jreback | 4,731 | 1 |

---

## Q4. How many contributors are associated with each repository?

```sql
SELECT r.full_name,
       COUNT(rc.contributor_id)        AS contributor_count,
       SUM(rc.contributions)           AS total_contributions,
       ROUND(AVG(rc.contributions), 2) AS avg_contributions_per_contributor,
       MAX(rc.contributions)           AS top_contributor_contributions
FROM repositories r
LEFT JOIN repository_contributors rc ON rc.repo_id = r.repo_id
GROUP BY r.full_name
ORDER BY contributor_count DESC, r.full_name;
```

**SQL concepts:** `LEFT JOIN` on a many-to-many table, `COUNT`, `SUM`, `AVG`, `MAX`, `ROUND`.

**Business significance:** community size and concentration. A repository where one person has
7,301 contributions and the average is 354 has a **bus factor** risk: if that person leaves, the
project slows down.

---

## Q5. What is the monthly commit trend?

```sql
SELECT DATE_TRUNC('month', c.authored_at)::date                        AS commit_month,
       COUNT(*)                                                        AS commits,
       COUNT(DISTINCT c.repo_id)                                       AS active_repositories,
       SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC('month', c.authored_at)) AS running_total_commits
FROM commits c
WHERE c.authored_at IS NOT NULL
GROUP BY DATE_TRUNC('month', c.authored_at)
ORDER BY commit_month;
```

**SQL concepts:** `DATE_TRUNC`, `WHERE`, `GROUP BY`, and a **window function**
(`SUM(...) OVER (ORDER BY ...)`) that adds a running total without collapsing the rows.

**Business significance:** development momentum over time. The running total shows cumulative
growth, which is easier to read than isolated monthly numbers.

---

## Q6. What is the open versus closed issue count for each repository?

```sql
SELECT r.full_name,
       COUNT(i.issue_id)                                    AS total_issues,
       SUM(CASE WHEN i.state = 'open'   THEN 1 ELSE 0 END)   AS open_issues,
       SUM(CASE WHEN i.state = 'closed' THEN 1 ELSE 0 END)   AS closed_issues,
       ROUND(100.0 * SUM(CASE WHEN i.state = 'closed' THEN 1 ELSE 0 END)
             / COUNT(i.issue_id), 2)                         AS closed_issue_percentage
FROM repositories r
LEFT JOIN issues i ON i.repo_id = r.repo_id
GROUP BY r.full_name
ORDER BY open_issues DESC, r.full_name;
```

**SQL concepts:** conditional aggregation with `SUM(CASE WHEN …)`, `ROUND`, `LEFT JOIN`,
division of integers with `100.0` to avoid integer division.

**Business significance:** maintenance health. A low closed percentage means bugs and feature
requests pile up faster than maintainers can handle them.

| full_name | total | open | closed | closed % |
|-----------|-------|------|--------|----------|
| psf/requests | 45 | 26 | 19 | 42.22 |
| numpy/numpy | 31 | 14 | 17 | 54.84 |
| pandas-dev/pandas | 13 | 10 | 3 | 23.08 |

---

## Q7. Which repositories have the highest number of open issues?

```sql
SELECT r.full_name,
       COUNT(*)                  AS open_issues,
       ROUND(AVG(i.comments), 2) AS avg_comments_per_open_issue
FROM issues i
JOIN repositories r ON r.repo_id = i.repo_id
WHERE i.state = 'open'
GROUP BY r.full_name
HAVING COUNT(*) > 0
ORDER BY open_issues DESC, r.full_name
LIMIT 10;
```

**SQL concepts:** `WHERE` (row filter before grouping), `GROUP BY`, `HAVING` (filter after
grouping), `AVG`, `ORDER BY`, `LIMIT`.

**Business significance:** the backlog. `psf/requests` has 26 open issues in this snapshot — the
largest backlog of the three projects.

---

## Q8. What is the pull-request merge percentage for each repository?

```sql
SELECT r.full_name,
       COUNT(pr.pr_id)                                                     AS total_pull_requests,
       SUM(CASE WHEN pr.merged THEN 1 ELSE 0 END)                          AS merged_pull_requests,
       SUM(CASE WHEN pr.state = 'open' AND NOT pr.merged THEN 1 ELSE 0 END) AS open_pull_requests,
       ROUND(100.0 * SUM(CASE WHEN pr.merged THEN 1 ELSE 0 END)
             / COUNT(pr.pr_id), 2)                                          AS merge_percentage
FROM repositories r
JOIN pull_requests pr ON pr.repo_id = r.repo_id
GROUP BY r.full_name
ORDER BY merge_percentage DESC, r.full_name;
```

**SQL concepts:** boolean column used directly in `CASE`, `SUM`, `COUNT`, `ROUND`, `NOT`.

**Business significance:** how welcoming a project is. `numpy` merges 50 % of the pull requests
sampled here, `pandas` only 23 %, and `pandas` also has 69 pull requests still open — a
contributor considering a first contribution would find `numpy` friendlier.

---

## Q9. Which contributors work across multiple repositories?

```sql
SELECT c.username,
       COUNT(DISTINCT rc.repo_id) AS repositories_contributed_to,
       SUM(rc.contributions)      AS total_contributions
FROM contributors c
JOIN repository_contributors rc ON rc.contributor_id = c.contributor_id
GROUP BY c.username
HAVING COUNT(DISTINCT rc.repo_id) > 1
ORDER BY repositories_contributed_to DESC, total_contributions DESC;
```

**SQL concepts:** `HAVING` with `COUNT(DISTINCT …)`, `JOIN`, `GROUP BY`.

**Business significance:** cross-project people. `dependabot[bot]` appears in all three
repositories (720 contributions) — automation, not a human — while `gfyoung`, `marcogorelli`,
`bashtage` and `shoyer` are real people contributing to two of the three projects. These are the
best candidates for cross-team review or architecture discussions.

---

## Q10. How do contributors rank within each repository?

```sql
WITH ranked_contributors AS (
    SELECT rc.repo_id, c.username, rc.contributions,
           RANK() OVER (PARTITION BY rc.repo_id ORDER BY rc.contributions DESC) AS contribution_rank
    FROM repository_contributors rc
    JOIN contributors c ON c.contributor_id = rc.contributor_id
)
SELECT r.full_name, rc.username, rc.contributions, rc.contribution_rank
FROM ranked_contributors rc
JOIN repositories r ON r.repo_id = rc.repo_id
WHERE rc.contribution_rank <= 3
ORDER BY r.full_name, rc.contribution_rank;
```

**SQL concepts:** **CTE** (`WITH … AS`), **window function** `RANK() OVER (PARTITION BY …)`.

**Business significance:** the key maintainers of each project. `RANK()` (unlike `ROW_NUMBER()`)
gives equal contribution counts the same rank, which is the fair way to rank people.

---

## Q11. What is the average popularity of each programming language?

```sql
SELECT language,
       COUNT(*)             AS repository_count,
       ROUND(AVG(stars), 2) AS average_stars,
       ROUND(AVG(forks), 2) AS average_forks,
       SUM(stars)           AS total_stars
FROM repositories
GROUP BY language
HAVING COUNT(*) >= 1
ORDER BY average_stars DESC;
```

**SQL concepts:** `GROUP BY` on a categorical column, `AVG`, `SUM`, `COUNT`, `HAVING`.

**Business significance:** technology comparison. With only Python repositories the result is a
single row (average 46,285 stars), but adding JavaScript, Rust or Go repositories in
`config/repositories.json` turns this into a real language-popularity comparison.

---

## Q12. Which repositories have more open issues than the average repository?

```sql
WITH open_issue_counts AS (
    SELECT repo_id, COUNT(*) AS open_issues
    FROM issues
    WHERE state = 'open'
    GROUP BY repo_id
)
SELECT r.full_name,
       oic.open_issues,
       ROUND((SELECT AVG(open_issues) FROM open_issue_counts), 2) AS average_open_issues
FROM open_issue_counts oic
JOIN repositories r ON r.repo_id = oic.repo_id
WHERE oic.open_issues > (SELECT AVG(open_issues) FROM open_issue_counts)
ORDER BY oic.open_issues DESC;
```

**SQL concepts:** CTE, aggregate sub-query, comparison against an aggregate in `WHERE`.

**Business significance:** outlier detection without hard-coding a threshold. Only
`psf/requests` (26 open issues) exceeds the average of 16.67, so it is the one project that needs
extra maintainer time.

---

## Q13. Who is the single top contributor of each repository?

```sql
WITH numbered_contributors AS (
    SELECT rc.repo_id, c.username, rc.contributions,
           ROW_NUMBER() OVER (PARTITION BY rc.repo_id
                              ORDER BY rc.contributions DESC, c.username ASC) AS row_number_in_repo
    FROM repository_contributors rc
    JOIN contributors c ON c.contributor_id = rc.contributor_id
)
SELECT r.full_name, nc.username AS top_contributor, nc.contributions
FROM numbered_contributors nc
JOIN repositories r ON r.repo_id = nc.repo_id
WHERE nc.row_number_in_repo = 1
ORDER BY nc.contributions DESC;
```

**SQL concepts:** CTE + `ROW_NUMBER() OVER (PARTITION BY …)`, filtered on `row_number = 1`.

**Business significance:** a "project champion" list — exactly one contact person per project
(`charris` for numpy, `jbrockmendel` for pandas, `kennethreitz` for requests in this snapshot).
`ROW_NUMBER()` guarantees one row per repository even when two people have the same count.

---

## Q14. What does the monthly issue-resolution trend look like?

```sql
SELECT DATE_TRUNC('month', i.created_at)::date                 AS month,
       COUNT(*)                                                 AS issues_created,
       SUM(CASE WHEN i.closed_at IS NOT NULL THEN 1 ELSE 0 END)  AS issues_closed
FROM issues i
WHERE i.created_at IS NOT NULL
GROUP BY DATE_TRUNC('month', i.created_at)
ORDER BY month;
```

**SQL concepts:** `DATE_TRUNC`, `CASE` with `IS NOT NULL`, `GROUP BY`, `ORDER BY`.

**Business significance:** compares incoming work with completed work per month. When
`issues_created` is consistently larger than `issues_closed`, the backlog is growing — the same
conclusion as Q6 and Q7, but visible as a trend rather than a single number.

---

## Summary of SQL concepts used

| Concept | Where |
|---------|-------|
| `SELECT`, column aliases | all queries |
| `WHERE` | Q5, Q7, Q12, Q14 |
| `ORDER BY` (+ `DESC`, `ASC`, `LIMIT`) | all queries |
| `GROUP BY` | Q2–Q14 |
| `HAVING` | Q7, Q9, Q11 |
| `JOIN` / `LEFT JOIN` | Q2, Q3, Q4, Q6, Q7, Q8, Q9, Q10, Q12, Q13 |
| `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `ROUND` | Q2–Q14 |
| `COUNT(DISTINCT …)` | Q2, Q3, Q9 |
| `CASE WHEN` (conditional aggregation) | Q6, Q8, Q14 |
| Casts (`::date`) and `DATE_TRUNC` | Q2, Q5, Q14 |
| CTE (`WITH`) | Q10, Q12, Q13 |
| Window functions | `SUM() OVER` (Q5), `RANK() OVER` (Q10), `ROW_NUMBER() OVER` (Q13) |
| Sub-query | Q12 |
