-- ===========================================================================
-- GitHub Repository Performance Analytics - Business SQL Queries
-- ===========================================================================
-- Database : github_analytics (PostgreSQL)
-- Tables   : repositories, contributors, repository_contributors, commits,
--            issues, pull_requests
--
-- How to run in pgAdmin:
--   Right click the database "github_analytics" -> Query Tool -> paste a query
--   -> press F5 (Execute).
--
-- Each query starts with the business question it answers.
-- SQL concepts used: SELECT, WHERE, ORDER BY, GROUP BY, HAVING, JOIN,
-- COUNT / SUM / AVG / ROUND / CASE, CTEs (WITH) and window functions
-- (RANK, ROW_NUMBER, SUM OVER).
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- Q1. Which repositories have the highest number of stars?
--     Business use: shows which of the tracked projects attract the most
--     attention from the developer community.
-- ---------------------------------------------------------------------------
SELECT
    full_name,
    owner_login,
    language,
    stars,
    forks,
    watchers
FROM repositories
ORDER BY stars DESC, full_name ASC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- Q2. Which repositories have the highest commit activity?
--     Business use: measures how actively each project is being developed.
--     (COUNT of commits stored for the repository; LEFT JOIN keeps projects
--     with zero commits in the result.)
-- ---------------------------------------------------------------------------
SELECT
    r.full_name,
    COUNT(c.commit_sha)                  AS commit_count,
    COUNT(DISTINCT c.author_username)    AS distinct_commit_authors,
    MIN(c.authored_at)::date             AS first_commit_date,
    MAX(c.authored_at)::date             AS last_commit_date
FROM repositories r
LEFT JOIN commits c ON c.repo_id = r.repo_id
GROUP BY r.full_name
ORDER BY commit_count DESC, r.full_name;


-- ---------------------------------------------------------------------------
-- Q3. Who are the top 10 contributors?
--     Business use: identifies the most valuable individual contributors
--     across all tracked repositories.
-- ---------------------------------------------------------------------------
SELECT
    c.username,
    SUM(rc.contributions)          AS total_contributions,
    COUNT(DISTINCT rc.repo_id)     AS repositories_contributed_to
FROM contributors c
JOIN repository_contributors rc ON rc.contributor_id = c.contributor_id
GROUP BY c.username
ORDER BY total_contributions DESC, c.username ASC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- Q4. How many contributors are associated with each repository?
--     Business use: community size per project, useful to compare small and
--     large open-source communities.
-- ---------------------------------------------------------------------------
SELECT
    r.full_name,
    COUNT(rc.contributor_id)             AS contributor_count,
    SUM(rc.contributions)                AS total_contributions,
    ROUND(AVG(rc.contributions), 2)      AS avg_contributions_per_contributor,
    MAX(rc.contributions)                AS top_contributor_contributions
FROM repositories r
LEFT JOIN repository_contributors rc ON rc.repo_id = r.repo_id
GROUP BY r.full_name
ORDER BY contributor_count DESC, r.full_name;


-- ---------------------------------------------------------------------------
-- Q5. What is the monthly commit trend?
--     Business use: shows development momentum over time. The window function
--     adds a running total of commits so growth is easy to see.
-- ---------------------------------------------------------------------------
SELECT
    DATE_TRUNC('month', c.authored_at)::date                       AS commit_month,
    COUNT(*)                                                       AS commits,
    COUNT(DISTINCT c.repo_id)                                      AS active_repositories,
    SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC('month', c.authored_at)) AS running_total_commits
FROM commits c
WHERE c.authored_at IS NOT NULL
GROUP BY DATE_TRUNC('month', c.authored_at)
ORDER BY commit_month;


-- ---------------------------------------------------------------------------
-- Q6. What is the open versus closed issue count for each repository?
--     Business use: shows how well each project keeps up with reported
--     problems. A high open count can mean a slow maintenance process.
-- ---------------------------------------------------------------------------
SELECT
    r.full_name,
    COUNT(i.issue_id)                                          AS total_issues,
    SUM(CASE WHEN i.state = 'open'   THEN 1 ELSE 0 END)         AS open_issues,
    SUM(CASE WHEN i.state = 'closed' THEN 1 ELSE 0 END)         AS closed_issues,
    ROUND(
        100.0 * SUM(CASE WHEN i.state = 'closed' THEN 1 ELSE 0 END) / COUNT(i.issue_id),
        2
    )                                                           AS closed_issue_percentage
FROM repositories r
LEFT JOIN issues i ON i.repo_id = r.repo_id
GROUP BY r.full_name
ORDER BY open_issues DESC, r.full_name;


-- ---------------------------------------------------------------------------
-- Q7. Which repositories have the highest number of open issues?
--     Business use: the maintenance backlog of each project (HAVING removes
--     repositories with no open issues at all).
-- ---------------------------------------------------------------------------
SELECT
    r.full_name,
    COUNT(*)                    AS open_issues,
    ROUND(AVG(i.comments), 2)   AS avg_comments_per_open_issue
FROM issues i
JOIN repositories r ON r.repo_id = i.repo_id
WHERE i.state = 'open'
GROUP BY r.full_name
HAVING COUNT(*) > 0
ORDER BY open_issues DESC, r.full_name
LIMIT 10;


-- ---------------------------------------------------------------------------
-- Q8. What is the pull-request merge percentage for each repository?
--     Business use: a merge ratio tells us how many proposed changes are
--     actually accepted by the maintainers.
-- ---------------------------------------------------------------------------
SELECT
    r.full_name,
    COUNT(pr.pr_id)                                              AS total_pull_requests,
    SUM(CASE WHEN pr.merged THEN 1 ELSE 0 END)                   AS merged_pull_requests,
    SUM(CASE WHEN pr.state = 'open' AND NOT pr.merged THEN 1 ELSE 0 END) AS open_pull_requests,
    ROUND(
        100.0 * SUM(CASE WHEN pr.merged THEN 1 ELSE 0 END) / COUNT(pr.pr_id),
        2
    )                                                            AS merge_percentage
FROM repositories r
JOIN pull_requests pr ON pr.repo_id = r.repo_id
GROUP BY r.full_name
ORDER BY merge_percentage DESC, r.full_name;


-- ---------------------------------------------------------------------------
-- Q9. Which contributors work across multiple repositories?
--     Business use: finds cross-project contributors ("core" people of the
--     ecosystem) instead of people active in only one repository.
-- ---------------------------------------------------------------------------
SELECT
    c.username,
    COUNT(DISTINCT rc.repo_id)   AS repositories_contributed_to,
    SUM(rc.contributions)        AS total_contributions
FROM contributors c
JOIN repository_contributors rc ON rc.contributor_id = c.contributor_id
GROUP BY c.username
HAVING COUNT(DISTINCT rc.repo_id) > 1
ORDER BY repositories_contributed_to DESC, total_contributions DESC;


-- ---------------------------------------------------------------------------
-- Q10. How do contributors rank within each repository?
--      Business use: ranking inside a project shows who the key maintainers
--      are. RANK() gives the same rank to equal contribution counts.
-- ---------------------------------------------------------------------------
WITH ranked_contributors AS (
    SELECT
        rc.repo_id,
        c.username,
        rc.contributions,
        RANK() OVER (PARTITION BY rc.repo_id ORDER BY rc.contributions DESC) AS contribution_rank
    FROM repository_contributors rc
    JOIN contributors c ON c.contributor_id = rc.contributor_id
)
SELECT
    r.full_name,
    rc.username,
    rc.contributions,
    rc.contribution_rank
FROM ranked_contributors rc
JOIN repositories r ON r.repo_id = rc.repo_id
WHERE rc.contribution_rank <= 3
ORDER BY r.full_name, rc.contribution_rank;


-- ---------------------------------------------------------------------------
-- Q11. What is the average popularity (stars / forks) of each programming
--      language in the tracked repositories?
--      Business use: shows which technology stacks attract the most interest.
-- ---------------------------------------------------------------------------
SELECT
    language,
    COUNT(*)                AS repository_count,
    ROUND(AVG(stars), 2)    AS average_stars,
    ROUND(AVG(forks), 2)    AS average_forks,
    SUM(stars)              AS total_stars
FROM repositories
GROUP BY language
HAVING COUNT(*) >= 1
ORDER BY average_stars DESC;


-- ---------------------------------------------------------------------------
-- Q12. Which repositories have more open issues than the average repository?
--      Business use: highlights projects with an unusually large backlog
--      (uses a CTE plus a sub-query for the average).
-- ---------------------------------------------------------------------------
WITH open_issue_counts AS (
    SELECT
        repo_id,
        COUNT(*) AS open_issues
    FROM issues
    WHERE state = 'open'
    GROUP BY repo_id
)
SELECT
    r.full_name,
    oic.open_issues,
    ROUND((SELECT AVG(open_issues) FROM open_issue_counts), 2) AS average_open_issues
FROM open_issue_counts oic
JOIN repositories r ON r.repo_id = oic.repo_id
WHERE oic.open_issues > (SELECT AVG(open_issues) FROM open_issue_counts)
ORDER BY oic.open_issues DESC;


-- ---------------------------------------------------------------------------
-- Q13. Who is the single top contributor of each repository?
--      Business use: a "project champion" list for recognition and for
--      contacting the right person about a repository.
--      ROW_NUMBER() returns exactly one row per repository.
-- ---------------------------------------------------------------------------
WITH numbered_contributors AS (
    SELECT
        rc.repo_id,
        c.username,
        rc.contributions,
        ROW_NUMBER() OVER (
            PARTITION BY rc.repo_id
            ORDER BY rc.contributions DESC, c.username ASC
        ) AS row_number_in_repo
    FROM repository_contributors rc
    JOIN contributors c ON c.contributor_id = rc.contributor_id
)
SELECT
    r.full_name,
    nc.username              AS top_contributor,
    nc.contributions         AS top_contributor_contributions
FROM numbered_contributors nc
JOIN repositories r ON r.repo_id = nc.repo_id
WHERE nc.row_number_in_repo = 1
ORDER BY nc.contributions DESC;


-- ---------------------------------------------------------------------------
-- Q14. What does the monthly issue-resolution trend look like?
--      Business use: compares newly created issues with closed issues per
--      month, which shows whether the backlog is shrinking or growing.
-- ---------------------------------------------------------------------------
SELECT
    DATE_TRUNC('month', i.created_at)::date                    AS month,
    COUNT(*)                                                    AS issues_created,
    SUM(CASE WHEN i.closed_at IS NOT NULL THEN 1 ELSE 0 END)     AS issues_closed
FROM issues i
WHERE i.created_at IS NOT NULL
GROUP BY DATE_TRUNC('month', i.created_at)
ORDER BY month;
