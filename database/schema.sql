-- ===========================================================================
-- GitHub Repository Performance Analytics - PostgreSQL schema
-- ===========================================================================
-- This file is executed automatically by database/loader.py when the pipeline
-- runs (CREATE TABLE IF NOT EXISTS ...), so you do NOT have to create the
-- tables by hand in pgAdmin. You only need to create the empty database
-- "github_analytics" once (right click on Databases -> Create -> Database).
--
-- Table order matters because of the foreign keys:
--   repositories -> contributors -> repository_contributors
--   repositories -> commits / issues / pull_requests
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. repositories : one row per GitHub repository
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS repositories (
    repo_id            BIGINT PRIMARY KEY,
    full_name          VARCHAR(200) NOT NULL UNIQUE,
    owner_login        VARCHAR(100) NOT NULL,
    repo_name          VARCHAR(100) NOT NULL,
    description        TEXT,
    language           VARCHAR(60),
    stars              INTEGER DEFAULT 0,
    forks              INTEGER DEFAULT 0,
    watchers           INTEGER DEFAULT 0,
    open_issues_count  INTEGER DEFAULT 0,
    size_kb            INTEGER DEFAULT 0,
    default_branch     VARCHAR(100),
    license_name       VARCHAR(100),
    is_fork            BOOLEAN DEFAULT FALSE,
    created_at         TIMESTAMP,
    updated_at         TIMESTAMP,
    pushed_at          TIMESTAMP,
    html_url           TEXT,
    extracted_at       TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- 2. contributors : one row per GitHub account (unique username)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contributors (
    contributor_id     BIGINT PRIMARY KEY,
    username           VARCHAR(100) NOT NULL UNIQUE,
    avatar_url         TEXT,
    profile_url        TEXT,
    account_type       VARCHAR(30) DEFAULT 'User',
    extracted_at       TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- 3. repository_contributors : many-to-many link between repositories and
--    contributors, with the number of contributions in that repository
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS repository_contributors (
    repo_id            BIGINT NOT NULL REFERENCES repositories (repo_id) ON DELETE CASCADE,
    contributor_id     BIGINT NOT NULL REFERENCES contributors (contributor_id) ON DELETE CASCADE,
    contributions      INTEGER DEFAULT 0,
    PRIMARY KEY (repo_id, contributor_id)
);

-- ---------------------------------------------------------------------------
-- 4. commits : one row per commit sha
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commits (
    commit_sha         VARCHAR(64) PRIMARY KEY,
    repo_id            BIGINT NOT NULL REFERENCES repositories (repo_id) ON DELETE CASCADE,
    author_username    VARCHAR(100),
    author_name        VARCHAR(150),
    author_email       VARCHAR(150),
    authored_at        TIMESTAMP,
    commit_message     TEXT,
    comment_count      INTEGER DEFAULT 0,
    html_url           TEXT
);

-- ---------------------------------------------------------------------------
-- 5. issues : ordinary issues only. Pull requests returned by the /issues
--    endpoint are excluded during transformation (is_pull_request flag kept
--    for transparency and always FALSE for stored rows).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS issues (
    issue_id           BIGINT PRIMARY KEY,
    repo_id            BIGINT NOT NULL REFERENCES repositories (repo_id) ON DELETE CASCADE,
    issue_number       INTEGER NOT NULL,
    title              TEXT,
    state              VARCHAR(20) NOT NULL DEFAULT 'open',
    state_category     VARCHAR(20),
    is_closed          BOOLEAN DEFAULT FALSE,
    author_username    VARCHAR(100),
    labels             TEXT,
    comments           INTEGER DEFAULT 0,
    created_at         TIMESTAMP,
    closed_at          TIMESTAMP,
    is_pull_request    BOOLEAN DEFAULT FALSE,
    html_url           TEXT,
    CONSTRAINT uq_issues_repo_number UNIQUE (repo_id, issue_number)
);

-- ---------------------------------------------------------------------------
-- 6. pull_requests : open / closed / merged classification
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pull_requests (
    pr_id              BIGINT PRIMARY KEY,
    repo_id            BIGINT NOT NULL REFERENCES repositories (repo_id) ON DELETE CASCADE,
    pr_number          INTEGER NOT NULL,
    title              TEXT,
    state              VARCHAR(20) NOT NULL DEFAULT 'open',
    state_category     VARCHAR(20),
    merged             BOOLEAN DEFAULT FALSE,
    merged_at          TIMESTAMP,
    author_username    VARCHAR(100),
    created_at         TIMESTAMP,
    closed_at          TIMESTAMP,
    is_draft           BOOLEAN DEFAULT FALSE,
    html_url           TEXT,
    CONSTRAINT uq_pull_requests_repo_number UNIQUE (repo_id, pr_number)
);

-- ---------------------------------------------------------------------------
-- Indexes for the business queries (GROUP BY repo_id, state, merged ...)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_commits_repo_id ON commits (repo_id);
CREATE INDEX IF NOT EXISTS idx_commits_authored_at ON commits (authored_at);
CREATE INDEX IF NOT EXISTS idx_commits_author ON commits (author_username);
CREATE INDEX IF NOT EXISTS idx_issues_repo_id ON issues (repo_id);
CREATE INDEX IF NOT EXISTS idx_issues_state ON issues (state);
CREATE INDEX IF NOT EXISTS idx_pull_requests_repo_id ON pull_requests (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_requests_merged ON pull_requests (merged);
CREATE INDEX IF NOT EXISTS idx_repo_contributors_contributor ON repository_contributors (contributor_id);
CREATE INDEX IF NOT EXISTS idx_repositories_language ON repositories (language);
