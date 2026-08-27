-- Append-only publication history for the Hacker News AI Digest example.
-- `schema` is a psql identifier variable supplied by repository tooling.
CREATE SCHEMA IF NOT EXISTS :"schema";

CREATE TABLE IF NOT EXISTS :"schema".hn_digest_summaries (
    digest_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_mode TEXT NOT NULL,
    source_identity TEXT NOT NULL,
    ai_provider TEXT NOT NULL,
    ai_model TEXT NOT NULL,
    token_estimator TEXT NOT NULL,
    interests TEXT NULL,
    story_ids TEXT[] NOT NULL,
    output_markdown TEXT NOT NULL
);
