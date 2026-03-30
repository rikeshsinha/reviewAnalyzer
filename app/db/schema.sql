PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    external_id TEXT,
    name TEXT NOT NULL,
    metadata_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, external_id)
);

CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    external_id TEXT,
    title TEXT,
    body TEXT,
    author TEXT,
    url TEXT,
    published_at TEXT,
    raw_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE IF NOT EXISTS document_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    tag_type TEXT NOT NULL CHECK (tag_type IN ('product', 'issue', 'competitor', 'feature')),
    tag_value TEXT NOT NULL,
    tag_source TEXT NOT NULL DEFAULT 'rules',
    confidence REAL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id),
    UNIQUE(document_id, tag_type, tag_value)
);

CREATE TABLE IF NOT EXISTS enrichments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    model_name TEXT,
    summary TEXT,
    sentiment_score REAL,
    metadata_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    status TEXT NOT NULL,
    records_fetched INTEGER NOT NULL DEFAULT 0,
    records_inserted INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);


CREATE TABLE IF NOT EXISTS enrichment_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    status TEXT NOT NULL,
    candidates INTEGER NOT NULL DEFAULT 0,
    enriched INTEGER NOT NULL DEFAULT 0,
    skipped_short INTEGER NOT NULL DEFAULT 0,
    failed_batches INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS saved_insights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    created_by TEXT,
    metadata_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    title,
    content,
    review_text
);

CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
    INSERT INTO documents_fts(rowid, title, content, review_text)
    VALUES (
        new.id,
        COALESCE(new.title, ''),
        COALESCE(new.body, ''),
        COALESCE(new.body, '')
    );
END;

CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, title, content, review_text)
    VALUES('delete', old.id, old.title, old.body, old.body);
END;

CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, title, content, review_text)
    VALUES('delete', old.id, old.title, old.body, old.body);
    INSERT INTO documents_fts(rowid, title, content, review_text)
    VALUES (
        new.id,
        COALESCE(new.title, ''),
        COALESCE(new.body, ''),
        COALESCE(new.body, '')
    );
END;

INSERT INTO documents_fts(documents_fts) VALUES('rebuild');
