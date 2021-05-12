CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS podscaler;

CREATE TABLE IF NOT EXISTS podscaler.queries
(
    id      SERIAL PRIMARY KEY,
    cluster TEXT NOT NULL,
    query   TEXT NOT NULL,
    UNIQUE (cluster, query)
);

CREATE TABLE IF NOT EXISTS podscaler.ranges
(
    id          INTEGER NOT NULL PRIMARY KEY,
    FOREIGN KEY (id) REFERENCES podscaler.queries (id),
    query_start TIMESTAMP NOT NULL,
    query_end   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS podscaler.conf
(
    id      SERIAL PRIMARY KEY,
    org     TEXT NOT NULL,
    repo    TEXT NOT NULL,
    branch  TEXT NOT NULL,
    variant TEXT,
    UNIQUE (org, repo, branch, variant)
);

CREATE TABLE IF NOT EXISTS podscaler.steps
(
    id        UUID NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    conf_id   INTEGER NOT NULL REFERENCES podscaler.conf (id),
    target    TEXT NOT NULL,
    step      TEXT NOT NULL,
    container TEXT NOT NULL,
    UNIQUE (conf_id, target, step, container)
);

CREATE TABLE IF NOT EXISTS podscaler.pods
(
    id        UUID NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    conf_id   INTEGER NOT NULL REFERENCES podscaler.conf (id),
    target    TEXT NOT NULL,
    container TEXT NOT NULL,
    UNIQUE (conf_id, target, container)
);

CREATE TABLE IF NOT EXISTS podscaler.builds
(
    id        UUID NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    conf_id   INTEGER NOT NULL REFERENCES podscaler.conf (id),
    build     TEXT NOT NULL,
    container TEXT NOT NULL,
    UNIQUE (conf_id, build, container)
);

CREATE TABLE IF NOT EXISTS podscaler.releases
(
    id        UUID NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    conf_id   INTEGER NOT NULL REFERENCES podscaler.conf (id),
    pod       TEXT NOT NULL,
    container TEXT NOT NULL,
    UNIQUE (conf_id, pod, container)
);

CREATE TABLE IF NOT EXISTS podscaler.rpms
(
    id      UUID NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    conf_id INTEGER NOT NULL REFERENCES podscaler.conf (id),
    UNIQUE (conf_id)
);

CREATE TABLE IF NOT EXISTS podscaler.prowjobs
(
    id        UUID NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    conf_id   INTEGER NOT NULL REFERENCES podscaler.conf (id),
    context   TEXT NOT NULL,
    container TEXT NOT NULL,
    UNIQUE (conf_id, context, container)
);

CREATE TABLE IF NOT EXISTS podscaler.raw_prowjobs
(
    id        UUID NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    job       TEXT NOT NULL,
    container TEXT NOT NULL,
    UNIQUE (job, container)
);

CREATE TABLE IF NOT EXISTS podscaler.histograms
(
    hist_id   SERIAL PRIMARY KEY,
    id        UUID NOT NULL,
    added     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    metric    TEXT NOT NULL,
    histogram BYTEA NOT NULL
);
