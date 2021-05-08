CREATE SCHEMA IF NOT EXISTS podscaler;

CREATE TABLE IF NOT EXISTS podscaler.queries
(
    id      SERIAL PRIMARY KEY,
    cluster VARCHAR(256) NOT NULL,
    query   VARCHAR(2048) NOT NULL,
    UNIQUE (cluster, query)
);

CREATE TABLE IF NOT EXISTS podscaler.ranges
(
    id          INTEGER NOT NULL PRIMARY KEY,
    FOREIGN KEY (id) REFERENCES podscaler.queries (id),
    query_start TIMESTAMP NOT NULL,
    query_end   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS podscaler.meta
(
    id  SERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS podscaler.conf
(
    id      SERIAL PRIMARY KEY,
    org     VARCHAR(256) NOT NULL,
    repo    VARCHAR(256) NOT NULL,
    branch  VARCHAR(256) NOT NULL,
    variant VARCHAR(256),
    UNIQUE (org, repo, branch, variant)
);

CREATE TABLE IF NOT EXISTS podscaler.steps
(
    meta_id   INTEGER NOT NULL PRIMARY KEY,
    CONSTRAINT meta_id_constraint FOREIGN KEY (meta_id) REFERENCES podscaler.meta (id),
    conf_id   INTEGER NOT NULL,
    FOREIGN KEY (conf_id) REFERENCES podscaler.conf (id),
    target    VARCHAR(256) NOT NULL,
    step      VARCHAR(256) NOT NULL,
    container VARCHAR(256) NOT NULL,
    UNIQUE (meta_id, conf_id, target, step, container)
);

CREATE TABLE IF NOT EXISTS podscaler.builds
(
    meta_id   INTEGER NOT NULL PRIMARY KEY,
    CONSTRAINT meta_id_constraint FOREIGN KEY (meta_id) REFERENCES podscaler.meta (id),
    conf_id   INTEGER NOT NULL,
    FOREIGN KEY (conf_id) REFERENCES podscaler.conf (id),
    build     VARCHAR(256) NOT NULL,
    container VARCHAR(256) NOT NULL,
    UNIQUE (meta_id, conf_id, build, container)
);

CREATE TABLE IF NOT EXISTS podscaler.releases
(
    meta_id   INTEGER NOT NULL PRIMARY KEY,
    CONSTRAINT meta_id_constraint FOREIGN KEY (meta_id) REFERENCES podscaler.meta (id),
    conf_id   INTEGER NOT NULL,
    FOREIGN KEY (conf_id) REFERENCES podscaler.conf (id),
    pod       VARCHAR(256) NOT NULL,
    container VARCHAR(256) NOT NULL,
    UNIQUE (meta_id, conf_id, pod, container)
);

CREATE TABLE IF NOT EXISTS podscaler.rpms
(
    meta_id INTEGER NOT NULL PRIMARY KEY,
    CONSTRAINT meta_id_constraint FOREIGN KEY (meta_id) REFERENCES podscaler.meta (id),
    conf_id INTEGER NOT NULL,
    FOREIGN KEY (conf_id) REFERENCES podscaler.conf (id),
    UNIQUE (meta_id, conf_id)
);

CREATE TABLE IF NOT EXISTS podscaler.prowjobs
(
    meta_id   INTEGER NOT NULL PRIMARY KEY,
    CONSTRAINT meta_id_constraint FOREIGN KEY (meta_id) REFERENCES podscaler.meta (id),
    conf_id   INTEGER,
    FOREIGN KEY (conf_id) REFERENCES podscaler.conf (id),
    context   VARCHAR(256) NOT NULL,
    container VARCHAR(256) NOT NULL,
    UNIQUE (meta_id, conf_id, context, container)
);

CREATE TABLE IF NOT EXISTS podscaler.histograms
(
    id        SERIAL PRIMARY KEY,
    meta_id   INTEGER NOT NULL,
    FOREIGN KEY (meta_id) REFERENCES podscaler.meta (id),
    added     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    metric    VARCHAR(256) NOT NULL,
    histogram BYTEA NOT NULL
);
