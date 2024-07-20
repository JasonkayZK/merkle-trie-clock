CREATE TABLE IF NOT EXISTS messages
(
    timestamp  TEXT,
    group_id   TEXT,
    dataset    TEXT,
    row        TEXT,
    column     TEXT,
    value_type TEXT,
    value      TEXT,
    PRIMARY KEY (timestamp, group_id)
);

CREATE TABLE IF NOT EXISTS messages_merkles
(
    group_id TEXT PRIMARY KEY,
    merkle   TEXT,
    merkle_base INT
);
