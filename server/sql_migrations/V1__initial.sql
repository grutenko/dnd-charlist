CREATE TABLE properties(
    key TEXT NOT NULL UNIQUE,
    value
);

/* Table for realize links beetween objects */
CREATE TABLE relationship (
    id INTEGER PRIMARY KEY,
    id0 INTEGER NOT NULL,
    id0_kind TEXT NOT NULL,
    id0_id INTEGER NOT NULL,
    id1 INTEGER NOT NULL,
    id1_kind TEXT NOT NULL,
    id1_id INTEGER NOT NULL,
    type INTEGER NOT NULL DEFAULT 0,
    meta BLOB
);
CREATE UNIQUE INDEX unique_objs_relation ON relationship( id0, id1, id0_kind, id0_id, id1_kind, id1_id );

CREATE TABLE obj_characters (
    id INTEGER PRIMARY KEY,
    name TEXT
);