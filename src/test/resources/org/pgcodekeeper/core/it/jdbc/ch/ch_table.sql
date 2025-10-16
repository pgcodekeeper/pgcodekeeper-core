CREATE TABLE default.t4_1
(
    `col1` Int64 COMMENT 'test',
    `col2` Float64 DEFAULT 0.12,
    INDEX ind2 col3 TYPE minmax GRANULARITY 1,
    `col3` String
)
ENGINE = MergeTree
ORDER BY col1;

-- table with projection
CREATE TABLE default.t5
(
    `col1` UInt64 COMMENT 'test',
    Col2 String,
    col3 Int8,
    PROJECTION proj1 (SELECT col1 AS some_name, Col2, col3 ORDER BY col2),
    `col2` Float64 DEFAULT 0.012
)
ENGINE = MergeTree
ORDER BY col1
SAMPLE BY col1;

-- column with codec, null, and ttl options. table with pk
CREATE TABLE default.t6
(
    `col9` Int32,
    `col10` Int32,
    `col11` DateTime CODEC(DoubleDelta, T64),
    `col12` Nullable(Int32),
    `col13` Int32 TTL col11 + toIntervalDay(1),
    `col14` Int32 CODEC(LZ4HC(0)),
    `a` DateTime64(9, 'America/Chicago')
)
ENGINE = MergeTree
PARTITION BY a
PRIMARY KEY col9 AND col10
ORDER BY col9 AND col10
SETTINGS min_bytes_for_wide_part = 123,
index_granularity = 8192
COMMENT 'TEST';