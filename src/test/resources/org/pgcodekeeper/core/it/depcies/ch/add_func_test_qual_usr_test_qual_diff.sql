CREATE TABLE default.t2
(
	`col1` Int64,
	`col2` Int64
)
ENGINE = MergeTree
ORDER BY col1
SETTINGS index_granularity = 8192;

CREATE FUNCTION test_qual AS (x) -> default.t2;