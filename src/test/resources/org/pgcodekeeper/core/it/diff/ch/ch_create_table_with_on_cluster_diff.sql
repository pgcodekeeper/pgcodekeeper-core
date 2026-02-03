CREATE TABLE default.test ON CLUSTER test
(
	`id` UInt64,
	`updated_at` DateTime DEFAULT now(),
	`updated_at_date` Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;