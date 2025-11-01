DROP TABLE default.stripe_log_table;

ALTER TABLE default.t1
	ADD COLUMN `b` UInt8;

ALTER TABLE default.t1
	ADD COLUMN `col13` Int32 TTL col11 + toIntervalDay(1);

CREATE TABLE default.stripe_log_table
(
	`timestamp` DateTime,
	`message_type` String,
	`message` String,
	`col12` String,
	`col1` String
)
ENGINE = StripeLog;