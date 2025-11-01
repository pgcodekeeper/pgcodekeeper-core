SET search_path = pg_catalog;

DROP EXTERNAL TABLE public.ext_customer;

CREATE EXTERNAL TABLE public.ext_customer (
	name text,
	id integer,
	sponsor text
)
LOCATION (
'gpfdist://filehost:8081/*.txt'
)
FORMAT 'TEXT' ( delimiter '|' null ' ' )
LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS;