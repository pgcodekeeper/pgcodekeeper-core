CREATE EXTERNAL TABLE public.ext_customer (
    name text,
    id int,
    sponsor text
)
LOCATION ( 'gpfdist://filehost:8081/*.txt' )
FORMAT 'TEXT' ( DELIMITER '|' NULL ' ')
LOG ERRORS SEGMENT REJECT LIMIT 10;

CREATE EXTERNAL TABLE public.ext_customer2 (
    name text,
    id int,
    sponsor text
)
LOCATION ( 'gpfdist://filehost:8081/*.txt' )
FORMAT 'TEXT' ( DELIMITER '|' NULL ' ')
LOG ERRORS SEGMENT REJECT LIMIT 5;
