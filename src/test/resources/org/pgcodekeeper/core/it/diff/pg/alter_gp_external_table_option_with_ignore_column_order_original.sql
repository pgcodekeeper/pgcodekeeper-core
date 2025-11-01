CREATE EXTERNAL TABLE public.ext_customer (
    id int,
    name text,
    sponsor text
)
LOCATION ( 'gpfdist://filehost:8081/*.txt' )
FORMAT 'TEXT' ( DELIMITER '|' NULL ' ')
LOG ERRORS SEGMENT REJECT LIMIT 5;


CREATE EXTERNAL TABLE public.ext_customer2 (
    id int,
    name text,
    sponsor text
)
LOCATION ( 'gpfdist://filehost:8081/*.txt' )
FORMAT 'TEXT' ( DELIMITER '|' NULL ' ')
LOG ERRORS SEGMENT REJECT LIMIT 5;
