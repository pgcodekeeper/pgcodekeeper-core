SET search_path = pg_catalog;

CREATE TABLE public.new_table (
	a1 integer,
	a2 integer,
	a3 integer
);

ALTER TABLE public.testtable
	ADD COLUMN a3 integer;