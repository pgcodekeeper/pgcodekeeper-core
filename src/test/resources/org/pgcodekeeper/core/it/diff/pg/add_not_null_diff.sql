SET search_path = pg_catalog;

CREATE TABLE public.testtable2 (
	field1 integer NOT NULL,
	field2 integer CONSTRAINT test_not_null_table2 NOT NULL NO INHERIT
);

ALTER TABLE public.testtable
	ALTER COLUMN field2 SET NOT NULL;

UPDATE public.testtable
	SET field3 = DEFAULT WHERE field3 IS NULL;

ALTER TABLE public.testtable
	ALTER COLUMN field3 SET NOT NULL;

ALTER TABLE public.testtable
	ADD CONSTRAINT test_not_null NOT NULL field4 NO INHERIT;