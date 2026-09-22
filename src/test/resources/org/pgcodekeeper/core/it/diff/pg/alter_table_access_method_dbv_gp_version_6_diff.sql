SET search_path = pg_catalog;

DROP TABLE public.t1;

CREATE TABLE public.t1 (
	c1 integer
)
USING gin;
