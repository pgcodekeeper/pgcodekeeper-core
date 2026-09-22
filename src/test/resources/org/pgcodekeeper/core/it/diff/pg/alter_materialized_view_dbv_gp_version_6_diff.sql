SET search_path = pg_catalog;

DROP MATERIALIZED VIEW public.test_mv;

CREATE MATERIALIZED VIEW public.test_mv AS
	SELECT 1 AS id, 'test' AS name
WITH DATA;
