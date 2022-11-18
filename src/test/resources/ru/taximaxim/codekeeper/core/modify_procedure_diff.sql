SET search_path = pg_catalog;

CREATE OR REPLACE PROCEDURE public.insert_data(a integer, b integer)
    LANGUAGE sql
    AS $$
INSERT INTO tbl VALUES (a);
$$;

ALTER PROCEDURE public.insert_data(a integer, b integer) OWNER TO shamsutdinov_lr;