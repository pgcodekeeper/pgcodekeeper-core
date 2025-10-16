CREATE SCHEMA first;

CREATE SCHEMA second;

CREATE TABLE first.t1 (a integer, b integer);

CREATE MATERIALIZED VIEW first.v1 AS SELECT a, b from first.t1;

CREATE OR REPLACE FUNCTION first.f1(f1 integer) RETURNS integer
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
  SELECT f1 + 1;
END;
$$;

--ORIGIN:
--
--CREATE STATISTICS second.s1 ON first.f1(b), a FROM first.t1;
--
--DIFF:

--CREATE STATISTICS second.s1 ON a, first.f1(b) FROM first.t1;

CREATE STATISTICS second.s2 ON b, first.f1(a) FROM first.v1;

ALTER SCHEMA public OWNER TO pg_database_owner;

GRANT USAGE ON SCHEMA public TO PUBLIC;

ALTER SCHEMA first OWNER TO test;

ALTER SCHEMA second OWNER TO test;

ALTER TABLE first.t1 OWNER TO test;

ALTER FUNCTION first.f1(f1 integer) OWNER TO test;

ALTER MATERIALIZED VIEW first.v1 OWNER TO test;

ALTER STATISTICS second.s2 OWNER TO test;

--ALTER STATISTICS second.s1 OWNER TO test;