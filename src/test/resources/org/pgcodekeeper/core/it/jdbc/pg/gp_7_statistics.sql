CREATE SCHEMA first;

CREATE SCHEMA second;

CREATE TABLE first.t1 (a integer, b integer) DISTRIBUTED BY (a);

CREATE MATERIALIZED VIEW first.v1 AS
	SELECT t1.a,
    t1.b
   FROM first.t1
WITH DATA
DISTRIBUTED RANDOMLY;

CREATE OR REPLACE FUNCTION first.f1(f1 integer) RETURNS integer
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
SELECT f1 + 1;
END;
$$;

CREATE STATISTICS second.s1 ON b, a FROM first.t1;

CREATE STATISTICS second.s2 ON b, a FROM first.v1;

ALTER SCHEMA public OWNER TO gpadmin;

REVOKE ALL ON SCHEMA public FROM PUBLIC;

REVOKE ALL ON SCHEMA public FROM gpadmin;

GRANT ALL ON SCHEMA public TO gpadmin;

GRANT ALL ON SCHEMA public TO PUBLIC;

ALTER SCHEMA first OWNER TO gpadmin;

ALTER SCHEMA second OWNER TO gpadmin;

ALTER TABLE first.t1 OWNER TO gpadmin;

ALTER FUNCTION first.f1(f1 integer) OWNER TO gpadmin;

ALTER MATERIALIZED VIEW first.v1 OWNER TO gpadmin;

ALTER STATISTICS second.s1 OWNER TO gpadmin;

ALTER STATISTICS second.s2 OWNER TO gpadmin;