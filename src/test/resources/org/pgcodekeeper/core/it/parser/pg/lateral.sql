CREATE SCHEMA test;

CREATE OR REPLACE FUNCTION test.get_id(_id integer)
 RETURNS TABLE(c1 integer)
    LANGUAGE sql STABLE SECURITY DEFINER
    AS $$
  select 1 AS c1;
$$;

CREATE TABLE test.t1 (
    id integer NOT NULL,
    c2 boolean DEFAULT false NOT NULL
);

CREATE VIEW test.v1 AS
  SELECT 
    t.id,
    t.c2,
    ct.c1
  FROM test.t1 t,
  LATERAL test.get_id(t.id) ct(c1);
