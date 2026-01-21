SET search_path = pg_catalog;

CREATE TYPE public.user_code AS (
    f1 integer,
    f2 text
    );

ALTER TYPE public.user_code OWNER TO gpadmin;

CREATE SEQUENCE public.emp_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE CACHE 1;

ALTER SEQUENCE public.emp_id_seq OWNER TO gpadmin;

CREATE OR REPLACE FUNCTION public.emp_stamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
        -- Check that empname and salary are given
        IF
NEW.empname IS NULL THEN
            RAISE EXCEPTION 'empname cannot be null';
END IF;
        IF
NEW.salary IS NULL THEN
            RAISE EXCEPTION '% cannot have null salary', NEW.empname;
END IF;

        -- Who works for us when they must pay for it?
        IF
NEW.salary < 0 THEN
            RAISE EXCEPTION '% cannot have a negative salary', NEW.empname;
END IF;

        -- Remember who changed the payroll when
        NEW.last_date
:= current_timestamp;
        NEW.last_user
:= current_user;
RETURN NEW;
END;
$$;

ALTER FUNCTION public.emp_stamp() OWNER TO gpadmin;

CREATE OR REPLACE FUNCTION public.increment(i integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN i + 1;
END;
$$;

ALTER FUNCTION public.increment(i integer) OWNER TO gpadmin;

CREATE TABLE public.emp
(
    id        integer DEFAULT nextval('public.emp_id_seq'::regclass) NOT NULL,
    empname   text,
    salary    integer,
    last_date timestamp without time zone,
    last_user text,
    code public.user_code
) DISTRIBUTED BY (empname);

ALTER TABLE public.emp OWNER TO gpadmin;

CREATE UNIQUE INDEX name_ind ON public.emp USING btree (empname);

CREATE VIEW public.emp_view AS
SELECT emp.empname,
       emp.last_date,
       public.increment(emp.salary) AS salary,
       emp.code
FROM public.emp;

ALTER VIEW public.emp_view OWNER TO gpadmin;

CREATE TRIGGER emp_stamp
    BEFORE INSERT OR
UPDATE ON public.emp
    FOR EACH ROW
    EXECUTE PROCEDURE public.emp_stamp();

COMMENT ON TRIGGER emp_stamp ON public.emp IS 'trigger comment';

CREATE RULE notify_me AS
    ON
UPDATE TO public.emp DO NOTIFY emp;

ALTER SEQUENCE public.emp_id_seq
    OWNED BY public.emp.id;

GRANT
ALL
ON SCHEMA public TO PUBLIC;

-- test quoted server name

CREATE FOREIGN DATA WRAPPER wrap
    OPTIONS (debug 'true');

ALTER FOREIGN DATA WRAPPER wrap OWNER TO gpadmin;

CREATE SERVER "asdashSA/sdag" FOREIGN DATA WRAPPER wrap;

CREATE FOREIGN TABLE public.ft (
    c1 integer
)
SERVER "asdashSA/sdag";

ALTER FOREIGN TABLE public.ft OWNER TO gpadmin;

-- test syntax sugar, alias, comments

CREATE TABLE public.test
(
    c1 integer NOT NULL,
    c2 int,
    c3 text
) DISTRIBUTED BY (c1);

ALTER TABLE public.test OWNER TO gpadmin;

COMMENT ON TABLE public.test IS 'table comment';

COMMENT ON COLUMN public.test.c1 IS 'column comment';

-- test constraint comments

ALTER TABLE public.test
    ADD CONSTRAINT gpadmin_pk_c1 PRIMARY KEY (c1) WITH (FILLFACTOR = '10');

COMMENT ON CONSTRAINT gpadmin_pk_c1 on public.test is 'constraint comment';

-- test grant all with grant option

REVOKE ALL ON TABLE public.test FROM gpadmin;
GRANT
ALL
ON TABLE public.test TO gpadmin WITH GRANT OPTION;

-- test rules

CREATE RULE r1 AS ON UPDATE TO public.test DO NOTIFY gpadmin;
CREATE RULE r2 AS ON INSERT TO public.test DO NOTHING;
CREATE RULE r3 AS ON DELETE TO public.test DO NOTHING;

ALTER TABLE public.test ENABLE REPLICA RULE r1;
ALTER TABLE public.test DISABLE RULE r2;
ALTER TABLE public.test ENABLE ALWAYS RULE r3;

COMMENT ON RULE r3 ON public.test IS 'test comment';

-- test full text search statements

CREATE TEXT SEARCH PARSER public.testparser (
    START = prsd_start,
    GETTOKEN = prsd_nexttoken,
    END = prsd_end,
    HEADLINE = prsd_headline,
    LEXTYPES = prsd_lextype );

COMMENT ON TEXT SEARCH PARSER public.testparser IS 'is test parser';

CREATE TEXT SEARCH PARSER public.testconfig (
    START = prsd_start,
    GETTOKEN = prsd_nexttoken,
    END = prsd_end,
    HEADLINE = prsd_headline,
    LEXTYPES = prsd_lextype );

ALTER SERVER "asdashSA/sdag" OWNER TO gpadmin;

CREATE TEXT SEARCH TEMPLATE public.testtemplate (
    LEXIZE = dsnowball_lexize );

COMMENT ON TEXT SEARCH TEMPLATE public.testtemplate IS 'is test template';

CREATE TEXT SEARCH DICTIONARY public.testdictionary (
    TEMPLATE = public.testtemplate );

ALTER TEXT SEARCH DICTIONARY public.testdictionary OWNER TO gpadmin;

COMMENT ON TEXT SEARCH DICTIONARY public.testdictionary IS 'is test dictionary';

CREATE TEXT SEARCH CONFIGURATION public.testconfig (
    PARSER = public.testconfig );

ALTER TEXT SEARCH CONFIGURATION public.testconfig OWNER TO gpadmin;

COMMENT ON TEXT SEARCH CONFIGURATION public.testconfig IS 'is test configuration';

ALTER TEXT SEARCH CONFIGURATION public.testconfig
    ADD MAPPING FOR email WITH english_stem;

-- test aggregate

CREATE AGGREGATE public.avg(double precision) (
    SFUNC = float8_accum,
    STYPE = double precision[],
    INITCOND = '{0,0,0}',
    FINALFUNC = float8_avg
);

ALTER AGGREGATE public.avg(double precision) OWNER TO gpadmin;

COMMENT ON FUNCTION public.avg(double precision) IS 'is test aggregate';

-- test schema

CREATE SCHEMA test;

ALTER SCHEMA test OWNER TO gpadmin;

COMMENT ON SCHEMA test IS 'is test schema';

-- test new sequences

CREATE SEQUENCE public.seqtable_c1_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.seqtable_c2_seq
    START WITH 5
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE public.seqtable
(
    c1 bigint NOT NULL DEFAULT nextval('public.seqtable_c1_seq'::regclass),
    c2 integer NOT NULL DEFAULT nextval('public.seqtable_c2_seq'::regclass)
) DISTRIBUTED BY (c1);

ALTER TABLE public.seqtable OWNER TO gpadmin;
ALTER SEQUENCE public.seqtable_c1_seq OWNER TO gpadmin;
ALTER SEQUENCE public.seqtable_c2_seq OWNER TO gpadmin;

ALTER SEQUENCE public.seqtable_c1_seq OWNED BY public.seqtable.c1;
ALTER SEQUENCE public.seqtable_c2_seq OWNED BY public.seqtable.c2;

-- test partition tables

CREATE TABLE public.sales (id int, year int, qtr int, c_rank int, code char(1), region text)
    DISTRIBUTED BY (id)
PARTITION BY LIST (code)
( PARTITION sales VALUES ('S'),
  PARTITION returns VALUES ('R')
);

ALTER TABLE public.sales OWNER TO gpadmin;

-- test mat view

CREATE MATERIALIZED VIEW public.mv1
WITH (fillfactor='90') AS
SELECT 1 AS first,
    2 AS second
WITH NO DATA
    DISTRIBUTED RANDOMLY;

ALTER MATERIALIZED VIEW public.mv1 OWNER TO gpadmin;

-- test operators

CREATE FUNCTION public.pr(boolean, boolean) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
SELECT NULL::BOOLEAN;
$$;

ALTER FUNCTION public.pr(boolean, boolean) OWNER TO gpadmin;

CREATE OPERATOR public.=== (
    PROCEDURE = public.pr,
    LEFTARG = boolean,
    RIGHTARG = boolean,
    COMMUTATOR = OPERATOR(public.===),
    NEGATOR = OPERATOR(public.!==),
    MERGES,
    HASHES,
    JOIN = contjoinsel
);

ALTER OPERATOR public.=== (boolean, boolean) OWNER TO gpadmin;

-- test casts

CREATE FUNCTION public.user_code(integer) RETURNS public.user_code
    LANGUAGE plpgsql
    AS $_$
begin
return ($1, '')::public.user_code;
end;
$_$;

ALTER FUNCTION public.user_code(integer) OWNER TO gpadmin;

CREATE CAST (integer AS public.user_code) WITH FUNCTION public.user_code(integer);

COMMENT ON CAST (integer AS public.user_code) IS 'cast comment';

CREATE SCHEMA country;

ALTER SCHEMA country OWNER TO gpadmin;

CREATE SCHEMA worker;

ALTER SCHEMA worker OWNER TO gpadmin;

CREATE TABLE country.city
(
    id integer
) DISTRIBUTED BY (id);

ALTER TABLE country.city OWNER TO gpadmin;

CREATE TABLE worker.people
(
    fio text
) DISTRIBUTED BY (fio);

ALTER TABLE worker.people OWNER TO gpadmin;

CREATE OR REPLACE FUNCTION country.get_city() RETURNS void
    LANGUAGE sql
    AS $$
    --function body
$$;

ALTER FUNCTION country.get_city() OWNER TO gpadmin;

CREATE OR REPLACE FUNCTION worker.get_changes() RETURNS void
    LANGUAGE sql
    AS $$
$$;

ALTER FUNCTION worker.get_changes() OWNER TO gpadmin;

CREATE SCHEMA ignore1;

ALTER SCHEMA ignore1 OWNER TO gpadmin;

CREATE TABLE ignore1.testschema
(
    fio text
) DISTRIBUTED BY (fio);

ALTER TABLE ignore1.testschema OWNER TO gpadmin;

CREATE OR REPLACE FUNCTION ignore1.get_schema() RETURNS void
    LANGUAGE sql
    AS $$
    --function body
$$;

ALTER FUNCTION ignore1.get_schema() OWNER TO gpadmin;

CREATE SCHEMA ignoreI4vrw;

ALTER SCHEMA ignoreI4vrw OWNER TO gpadmin;

CREATE TABLE ignoreI4vrw.testschema2
(
    fio text
) DISTRIBUTED BY (fio);

ALTER TABLE ignoreI4vrw.testschema2 OWNER TO gpadmin;

CREATE SCHEMA ignore;

ALTER SCHEMA ignore OWNER TO gpadmin;

CREATE TABLE ignore.testtable
(
    fio text
) DISTRIBUTED BY (fio);

ALTER TABLE ignore.testtable OWNER TO gpadmin;

ALTER SCHEMA public OWNER TO gpadmin;