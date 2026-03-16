 -- test if tables have equal name period but differnet schema
CREATE SCHEMA worker;

CREATE TABLE worker.period (
    rid integer,
    pyear smallint,
    pnum smallint,
    pfs date,
    pfe date,
    upd integer
);

ALTER TABLE worker.period OWNER TO khazieva_gr;

-----------------------------------------------------------
CREATE TABLE public.period (
    rid integer DEFAULT nextval('public.period_rid_seq'::regclass) NOT NULL,
    pyear smallint,
    pnum smallint,
    pfs date,
    pfe date,
    upd integer
);

ALTER TABLE public.period OWNER TO khazieva_gr;

CREATE SEQUENCE public.period_rid_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

ALTER TABLE public.period
    ADD CONSTRAINT period2_pkey PRIMARY KEY (rid);

ALTER SEQUENCE public.period_rid_seq OWNED BY public.period.rid;

--------------------------------------------------------------------
 -- test if schema, table, sequence have quoted name
CREATE SCHEMA "MySchema";

CREATE TABLE "MySchema"."MyTable" (
    rid integer DEFAULT nextval('"MySchema"."period_rid_seq"'::regclass) NOT NULL,
    pyear smallint,
    pnum smallint,
    pfs date,
    pfe date,
    upd integer
);

ALTER TABLE "MySchema"."MyTable" OWNER TO khazieva_gr;

CREATE SEQUENCE "MySchema"."period_rid_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

ALTER TABLE "MySchema"."MyTable"
    ADD CONSTRAINT period2_pkey PRIMARY KEY (rid);

ALTER SEQUENCE "MySchema"."period_rid_seq" OWNED BY "MySchema"."MyTable".rid;