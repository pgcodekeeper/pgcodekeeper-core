SET search_path = pg_catalog;

-- DEPCY: This CONSTRAINT period2_pkey depends on the TABLE: public.period

ALTER TABLE public.period
	DROP CONSTRAINT period2_pkey;

ALTER TABLE public.period RENAME TO period_randomly_generated_part;

ALTER TABLE worker.period RENAME TO period_randomly_generated_part;

-- DEPCY: This CONSTRAINT period2_pkey depends on the TABLE: "MySchema"."MyTable"

ALTER TABLE "MySchema"."MyTable"
	DROP CONSTRAINT period2_pkey;

ALTER TABLE "MySchema"."MyTable" RENAME TO "MyTable_randomly_generated_part";

CREATE TABLE public.period (
	rid integer DEFAULT nextval('public.period_rid_seq'::regclass) NOT NULL,
	pyear smallint,
	pnum smallint,
	upd integer,
	pfs date,
	pfe date
);

ALTER TABLE public.period OWNER TO khazieva_gr;

INSERT INTO public.period(rid, pyear, pnum, upd, pfs, pfe)
SELECT rid, pyear, pnum, upd, pfs, pfe FROM public.period_randomly_generated_part;

ALTER SEQUENCE public.period_rid_seq
	OWNED BY public.period.rid;

DROP TABLE public.period_randomly_generated_part;

ALTER TABLE public.period
	ADD CONSTRAINT period2_pkey PRIMARY KEY (rid);

CREATE TABLE worker.period (
	rid integer,
	pyear smallint,
	pnum smallint,
	upd integer,
	pfs date,
	pfe date
);

ALTER TABLE worker.period OWNER TO khazieva_gr;

INSERT INTO worker.period(rid, pyear, pnum, upd, pfs, pfe)
SELECT rid, pyear, pnum, upd, pfs, pfe FROM worker.period_randomly_generated_part;

DROP TABLE worker.period_randomly_generated_part;

CREATE TABLE "MySchema"."MyTable" (
	rid integer DEFAULT nextval('"MySchema"."period_rid_seq"'::regclass) NOT NULL,
	pyear smallint,
	pnum smallint,
	upd integer,
	pfs date,
	pfe date
);

ALTER TABLE "MySchema"."MyTable" OWNER TO khazieva_gr;

INSERT INTO "MySchema"."MyTable"(rid, pyear, pnum, upd, pfs, pfe)
SELECT rid, pyear, pnum, upd, pfs, pfe FROM "MySchema"."MyTable_randomly_generated_part";

ALTER SEQUENCE "MySchema".period_rid_seq
	OWNED BY "MySchema"."MyTable".rid;

DROP TABLE "MySchema"."MyTable_randomly_generated_part";

ALTER TABLE "MySchema"."MyTable"
	ADD CONSTRAINT period2_pkey PRIMARY KEY (rid);