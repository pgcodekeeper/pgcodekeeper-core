CREATE TABLE public.test_table (
	a1 integer,
	a2 integer,
	a3 integer
);

ALTER TABLE public.test_table OWNER TO andrey;

CREATE TABLE public.ignored_table (
	a1 integer,
	a2 integer,
	a3 integer
);

ALTER TABLE public.ignored_table OWNER TO andrey;