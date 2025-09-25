CREATE TABLE public.testtable (
    field1 integer,
    field2 integer,
    field3 character varying(150) DEFAULT 'none'::character varying NOT NULL,
    field4 double precision
);

ALTER TABLE public.testtable add NOT NULL field2;

ALTER TABLE public.testtable add constraint test_not_null NOT NULL field4 NO INHERIT;

CREATE TABLE public.testtable2 (
    field1 integer not null,
    field2 integer constraint test_not_null_table2 not null no inherit
);