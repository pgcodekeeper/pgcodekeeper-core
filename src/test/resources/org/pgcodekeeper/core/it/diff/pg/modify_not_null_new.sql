CREATE TABLE public.testtable (
    field1 integer NOT NULL NO INHERIT,
    field2 integer,
    field3 character varying(150) DEFAULT 'none'::character varying,
    field4 double precision CONSTRAINT field4_not_null_test_renamed NOT NULL NO INHERIT,
    field5 integer NOT NULL,
    field6 integer CONSTRAINT field6_not_null_test NOT NULL NO INHERIT,
    field7 integer,
    field8 integer,
    field9 integer NOT NULL,
    field10 integer constraint testtable_field10_not_null NOT NULL,
    CONSTRAINT field7_not_null_test NOT NULL field7 NO INHERIT,
    CONSTRAINT testtable_field8_not_null NOT NULL field8
);