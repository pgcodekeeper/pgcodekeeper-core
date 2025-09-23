CREATE TABLE public.testtable (
    field1 integer NOT NULL,
    field2 integer CONSTRAINT field2_not_null_test NOT NULL NO INHERIT,
    field3 character varying(150) DEFAULT 'none'::character varying NOT NULL NO INHERIT,
    field4 double precision CONSTRAINT field4_not_null_test NOT NULL,
    field5 integer CONSTRAINT field5_not_null_test NOT NULL NO INHERIT,
    field6 integer NOT NULL,
    field7 integer NOT NULL,
    field8 integer NOT NULL,
    field9 integer NOT NULL
);