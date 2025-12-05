CREATE TABLE public.testtable (
    field1 integer NOT NULL,
    field2 integer CONSTRAINT field2_not_null_test NOT NULL NO INHERIT,
    field3 character varying(150) DEFAULT 'none'::character varying NOT NULL NO INHERIT,
    field4 double precision CONSTRAINT field4_not_null_test NOT NULL,
    field5 integer CONSTRAINT field5_not_null_test NOT NULL NO INHERIT,
    field6 integer NOT NULL,
    field7 integer NOT NULL,
    field8 integer NOT NULL,
    field9 integer NOT NULL,
    field10 integer NOT NULL,
    field11 integer,
    very_very_very_very_very_very_very_very_long_column_name integer NOT NULL
);

ALTER TABLE public.testtable
    ADD CONSTRAINT testtable_field11_not_null NOT NULL field11 NOT VALID;

CREATE TABLE public."👨‍👩‍👧‍👦👨‍👩‍👧‍👦" (
    "🇷🇺🇷🇺🇷🇺" integer NOT NULL,
    "шестьдесятбайт_шестьдесятбайт_ш" integer NOT NULL
);