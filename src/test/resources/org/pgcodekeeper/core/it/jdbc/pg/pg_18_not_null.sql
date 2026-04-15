SET search_path = pg_catalog;

CREATE TABLE public.user_code_some_long_table_name (
    some_long_column_name_for_some_long_table_name integer CONSTRAINT con1 NOT NULL,
    f2 text
);

ALTER TABLE public.user_code_some_long_table_name OWNER TO test;

CREATE TABLE public.user_code_some_long_table_name2 (
    some_long_column_name_for_some_long_table_name integer CONSTRAINT con2 NOT NULL,
    f2 text
);

ALTER TABLE public.user_code_some_long_table_name2 OWNER TO test;

ALTER SCHEMA public OWNER TO pg_database_owner;

REVOKE ALL ON SCHEMA public FROM PUBLIC;

REVOKE ALL ON SCHEMA public FROM pg_database_owner;

GRANT ALL ON SCHEMA public TO pg_database_owner;

GRANT USAGE ON SCHEMA public TO PUBLIC;