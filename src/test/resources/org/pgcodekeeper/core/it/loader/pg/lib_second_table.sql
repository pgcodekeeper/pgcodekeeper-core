CREATE TABLE public.lib_second_table (id integer NOT NULL, value text);
ALTER TABLE public.lib_second_table OWNER TO lib_user;
GRANT SELECT ON TABLE public.lib_second_table TO some_role;
