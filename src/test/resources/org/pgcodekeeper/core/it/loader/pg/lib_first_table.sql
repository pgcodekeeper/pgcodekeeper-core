CREATE TABLE public.lib_first_table (id integer NOT NULL, name text);
ALTER TABLE public.lib_first_table OWNER TO lib_user;
GRANT SELECT ON TABLE public.lib_first_table TO some_role;
