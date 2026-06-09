SET search_path = pg_catalog;

ALTER TABLE ONLY public.t3
	DROP COLUMN col1;

ALTER TABLE ONLY public.t3
	DROP COLUMN col2;

ALTER TABLE ONLY public.t3
	DROP COLUMN col3;

ALTER TABLE public.t3
	ADD COLUMN col1 numeric(10, 2) GENERATED ALWAYS AS (((uncompressed_bytes)::numeric / (data_bytes)::numeric)) STORED;

ALTER TABLE public.t3
	ADD COLUMN col2 numeric(20, 4) GENERATED ALWAYS AS (uncompressed_bytes * data_bytes);

ALTER TABLE public.t3
	ADD COLUMN col3 text COLLATE "POSIX" GENERATED ALWAYS AS ((uncompressed_bytes)::text) STORED;
