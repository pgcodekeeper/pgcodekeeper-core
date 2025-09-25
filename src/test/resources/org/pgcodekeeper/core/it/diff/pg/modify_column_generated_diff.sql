SET search_path = pg_catalog;

ALTER TABLE ONLY public.people
	DROP COLUMN height_in;

ALTER TABLE ONLY public.people
	DROP COLUMN height_m;

ALTER TABLE ONLY public.people
	DROP COLUMN height_virtual;

ALTER TABLE ONLY public.people
	DROP COLUMN height_implicit2;

ALTER TABLE public.people
	ADD COLUMN height_in numeric GENERATED ALWAYS AS (height_cm / 4.32) STORED;

ALTER TABLE public.people
	ADD COLUMN height_m numeric GENERATED ALWAYS AS (height_cm / 100);

ALTER TABLE public.people
	ADD COLUMN height_virtual numeric GENERATED ALWAYS AS (height_cm / 100) STORED;

ALTER TABLE public.people
	ADD COLUMN height_implicit2 numeric GENERATED ALWAYS AS (height_cm / 100) STORED;