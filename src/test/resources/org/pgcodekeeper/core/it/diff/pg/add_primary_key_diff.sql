SET search_path = pg_catalog;

ALTER TABLE public.t2
	ADD CONSTRAINT pk_t2_composite PRIMARY KEY (b1, b2, b3 WITHOUT OVERLAPS) WITH (fillfactor=80)
	USING INDEX TABLESPACE pg_default DEFERRABLE;