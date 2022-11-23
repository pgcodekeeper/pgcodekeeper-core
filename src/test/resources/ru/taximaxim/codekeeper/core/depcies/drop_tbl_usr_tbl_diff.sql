SET search_path = pg_catalog;

-- DEPCY: This TRIGGER depends on the TABLE: public.t1

DROP TRIGGER trig1 ON public.t1;

-- DEPCY: This INDEX depends on the TABLE: public.t1

DROP INDEX public.ind1;

DROP TABLE public.t1;