SET search_path = pg_catalog;

DROP RULE IF EXISTS rule_disable ON public.t1;

CREATE RULE rule_disable AS
    ON INSERT TO public.t1 DO NOTHING;

DROP RULE IF EXISTS rule_enable ON public.t1;

CREATE RULE rule_enable AS
    ON INSERT TO public.t1 DO NOTHING;

DROP RULE IF EXISTS rule_enable_replica ON public.t1;

CREATE RULE rule_enable_replica AS
    ON INSERT TO public.t1 DO NOTHING;

DROP RULE IF EXISTS rule_enable_always ON public.t1;

CREATE RULE rule_enable_always AS
    ON INSERT TO public.t1 DO NOTHING;