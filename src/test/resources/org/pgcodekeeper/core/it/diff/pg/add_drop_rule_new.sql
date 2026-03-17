CREATE TABLE public.t1();

CREATE RULE rule_disable AS ON INSERT TO public.t1 DO NOTHING;
CREATE RULE rule_enable AS ON INSERT TO public.t1 DO NOTHING;
CREATE RULE rule_enable_replica AS ON INSERT TO public.t1 DO NOTHING;
CREATE RULE rule_enable_always AS ON INSERT TO public.t1 DO NOTHING;