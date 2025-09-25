CREATE TABLE public.t1 (
    e1 bigint,
    e2 int,
    e3 int
);

CREATE TABLE public.t2 (
    b1 bigint,
    b2 int,
    b3 int
);

ALTER TABLE public.t2
ADD CONSTRAINT fk_complex_example 
FOREIGN KEY (b2, b3, PERIOD b1) 
REFERENCES public.t1(e1, e2, PERIOD e3) 
ON DELETE CASCADE 
ON UPDATE RESTRICT 
DEFERRABLE INITIALLY DEFERRED NOT ENFORCED;