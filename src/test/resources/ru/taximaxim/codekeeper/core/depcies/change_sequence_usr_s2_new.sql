CREATE SEQUENCE public.s2
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

ALTER SEQUENCE public.s2
    OWNED BY public.t1.c1;
