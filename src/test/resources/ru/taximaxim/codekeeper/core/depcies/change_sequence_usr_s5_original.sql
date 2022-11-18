CREATE SEQUENCE public.s5
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

ALTER SEQUENCE public.s5
    OWNED BY public.t5.c1;