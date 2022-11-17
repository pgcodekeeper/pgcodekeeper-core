CREATE SEQUENCE public.test_seq_1
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.test_seq_10
    START WITH -40
    INCREMENT BY 1
    MINVALUE -40
    MAXVALUE -20
    CACHE 1;

CREATE SEQUENCE public.test_seq_11
    START WITH -40
    INCREMENT BY 1
    MINVALUE -40
    MAXVALUE 90
    CACHE 1;

CREATE SEQUENCE public.test_seq_2
    START WITH 2
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.test_seq_3
    START WITH 2
    INCREMENT BY 3
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.test_seq_4
    START WITH 2
    INCREMENT BY 3
    MINVALUE -4
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.test_seq_5
    START WITH 2
    INCREMENT BY 3
    MINVALUE -4
    MAXVALUE 5
    CACHE 1;

CREATE SEQUENCE public.test_seq_6
    START WITH 2
    INCREMENT BY 3
    MINVALUE -4
    MAXVALUE 5
    CACHE 1;

CREATE SEQUENCE public.test_seq_7
    START WITH 2
    INCREMENT BY 3
    MINVALUE -4
    MAXVALUE 5
    CACHE 1
    CYCLE;

CREATE SEQUENCE public.test_seq_8
    START WITH -1
    INCREMENT BY -1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE public.test_seq_9
    START WITH 20
    INCREMENT BY 1
    MINVALUE 20
    NO MAXVALUE
    CACHE 1;
