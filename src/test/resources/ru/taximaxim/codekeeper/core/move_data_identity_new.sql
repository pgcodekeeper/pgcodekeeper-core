COMMENT ON SCHEMA public IS 'Standard public schema';


CREATE TABLE public.tbl (
    did integer NOT NULL,
    did_2 integer NOT NULL,
    name22 character varying(40) NOT NULL,
    event_time timestamp without time zone DEFAULT now() NOT NULL,
    description integer DEFAULT 55777,
    calculated bigint GENERATED ALWAYS AS ((did + 2000)) STORED
);

ALTER TABLE public.tbl ALTER COLUMN did ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.tbl_did_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1
);

ALTER TABLE public.tbl ALTER COLUMN did_2 ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.tbl_did_2_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1
);

--------------------------------------------------------------------------------

ALTER TABLE public.tbl
    ADD CONSTRAINT tbl_pkey PRIMARY KEY (did);

--------------------------------------------------------------------------------

ALTER TABLE public.tbl
    ADD CONSTRAINT tbl_name22_check CHECK (((name22)::text <> ''::text));


CREATE OR REPLACE FUNCTION public.events_insert_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  UPDATE public.tbl
  SET description = (SELECT trunc(random() * 100 + 1)) 
  WHERE did_2 = 2;
  RETURN NULL;
END;
$$;

CREATE TRIGGER events_insert
    AFTER INSERT ON public.tbl
    FOR EACH ROW
    EXECUTE PROCEDURE public.events_insert_trigger();


CREATE VIEW public.v AS
    SELECT tbl.name22,
    tbl.description,
    1 AS qwerty
   FROM public.tbl;


REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;