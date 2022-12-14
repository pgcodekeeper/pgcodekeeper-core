--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.3
-- Dumped by pg_dump version 9.6.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: test; Type: SCHEMA; Schema: -; Owner: galiev_mr
--

CREATE SCHEMA test;


ALTER SCHEMA test OWNER TO galiev_mr;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

--CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

--COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = pg_catalog;

--
-- Name: user_code; Type: TYPE; Schema: public; Owner: galiev_mr
--

CREATE TYPE public.user_code AS (
    f1 integer,
    f2 text
);


ALTER TYPE public.user_code OWNER TO galiev_mr;

--
-- Name: emp_stamp(); Type: FUNCTION; Schema: public; Owner: galiev_mr
--

CREATE FUNCTION public.emp_stamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        -- Check that empname and salary are given
        IF NEW.empname IS NULL THEN
            RAISE EXCEPTION 'empname cannot be null';
        END IF;
        IF NEW.salary IS NULL THEN
            RAISE EXCEPTION '% cannot have null salary', NEW.empname;
        END IF;

        -- Who works for us when they must pay for it?
        IF NEW.salary < 0 THEN
            RAISE EXCEPTION '% cannot have a negative salary', NEW.empname;
        END IF;

        -- Remember who changed the payroll when
        NEW.last_date := current_timestamp;
        NEW.last_user := current_user;
        RETURN NEW;
    END;
$$;


ALTER FUNCTION public.emp_stamp() OWNER TO galiev_mr;

--
-- Name: increment(integer); Type: FUNCTION; Schema: public; Owner: galiev_mr
--

CREATE FUNCTION public.increment(i integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
        BEGIN
                RETURN i + 1;
        END;
$$;


ALTER FUNCTION public.increment(i integer) OWNER TO galiev_mr;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: emp; Type: TABLE; Schema: public; Owner: galiev_mr
--

CREATE TABLE public.emp (
    id integer NOT NULL,
    empname text,
    salary integer,
    last_date timestamp without time zone,
    last_user text,
    code public.user_code
);


ALTER TABLE public.emp OWNER TO galiev_mr;

--
-- Name: emp_id_seq; Type: SEQUENCE; Schema: public; Owner: galiev_mr
--

CREATE SEQUENCE public.emp_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.emp_id_seq OWNER TO galiev_mr;

--
-- Name: emp_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: galiev_mr
--

ALTER SEQUENCE public.emp_id_seq OWNED BY public.emp.id;


--
-- Name: emp_view; Type: VIEW; Schema: public; Owner: galiev_mr
--

CREATE VIEW public.emp_view AS
 SELECT emp.empname,
    emp.last_date,
    increment(emp.salary) AS salary,
    emp.code
   FROM public.emp;


ALTER TABLE public.emp_view OWNER TO galiev_mr;

--
-- Name: emp id; Type: DEFAULT; Schema: public; Owner: galiev_mr
--

ALTER TABLE ONLY public.emp ALTER COLUMN id SET DEFAULT nextval('public.emp_id_seq'::regclass);


--
-- Name: name_ind; Type: INDEX; Schema: public; Owner: galiev_mr
--

CREATE UNIQUE INDEX name_ind ON public.emp USING btree (empname);


--
-- Name: emp notify_me; Type: RULE; Schema: public; Owner: galiev_mr
--

CREATE RULE notify_me AS
    ON UPDATE TO public.emp DO
 NOTIFY emp;


--
-- Name: emp emp_stamp; Type: TRIGGER; Schema: public; Owner: galiev_mr
--

CREATE TRIGGER emp_stamp BEFORE INSERT OR UPDATE ON public.emp FOR EACH ROW EXECUTE PROCEDURE public.emp_stamp();


--
-- PostgreSQL database dump complete
--

