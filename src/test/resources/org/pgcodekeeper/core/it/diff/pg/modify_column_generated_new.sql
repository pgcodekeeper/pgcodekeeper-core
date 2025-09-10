CREATE TABLE public.people (
    height_cm numeric,
    height_in numeric GENERATED ALWAYS AS (height_cm / 4.32) STORED,
    height_m numeric GENERATED ALWAYS AS (height_cm / 100) VIRTUAL,
    height_virtual numeric GENERATED ALWAYS AS (height_cm / 100) STORED,
    height_implicit1 numeric GENERATED ALWAYS AS (height_cm / 100) VIRTUAL,
    height_implicit2 numeric GENERATED ALWAYS AS (height_cm / 100) STORED
);