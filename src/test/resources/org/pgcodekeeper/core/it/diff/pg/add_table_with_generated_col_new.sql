CREATE TABLE public.people (
    height_cm numeric,
    height_in numeric GENERATED ALWAYS AS (height_cm / 2.54) STORED,
    height_in_virtual numeric GENERATED ALWAYS AS (height_cm / 2.54) VIRTUAL,
    height_implicit numeric GENERATED ALWAYS AS (height_cm / 2.54)
);