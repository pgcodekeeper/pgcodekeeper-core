CREATE TABLE public.t3 (
    uncompressed_bytes numeric,
    data_bytes numeric,
    col1 numeric GENERATED ALWAYS AS (((uncompressed_bytes)::numeric / (data_bytes)::numeric)) STORED,
    col2 numeric GENERATED ALWAYS AS (uncompressed_bytes * data_bytes) VIRTUAL,
    col3 text COLLATE "C" GENERATED ALWAYS AS ((uncompressed_bytes)::text) STORED
);
