CREATE TABLE public.t3 (
    uncompressed_bytes numeric,
    data_bytes numeric,
    col1 numeric(10, 2) GENERATED ALWAYS AS (((uncompressed_bytes)::numeric / (data_bytes)::numeric)) STORED,
    col2 numeric(20, 4) GENERATED ALWAYS AS (uncompressed_bytes * data_bytes) VIRTUAL,
    col3 text COLLATE "POSIX" GENERATED ALWAYS AS ((uncompressed_bytes)::text) STORED
);
