CREATE TABLE public.test_table4 (
    col1 integer NOT NULL,
    col2 text CONSTRAINT payload_nn4 NOT NULL,
	col3 text
);

COMMENT ON CONSTRAINT payload_nn4 ON public.test_table4 IS 'test_constraint_comment';

ALTER TABLE public.test_table4
    ADD CONSTRAINT col3_nn4 NOT NULL col3 NOT VALID;

COMMENT ON CONSTRAINT col3_nn4 ON public.test_table4 IS 'test_constraint_comment';

CREATE TABLE public.partitioned_parent (
    id integer CONSTRAINT part_id_nn NOT NULL,
    created_at date NOT NULL,
    data text
) PARTITION BY RANGE (created_at);

CREATE TABLE public.partition_2024 PARTITION OF public.partitioned_parent
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE public.partition_2025 PARTITION OF public.partitioned_parent (
    data CONSTRAINT data_nn NOT NULL
)
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

COMMENT ON CONSTRAINT data_nn ON public.partition_2025 IS 'test_constraint_comment';

CREATE TABLE public.partition_2026 PARTITION OF public.partitioned_parent (
    data NOT NULL
)
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE public.partition_2027 PARTITION OF public.partitioned_parent
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

ALTER TABLE public.partition_2027 ADD NOT NULL data NOT VALID;

CREATE TABLE public.products (
    id serial PRIMARY KEY,
    name text NOT NULL,
    description text
);

CREATE TABLE public.books (
    isbn text
) INHERITS (public.products);

ALTER TABLE public.books ALTER COLUMN description SET NOT NULL;