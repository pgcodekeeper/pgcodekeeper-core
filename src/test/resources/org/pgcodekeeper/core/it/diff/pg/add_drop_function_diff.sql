SET search_path = pg_catalog;

DROP FUNCTION IF EXISTS public.avg_sfunc(state avg_pair, value NUMERIC);

CREATE OR REPLACE FUNCTION public.avg_sfunc(state avg_pair, value NUMERIC) RETURNS avg_pair
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF value IS NOT NULL THEN
        state.sum := state.sum + value;
        state.count := state.count + 1;
    END IF;
    RETURN state;
END;
$$;

DROP FUNCTION IF EXISTS public.calculate_rectangle_area(length NUMERIC, width NUMERIC);

CREATE OR REPLACE FUNCTION public.calculate_rectangle_area(length NUMERIC, width NUMERIC) RETURNS NUMERIC
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN length * width;
END;
$$;

DROP FUNCTION IF EXISTS public.calculate_square(num INTEGER);

CREATE OR REPLACE FUNCTION public.calculate_square(num INTEGER) RETURNS INTEGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN num * num;
END;
$$;