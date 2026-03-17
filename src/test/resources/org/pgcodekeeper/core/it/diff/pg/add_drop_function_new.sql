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

----------------------------------------------
CREATE OR REPLACE FUNCTION public.calculate_rectangle_area(length NUMERIC, width NUMERIC) RETURNS NUMERIC
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN length * width;
END;
$$;
--------------------------------------------------
CREATE OR REPLACE FUNCTION public.calculate_square(num INTEGER) RETURNS INTEGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN num * num;
END;
$$;