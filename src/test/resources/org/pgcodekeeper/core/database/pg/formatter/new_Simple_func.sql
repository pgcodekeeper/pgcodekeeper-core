CREATE OR REPLACE FUNCTION count_users()
RETURNS bigint
LANGUAGE sql
AS $$
SELECT id, name FROM users;
$$;