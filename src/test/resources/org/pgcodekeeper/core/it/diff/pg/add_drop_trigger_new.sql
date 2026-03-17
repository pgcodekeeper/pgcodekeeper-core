CREATE TABLE public.users (
    id bigint,
    first_name VARCHAR(50),
    last_name VARCHAR(50)
);

CREATE TRIGGER trigger_user_audit
    AFTER INSERT OR UPDATE OR DELETE ON public.users
    FOR EACH ROW
    EXECUTE FUNCTION log_user_changes();
--------------------------------------

CREATE TRIGGER trigger_cats_audit
    AFTER INSERT OR UPDATE OR DELETE ON public.users
    FOR EACH ROW
    EXECUTE FUNCTION log_user_changes();
--------------------------------------

CREATE TRIGGER trigger_dogs_audit
    AFTER INSERT OR UPDATE OR DELETE ON public.users
    FOR EACH ROW
    EXECUTE FUNCTION log_user_changes();
--------------------------------------