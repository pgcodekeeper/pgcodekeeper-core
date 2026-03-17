SET search_path = pg_catalog;

DROP TRIGGER IF EXISTS trigger_user_audit ON public.users;

CREATE TRIGGER trigger_user_audit
	AFTER INSERT OR UPDATE OR DELETE ON public.users
	FOR EACH ROW
	EXECUTE PROCEDURE log_user_changes();

DROP TRIGGER IF EXISTS trigger_cats_audit ON public.users;

CREATE TRIGGER trigger_cats_audit
	AFTER INSERT OR UPDATE OR DELETE ON public.users
	FOR EACH ROW
	EXECUTE PROCEDURE log_user_changes();

DROP TRIGGER IF EXISTS trigger_dogs_audit ON public.users;

CREATE TRIGGER trigger_dogs_audit
	AFTER INSERT OR UPDATE OR DELETE ON public.users
	FOR EACH ROW
	EXECUTE PROCEDURE log_user_changes();