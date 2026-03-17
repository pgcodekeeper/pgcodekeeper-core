SET search_path = pg_catalog;

DROP VIEW IF EXISTS public.active_users;

CREATE VIEW public.active_users AS
	SELECT id, username, email, created_at
FROM public.users
WHERE is_active = true;

DROP VIEW IF EXISTS public.active_cats;

CREATE VIEW public.active_cats AS
	SELECT id, username, email, created_at
FROM public.cats
WHERE is_active = true;

DROP VIEW IF EXISTS public.active_dogs;

CREATE VIEW public.active_dogs AS
	SELECT id, username, email, created_at
FROM public.dogs
WHERE is_active = true;