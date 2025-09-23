SET search_path = pg_catalog;

ALTER TABLE public.testtable
	ALTER CONSTRAINT testtable_field1_not_null NO INHERIT;

ALTER TABLE ONLY public.testtable
	ALTER COLUMN field2 DROP NOT NULL;

ALTER TABLE ONLY public.testtable
	ALTER COLUMN field3 DROP NOT NULL;

ALTER TABLE public.testtable
	RENAME CONSTRAINT field4_not_null_test TO field4_not_null_test_renamed;

ALTER TABLE public.testtable
	ALTER CONSTRAINT field4_not_null_test_renamed NO INHERIT;

ALTER TABLE public.testtable
	RENAME CONSTRAINT field5_not_null_test TO testtable_field5_not_null;

ALTER TABLE public.testtable
	ALTER CONSTRAINT testtable_field5_not_null INHERIT;

ALTER TABLE public.testtable
	RENAME CONSTRAINT testtable_field6_not_null TO field6_not_null_test;

ALTER TABLE public.testtable
	ALTER CONSTRAINT field6_not_null_test NO INHERIT;

ALTER TABLE public.testtable
	RENAME CONSTRAINT testtable_field7_not_null TO field7_not_null_test;

ALTER TABLE public.testtable
	ALTER CONSTRAINT field7_not_null_test NO INHERIT;