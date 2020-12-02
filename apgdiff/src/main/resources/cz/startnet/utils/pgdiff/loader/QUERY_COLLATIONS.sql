WITH sys_schemas AS (
    SELECT n.oid
    FROM pg_catalog.pg_namespace n
    WHERE n.nspname LIKE 'pg\_%'
        OR n.nspname = 'information_schema'
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_depend dp WHERE dp.objid = n.oid AND dp.deptype = 'e')
)

SELECT  c.collnamespace AS schema_oid,
        c.oid::bigint,
        c.collcollate,
        c.collctype,
        c.collowner::bigint
       
FROM pg_catalog.pg_collation c
WHERE c.collnamespace NOT IN (SELECT oid FROM sys_schemas)
