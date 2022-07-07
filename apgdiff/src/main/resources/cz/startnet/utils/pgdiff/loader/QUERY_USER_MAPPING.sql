WITH extension_deps AS (
    SELECT dep.objid 
    FROM pg_catalog.pg_depend dep 
    WHERE dep.classid = 'pg_catalog.pg_user_mapping'::pg_catalog.regclass 
        AND dep.deptype = 'e'
)

SELECT um.oid::bigint,
       um.umuser::bigint as username,
       fsrv.srvname as servername,
       um.umoptions 
FROM pg_catalog.pg_user_mapping um
LEFT JOIN pg_catalog.pg_foreign_server fsrv ON um.umserver = fsrv.oid
WHERE um.oid NOT IN (SELECT objid FROM extension_deps)
