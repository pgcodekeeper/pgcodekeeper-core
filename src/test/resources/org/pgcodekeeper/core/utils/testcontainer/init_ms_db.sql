CREATE DATABASE test_db;

ALTER DATABASE test_db
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

CREATE DATABASE test_db_memory_optimized;

ALTER DATABASE test_db_memory_optimized
ADD FILEGROUP memory_optimized_fg
CONTAINS MEMORY_OPTIMIZED_DATA;

ALTER DATABASE test_db_memory_optimized
ADD FILE (
    NAME = memory_optimized_container,
    FILENAME = '/var/opt/mssql/data/test_db_mopt'
)
TO FILEGROUP memory_optimized_fg;