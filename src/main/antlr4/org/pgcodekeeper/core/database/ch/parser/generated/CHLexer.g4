lexer grammar CHLexer;

options {
    caseInsensitive = true;
    superClass = CodeUnitLexer;
}

@header {
import org.pgcodekeeper.core.database.base.parser.CodeUnitLexer;
}

// NOTE: don't forget to add new keywords to the parser rule "keyword"!

// case sensitive privigies
ADDRESS_TO_LINE      options { caseInsensitive = false; } : 'addressToLine';
ADDRESS_TO_LINE_WITH_IN_LINES options { caseInsensitive = false; } : 'addressToLineWithInlines';
ADDRESS_TO_SYMBOL  options { caseInsensitive = false; } : 'addressToSymbol';
DEMANGLE           options { caseInsensitive = false; } : 'demangle';
DICTGET            options { caseInsensitive = false; } : 'dictGet';
DISPLAY_SECRETS_IN_SHOW_AND_SELECT options { caseInsensitive = false; } : 'displaySecretsInShowAndSelect';

// case sensitive data types
AGGREGATE_FUNCTION options { caseInsensitive = false; } : 'Simple'? 'AggregateFunction';
BFLOAT             options { caseInsensitive = false; } : 'BFloat16';
DECIMAL_BIT        options { caseInsensitive = false; } : 'Decimal' ('8' | '16' | '32' | '64' | '128' | '256');
// ARRAY_TYPE: 'Array';
DYNAMIC            options { caseInsensitive = false; } : 'Dynamic';
ENUM               options { caseInsensitive = false; } : 'Enum' ('8'|'16') | [eE] [nN] [uU] [mM];
FIXED_STRING       options { caseInsensitive = false; } : 'FixedString';
FLOAT              options { caseInsensitive = false; } : 'Float' ('32' | '64') | [fF] [lL] [oO] [aA] [tT];
GEOMETRY           options { caseInsensitive = false; } : 'Geometry';
INF                options { caseInsensitive = false; } : '-'? 'Inf';
INT_TYPE           options { caseInsensitive = false; } : 'U'? 'Int' ('8' | '16' | '32' | '64' | '128' | '256');
IPV4               options { caseInsensitive = false; } : 'IPv4' | [iI] [nN] [eE] [tT] '4';
IPV6               options { caseInsensitive = false; } : 'IPv6' | [iI] [nN] [eE] [tT] '6';
LINE_STRING        options { caseInsensitive = false; } : 'Multi'? 'LineString';
LOW_CARDINALITY    options { caseInsensitive = false; } : 'LowCardinality';
MAP                options { caseInsensitive = false; } : 'Map';
MULTI_POLYGON      options { caseInsensitive = false; } : 'MultiPolygon';
NAN                options { caseInsensitive = false; } : 'NaN';
NESTED             options { caseInsensitive = false; } : 'Nested';
NOTHING            options { caseInsensitive = false; } : 'Nothing';
NULLABLE           options { caseInsensitive = false; } : 'Nullable';
OBJECT_TYPE        options { caseInsensitive = false; } : 'Object';
POINT              options { caseInsensitive = false; } : 'Point';
POLYGIN            options { caseInsensitive = false; } : 'Polygon';
QBIT               options { caseInsensitive = false; } : 'QBit';
RING               options { caseInsensitive = false; } : 'Ring';
STRING             options { caseInsensitive = false; } : 'String';
TUPLE              options { caseInsensitive = false; } : 'Tuple';
TIME               options { caseInsensitive = false; } : 'Time' '64'?;
// UUID_TYPE: 'UUID';
INTERVAL_TYPE options { caseInsensitive = false; }
    : 'IntervalDay'
    | 'IntervalHour'
    | 'IntervalMicrosecond'
    | 'IntervalMillisecond'
    | 'IntervalMinute'
    | 'IntervalMonth'
    | 'IntervalNanosecond'
    | 'IntervalQuarter'
    | 'IntervalSecond'
    | 'IntervalWeek'
    | 'IntervalYear'
    ;
VARIANT            options { caseInsensitive = false; } : 'Variant';

// case sensitive options for data_types
MAX_DYNAMIC_PATHS  options { caseInsensitive = false; } : 'max_dynamic_paths';
MAX_DYNAMIC_TYPES  options { caseInsensitive = false; } : 'max_dynamic_types';
MAX_TYPES          options { caseInsensitive = false; } : 'max_types';

// Keywords

ACCESS: 'ACCESS';  // first UNRESERVED KEYWORD, sync with ChKeyword
ADD: 'ADD';
ADMIN: 'ADMIN';
AFTER: 'AFTER';
ALIAS: 'ALIAS';
APPEND: 'APPEND';
APPLY: 'APPLY';
ARBITRARY: 'ARBITRARY';
ASCENDING: 'ASC' | 'ASCENDING';
ASSUME: 'ASSUME';
AST: 'AST';
ASYNC: 'ASYNC';
ASYNCHRONOUS: 'ASYNCHRONOUS';
ATTACH: 'ATTACH';
AUTHENTICATION: 'AUTHENTICATION';
AUTHENTICATIONS: 'AUTHENTICATIONS';
AUTO_INCREMENT: 'AUTO_INCREMENT';
AZURE: 'AZURE';
BACKUP: 'BACKUP';
BEGIN: 'BEGIN';
BIGINT: 'BIGINT';
BINARY: 'BINARY';
BIT: 'BIT';
BLOB: 'BLOB';
BOOL: 'BOOL';
BOOLEAN: 'BOOLEAN';
BOTH: 'BOTH';
BY: 'BY';
BYTE: 'BYTE';
BYTEA: 'BYTEA';
BYTES: 'BYTES';
CACHE: 'CACHE';
CACHES: 'CACHES';
CANCEL: 'CANCEL';
CAST: 'CAST';
CHANGED: 'CHANGED';
CHANGEABLE_IN_READONLY: 'CHANGEABLE_IN_READONLY';
CHAR: 'CHAR';
CHARACTER: 'CHARACTER';
CHECK: 'CHECK';
CLEAR: 'CLEAR';
CLOB: 'CLOB';
CLUSTER: 'CLUSTER';
CLUSTERS: 'CLUSTERS';
CN: 'CN';
CODEC: 'CODEC';
COLLATE: 'COLLATE';
COLLECTION: 'COLLECTION';
COLLECTIONS: 'COLLECTIONS';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMPILED: 'COMPILED';
COMPRESSION: 'COMPRESSION';
CONDITION: 'CONDITION';
CONFIG: 'CONFIG';
CONST: 'CONST';
CONSTRAINT: 'CONSTRAINT';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_USER: 'CURRENT_USER';
CUSTOM: 'CUSTOM';
DATABASE: 'DATABASE';
DATABASES: 'DATABASES';
DATE: 'DATE';
DATE32: 'DATE32';
DATETIME: 'DATETIME';
DATETIME32: 'DATETIME32';
DATETIME64: 'DATETIME64';
DAY: 'DAY';
DAYS: 'DAYS';
DEC: 'DEC';
DECIMAL: 'DECIMAL';
DEDUPLICATE: 'DEDUPLICATE';
DEFAULT: 'DEFAULT';
DEFINER: 'DEFINER';
DELAY: 'DELAY';
DELETE: 'DELETE';
DELETED: 'DELETED';
DEPENDS: 'DEPENDS';
DESC: 'DESC';
DESCENDING: 'DESCENDING';
DESCRIBE: 'DESCRIBE';
DETACH: 'DETACH';
DETACHED: 'DETACHED';
DICTIONARIES: 'DICTIONARIES';
DISK: 'DISK';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DIV : 'DIV';
DNS: 'DNS';
DOUBLE: 'DOUBLE';
EMPTY: 'EMPTY';
ENABLED: 'ENABLED';
ENGINE: 'ENGINE';
ENGINES: 'ENGINES';
EMBEDDED: 'EMBEDDED';
ENTRY: 'ENTRY';
EPHEMERAL: 'EPHEMERAL';
ESTIMATE: 'ESTIMATE';
EVENTS: 'EVENTS';
EVERY: 'EVERY';
EXCHANGE: 'EXCHANGE';
EXECUTION: 'EXECUTION';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXIT: 'EXIT';
EXPLAIN: 'EXPLAIN';
EXPRESSION: 'EXPRESSION';
EXTENDED: 'EXTENDED';
EXTRACT: 'EXTRACT';
FAILED: 'FAILED';
FETCH: 'FETCH';
FETCHES: 'FETCHES';
FIELDS: 'FIELDS';
FILE: 'FILE';
FILES: 'FILES';
FILESYSTEM: 'FILESYSTEM';
FILL: 'FILL';
FIRST: 'FIRST';
FIXED: 'FIXED';
FLUSH: 'FLUSH';
FOLLOWING: 'FOLLOWING';
FORGET: 'FORGET';
FREEZE: 'FREEZE';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GRANT: 'GRANT';
GRANTEES: 'GRANTEES';
GRANTS: 'GRANTS';
GRANULARITY: 'GRANULARITY';
GRPC: 'GRPC';
GROUPING: 'GROUPING';
HDFS: 'HDFS';
HEADER: 'HEADER';
HIERARCHICAL: 'HIERARCHICAL';
HIVE: 'HIVE';
HOST: 'HOST';
HOUR: 'HOUR';
HOURS: 'HOURS';
HTTP: 'HTTP';
HTTPS: 'HTTPS';
ICEBERG: 'ICEBERG';
ID: 'ID';
IDENTIFIED: 'IDENTIFIED';
INHERIT: 'INHERIT';
IMPLICIT: 'IMPLICIT';
IMPERSONATE: 'IMPERSONATE';
INDEX: 'INDEX';
INDEXES: 'INDEXES';
INDICES: 'INDICES';
INFILE: 'INFILE';
INJECTIVE: 'INJECTIVE';
INSERTS: 'INSERTS';
INSTRUMENT: 'INSTRUMENT';
INT: 'INT';
INT1: 'INT1';
INTEGER: 'INTEGER';
INTERPOLATE: 'INTERPOLATE';
INTERVAL: 'INTERVAL';
INTROSPECTION: 'INTROSPECTION';
INVOKER: 'INVOKER';
IS_OBJECT_ID: 'IS_OBJECT_ID';
IP: 'IP';
JDBC: 'JDBC';
JSON: 'JSON';
KAFKA: 'KAFKA';
KEY: 'KEY';
KEYED: 'KEYED';
KEYS: 'KEYS';
KILL: 'KILL';
LARGE: 'LARGE';
LAST: 'LAST';
LIGHTWEIGHT: 'LIGHTWEIGHT';
LAYOUT: 'LAYOUT';
LEADING: 'LEADING';
LEVEL: 'LEVEL';
LIFETIME: 'LIFETIME';
LIMITS: 'LIMITS';
LISTEN: 'LISTEN';
LIVE: 'LIVE';
LOAD: 'LOAD';
LOADING: 'LOADING';
LOCAL: 'LOCAL';
LOG: 'LOG';
LOGS: 'LOGS';
LONGBLOB: 'LONGBLOB';
LONGTEXT: 'LONGTEXT';
MANAGEMENT: 'MANAGEMENT';
MARK: 'MARK';
MASK: 'MASK';
MATERIALIZE: 'MATERIALIZE';
MATERIALIZED: 'MATERIALIZED';
MAX: 'MAX';
MEDIUMBLOB: 'MEDIUMBLOB';
MEDIUMINT: 'MEDIUMINT';
MEDIUMTEXT: 'MEDIUMTEXT';
MERGES: 'MERGES';
METADATA: 'METADATA';
METHODS: 'METHODS';
METRICS: 'METRICS';
MICROSECOND: 'MICROSECOND';
MICROSECONDS: 'MICROSECONDS';
MILLISECOND: 'MILLISECOND';
MILLISECONDS: 'MILLISECONDS';
MIN: 'MIN';
MINUTE: 'MINUTE';
MINUTES: 'MINUTES';
MOD : 'MOD';
MODEL: 'MODEL';
MODELS: 'MODELS';
MODIFY: 'MODIFY';
MONGO: 'MONGO';
MONTH: 'MONTH';
MONTHS: 'MONTHS';
MOVE: 'MOVE';
MOVES: 'MOVES';
MUTATION: 'MUTATION';
MYSQL: 'MYSQL';
NAME: 'NAME';
NAMED: 'NAMED';
NANOSECOND: 'NANOSECOND';
NANOSECONDS: 'NANOSECONDS';
NATS: 'NATS';
NATIONAL: 'NATIONAL';
NCHAR: 'NCHAR';
NEW: 'NEW';
NO: 'NO';
NONE: 'NONE';
NO_PASSWORD: 'NO_PASSWORD';
NULLS: 'NULLS';
NUMERIC: 'NUMERIC';
NVARCHAR: 'NVARCHAR';
OBJECT: 'OBJECT';
ODBC: 'ODBC';
ONLY: 'ONLY';
OPTIMIZE: 'OPTIMIZE';
OPTION: 'OPTION';
OUTER: 'OUTER';
OUTFILE: 'OUTFILE';
OVER: 'OVER';
OVERRIDABLE: 'OVERRIDABLE';
OVERRIDE: 'OVERRIDE';
PARALLEL: 'PARALLEL';
PART: 'PART';
PARTITION: 'PARTITION';
PARTS: 'PARTS';
PERIODIC: 'PERIODIC';
PERMANENTLY : 'PERMANENTLY';
PERMISSIVE: 'PERMISSIVE';
PIPELINE: 'PIPELINE';
PLAN: 'PLAN';
POLICIES: 'POLICIES';
POLICY: 'POLICY';
POPULATE: 'POPULATE';
POSTGRES: 'POSTGRES';
POSTGRESQL: 'POSTGRESQL';
POSTINGS: 'POSTINGS';
PRECEDING: 'PRECEDING';
PRECISION: 'PRECISION';
PRIMARY: 'PRIMARY';
PRIVILEGES: 'PRIVILEGES';
PROCESSLIST: 'PROCESSLIST';
PROFILE: 'PROFILE';
PROFILES: 'PROFILES';
PROMETHEUS: 'PROMETHEUS';
PROTOBUF: 'PROTOBUF';
PROXY: 'PROXY';
PULL: 'PULL';
PULLING: 'PULLING';
PROJECTION: 'PROJECTION';
QUALIFY: 'QUALIFY';
QUARTER: 'QUARTER';
QUARTERS: 'QUARTERS';
QUERIES: 'QUERIES';
QUERY: 'QUERY';
QUOTA: 'QUOTA';
QUOTAS: 'QUOTAS';
QUEUES: 'QUEUES';
RABBITMQ: 'RABBITMQ';
RANDOMIZE: 'RANDOMIZE';
RANDOMIZED: 'RANDOMIZED';
RANGE: 'RANGE';
READ: 'READ';
READONLY: 'READONLY';
REAL: 'REAL';
REALM: 'REALM';
REDIS: 'REDIS';
RECOMPRESS: 'RECOMPRESS';
REFRESH: 'REFRESH';
REGEXP: 'REGEXP';
RELOAD: 'RELOAD';
REMOTE: 'REMOTE';
REMOVE: 'REMOVE';
RENAME: 'RENAME';
REPLACE: 'REPLACE';
REPLICA: 'REPLICA';
REPLICAS: 'REPLICAS';
REPLICATED: 'REPLICATED';
REPLICATION: 'REPLICATION';
RECURSIVE: 'RECURSIVE';
RESET: 'RESET';
RESTART: 'RESTART';
RESTORE: 'RESTORE';
RESTRICTIVE: 'RESTRICTIVE';
RESULT: 'RESULT';
REWRITE: 'REWRITE';
REVOKE: 'REVOKE';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
SAN: 'SAN';
SCHEMA: 'SCHEMA';
SCHEME: 'SCHEME';
SECOND: 'SECOND';
SECONDS: 'SECONDS';
SECRETS: 'SECRETS';
SECURE: 'SECURE';
SECURITY: 'SECURITY';
SELECTS: 'SELECTS';
SENDS: 'SENDS';
SEQUENTIAL: 'SEQUENTIAL';
SERVER: 'SERVER';
SET: 'SET';
SETS: 'SETS';
SETTING: 'SETTING';
SHARD: 'SHARD';
SHARDS: 'SHARDS';
SHOW: 'SHOW';
SHUTDOWN: 'SHUTDOWN';
SIGNED: 'SIGNED';
SINGLE: 'SINGLE';
SKIP_: 'SKIP'; // because SKIP is rezerved by Antlr4
SLEEP: 'SLEEP';
SMALLINT: 'SMALLINT';
SOURCE: 'SOURCE';
SOURCES: 'SOURCES';
SQL: 'SQL';
SQLITE: 'SQLITE';
STALENESS: 'STALENESS';
START: 'START';
STATISTIC: 'STATISTIC';
STDOUT: 'STDOUT';
STEP: 'STEP';
STOP: 'STOP';
STRICT: 'STRICT';
SUBSTRING: 'SUBSTRING';
SYNC: 'SYNC';
SYNTAX: 'SYNTAX';
SYSTEM: 'SYSTEM';
S3: 'S3';
TABLE: 'TABLE';
TABLES: 'TABLES';
TAG: 'TAG';
TCP: 'TCP';
TEMPORARY: 'TEMPORARY';
TEST: 'TEST';
TEXT: 'TEXT';
TIES: 'TIES';
TIMEOUT: 'TIMEOUT';
TIMESTAMP: 'TIMESTAMP';
TINYBLOB: 'TINYBLOB';
TINYINT: 'TINYINT';
TINYTEXT: 'TINYTEXT';
TO: 'TO';
TOP: 'TOP';
TOTALS: 'TOTALS';
TRACKING: 'TRACKING';
TRAILING: 'TRAILING';
TRANSACTION: 'TRANSACTION';
TREE: 'TREE';
TRIM: 'TRIM';
TTL: 'TTL';
TYPE: 'TYPE';
UNBOUNDED: 'UNBOUNDED';
UNFREEZE: 'UNFREEZE';
UNCOMPRESSED: 'UNCOMPRESSED';
UNLOAD: 'UNLOAD';
UNDROP: 'UNDROP';
UNSIGNED: 'UNSIGNED';
UNTIL: 'UNTIL';
URL: 'URL';
USAGE: 'USAGE';
USE: 'USE';
USER: 'USER';
USERS: 'USERS';
VALID: 'VALID';
VALUES: 'VALUES';
VARBINARY: 'VARBINARY';
VARCHAR: 'VARCHAR';
VARCHAR2: 'VARCHAR2';
VARYING: 'VARYING';
VIEW: 'VIEW';
VIEWS: 'VIEWS';
VOLUME: 'VOLUME';
WAIT: 'WAIT';
WATCH: 'WATCH';
WEEK: 'WEEK';
WEEKS: 'WEEKS';
WRITABLE: 'WRITABLE';
WRITTEN: 'WRITTEN';
YEAR: 'YEAR';
YEARS: 'YEARS';
YYYY: 'YYYY';
ZKPATH: 'ZKPATH';   // last UNRESERVED KEYWORD, sync with ChKeyword

/*
==================================================
RESERVED_KEYWORD
==================================================
*/

ALL: 'ALL'; // first RESERVED_KEYWORD, sync with ChParserUtils.normalizeWhitespaceUnquoted, ChKeyword
ALTER: 'ALTER';
AND: 'AND';
ANTI: 'ANTI';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASOF: 'ASOF';
BETWEEN: 'BETWEEN';
CASE: 'CASE';
CREATE: 'CREATE';
CROSS: 'CROSS';
DICTIONARY: 'DICTIONARY';
DROP: 'DROP';
ELSE: 'ELSE';
END: 'END';
EXCEPT: 'EXCEPT';
FINAL: 'FINAL';
FOR: 'FOR';
FORMAT: 'FORMAT';
FROM: 'FROM';
FULL: 'FULL';
GLOBAL: 'GLOBAL';
GROUP: 'GROUP';
HAVING: 'HAVING';
IF: 'IF';
ILIKE: 'ILIKE';
IN: 'IN';
INNER: 'INNER';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTO: 'INTO';
IS: 'IS';
JOIN: 'JOIN';
LEFT: 'LEFT';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
NOT: 'NOT';
NULL: 'NULL';
OFFSET: 'OFFSET';
ON: 'ON';
OR: 'OR';
ORDER: 'ORDER';
PASTE: 'PASTE';
PREWHERE: 'PREWHERE';
RIGHT: 'RIGHT';
SAMPLE: 'SAMPLE';
SELECT: 'SELECT';
SEMI: 'SEMI';
SETTINGS: 'SETTINGS';
THEN: 'THEN';
TRUNCATE: 'TRUNCATE';
UNION: 'UNION';
UPDATE: 'UPDATE';
USING: 'USING';
UUID: 'UUID';
WHEN: 'WHEN';
WHERE: 'WHERE';
WINDOW: 'WINDOW';
WITH: 'WITH'; // last RESERVED_KEYWORD, sync with ChParserUtils.normalizeWhitespaceUnquoted, ChKeyword

// Tokens

NUMBER
    : '0' 'B' BINARY_DIGIT (BINARY_DIGIT | UNDERSCORE)*
    | DEC_DIGIT (DEC_DIGIT | UNDERSCORE)*
    | '0' 'X' HEX_DIGIT+
    ;

IDENTIFIER
    : (LETTER | UNDERSCORE) (LETTER | UNDERSCORE | DEC_DIGIT)*
    ;

GLOBAL_VARIABLE: '@''@' IDENTIFIER;

FLOATING_LITERAL
    : NUMBER+ '.' EXPONENT?
    | HEXADECIMAL_NUMERIC_LITERAL (DOT HEX_DIGIT*)? ('P' | 'E') ('+'|'-')? DEC_DIGIT+
    | NUMBER+ '.' NUMBER+ EXPONENT?
//  | '.' NUMBER+ EXPONENT?
    | NUMBER+ EXPONENT
    ;

HEXADECIMAL_NUMERIC_LITERAL: '0' 'X' HEX_DIGIT+;

// It's important that quote-symbol is a single character.
STRING_LITERAL: QUOTE_SINGLE ( ~([\\']) | (BACKSLASH .) | (QUOTE_SINGLE QUOTE_SINGLE) )* QUOTE_SINGLE {calculateOffset();};
BINARY_LITERAL: 'X' QUOTE_SINGLE HEX_DIGIT* QUOTE_SINGLE;
// DOLLAR_LITERAL: '$' IDENTIFIER? '$' (DOLLAR_LITERAL |.)*? '$' IDENTIFIER? '$';

// Alphabet and allowed symbols

fragment LETTER: [A-Z];
fragment DEC_DIGIT: [0-9];
fragment HEX_DIGIT: [0-9A-F];
fragment BINARY_DIGIT: [0-1];

fragment EXPONENT : 'E' ('+'|'-')? DEC_DIGIT+;

ARROW: '->';
ASTERISK: '*';
BACKQUOTE: '`';
BACKSLASH: '\\';
COLON: ':';
COMMA: ',';
CONCAT: '||';
MINUS: '-';
DOT: '.';
EQ_DOUBLE: '==';
EQ_SINGLE: '=';
GE: '>=';
GT: '>';
LBRACE: '{';
LBRACKET: '[';
LE: '<=';
LPAREN: '(';
LT: '<';
NOT_EQ: '!=' | '<>';
PERCENT: '%';
PLUS: '+';
QUESTION: '?';
QUOTE_DOUBLE: '"';
QUOTE_SINGLE: '\'';
RBRACE: '}';
RBRACKET: ']';
RPAREN: ')';
SEMICOLON: ';';
SLASH: '/';
UNDERSCORE: '_';
CAST_EXPRESSION: ':'':';
NOT_DIST: '<' '=' '>';

BOM: '\ufeff';

// Comments and whitespace

BLOCK_COMMENT: '/*' (BLOCK_COMMENT |.)*? '*/' -> channel(HIDDEN);
LINE_COMMENT: '--' ~[\r\n]* -> channel(HIDDEN);

SPACE: ' ' -> channel(HIDDEN);
WHITESPACE: [\u000B\u000C] -> channel(HIDDEN);
NEW_LINE : [\n\r] {resetLineOffset();} -> channel(HIDDEN);
TAB : '\t' -> channel(HIDDEN);

/* Quoted Identifiers
*
* These are divided into four separate tokens, allowing distinction of valid quoted identifiers from invalid quoted
* identifiers without sacrificing the ability of the lexer to reliably recover from lexical errors in the input.
*/
DOUBLE_QUOTED_IDENTIFIER: UNTERMINATED_DOUBLE_QUOTED_IDENTIFIER '"' {removeQuotes("\"\"", "\"");};
BACK_QUOTED_IDENTIFIER: UNTERMINATED_BACK_QUOTED_IDENTIFIER '`' {removeQuotes("``", "`");};

// This is a quoted identifier which only contains valid characters but is not terminated
fragment UNTERMINATED_BACK_QUOTED_IDENTIFIER: '`' ( '``' | ~[\u0000`] )*;
fragment UNTERMINATED_DOUBLE_QUOTED_IDENTIFIER: '"' ( '""' | ~[\u0000"] )*;
