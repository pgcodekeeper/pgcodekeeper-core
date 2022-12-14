package ru.taximaxim.codekeeper.core.loader;

import java.io.IOException;
import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.taximaxim.codekeeper.core.fileutils.FileUtils;

/**
 * For every field in this class that starts with 'QUERY_'
 * the static initializer tries to: <br>
 * - if the field is String: find a file named %FIELD_NAME%.sql in this package
 *   and assign its contents to the field.<br>
 * - if the field is Map: load %FIELD_NAME%.sql as described above and map its contents to null,
 *   try to load every %FIELD_NAME%_%VERSION%.sql and map their contents with their versions.
 *
 * Similar to {@link org.eclipse.osgi.util.NLS}, OSGi localization classes.
 *
 * @author levsha_aa, galiev_mr
 */
public final class JdbcQueries {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcQueries.class);

    // SONAR-OFF

    public static String QUERY_TOTAL_OBJECTS_COUNT;
    public static String QUERY_TYPES_FOR_CACHE_ALL;
    public static String QUERY_CHECK_VERSION;
    public static String QUERY_CHECK_LAST_SYS_OID;
    public static String QUERY_CHECK_TIMESTAMPS;
    public static String QUERY_CHECK_USER_PRIVILEGES;

    public static final JdbcQuery QUERY_EXTENSIONS = new JdbcQuery();
    public static final JdbcQuery QUERY_FOREIGN_DATA_WRAPPERS = new JdbcQuery();
    public static final JdbcQuery QUERY_SERVERS = new JdbcQuery();
    public static final JdbcQuery QUERY_CASTS = new JdbcQuery();
    public static final JdbcQuery QUERY_USER_MAPPING = new JdbcQuery();
    public static final JdbcQuery QUERY_SCHEMAS = new JdbcQuery();

    public static final JdbcQuery QUERY_TABLES = new JdbcQuery();
    public static final JdbcQuery QUERY_FUNCTIONS = new JdbcQuery();
    public static final JdbcQuery QUERY_SEQUENCES = new JdbcQuery();
    public static final JdbcQuery QUERY_COLLATIONS = new JdbcQuery();
    public static final JdbcQuery QUERY_INDICES = new JdbcQuery();
    public static final JdbcQuery QUERY_CONSTRAINTS = new JdbcQuery();
    public static final JdbcQuery QUERY_TRIGGERS = new JdbcQuery();
    public static final JdbcQuery QUERY_VIEWS = new JdbcQuery();
    public static final JdbcQuery QUERY_TYPES = new JdbcQuery();
    public static final JdbcQuery QUERY_RULES = new JdbcQuery();
    public static final JdbcQuery QUERY_POLICIES = new JdbcQuery();
    public static final JdbcQuery QUERY_FTS_PARSERS = new JdbcQuery();
    public static final JdbcQuery QUERY_FTS_TEMPLATES = new JdbcQuery();
    public static final JdbcQuery QUERY_FTS_DICTIONARIES = new JdbcQuery();
    public static final JdbcQuery QUERY_FTS_CONFIGURATIONS = new JdbcQuery();
    public static final JdbcQuery QUERY_OPERATORS = new JdbcQuery();

    public static String QUERY_SYSTEM_FUNCTIONS;
    public static String QUERY_SYSTEM_RELATIONS;
    public static String QUERY_SYSTEM_OPERATORS;
    public static String QUERY_SYSTEM_CASTS;

    public static final JdbcQuery QUERY_MS_SCHEMAS = new JdbcQuery();

    public static final JdbcQuery QUERY_MS_TABLES = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_FUNCTIONS_PROCEDURES_VIEWS_TRIGGERS = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_EXTENDED_FUNCTIONS_AND_PROCEDURES = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_SEQUENCES = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_INDICES_AND_PK = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_FK = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_CHECK_CONSTRAINTS = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_ASSEMBLIES = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_ROLES = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_TYPES = new JdbcQuery();
    public static final JdbcQuery QUERY_MS_USERS = new JdbcQuery();

    // SONAR-ON

    static {
        for (Field f : JdbcQueries.class.getFields()) {
            if (!f.getName().startsWith("QUERY")) {
                continue;
            }
            try {
                if (JdbcQuery.class.isAssignableFrom(f.getType())) {
                    fillQueries(f);
                } else if (String.class.isAssignableFrom(f.getType())) {
                    String res = f.getName().startsWith("QUERY_SYSTEM") ? "system/" + f.getName() : f.getName();
                    f.set(null, readResource(res));
                }
            } catch (Exception ex) {
                LOG.error("Error while loading JDBC SQL Queries resource: " + f.getName(), ex);
            }
        }
    }

    private static void fillQueries (Field f) throws Exception {
        JdbcQuery query = (JdbcQuery) f.get(null);

        if (f.getName().startsWith("QUERY_MS")) {
            query.setQuery(readResource("ms/" + f.getName()));
            return;
        }

        query.setQuery(readResource(f.getName()));

        for (SupportedVersion version : SupportedVersion.values()) {
            String sinceQuery = readResource(f.getName() + '_' + version);
            if (sinceQuery != null) {
                query.addSinceQuery(version, sinceQuery);
            }

            for (SupportedVersion v2 : SupportedVersion.values()) {
                String intervalQuery = readResource(f.getName() + '_'
                        + version.getVersion() + '_' + v2.getVersion());
                if (intervalQuery != null) {
                    query.addIntervalQuery(version, v2, intervalQuery);
                }
            }
        }
    }

    private static String readResource(String name) throws IOException {
        return FileUtils.readResource(JdbcQueries.class, name + ".sql");
    }

    private JdbcQueries() {
    }
}