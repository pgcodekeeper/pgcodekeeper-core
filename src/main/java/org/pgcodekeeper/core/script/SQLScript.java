/*******************************************************************************
 * Copyright 2017-2025 TAXTELECOM, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.pgcodekeeper.core.script;

import org.pgcodekeeper.core.settings.ISettings;

import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL script builder for database migration operations.
 * Collects and organizes SQL statements by execution phases (PRE, BEGIN, MID, END, POST)
 * and generates properly formatted SQL scripts with database-specific separators.
 */
public final class SQLScript {

    private final ISettings settings;
    private static final String PG_SEPARATOR = ";";
    private static final String MS_SEPARATOR = "\nGO";

    private static final String DELIMITER = "\n\n";

    private final Map<SQLActionType, Set<String>> statements = new EnumMap<>(SQLActionType.class);

    private int count;

    /**
     * Creates a new SQL script builder with specified settings.
     *
     * @param settings the settings containing database type and formatting options
     */
    public SQLScript(ISettings settings) {
        this.settings = settings;
    }

    /**
     * Adds a comment statement to the script.
     * Comments are placed either in POST phase (if commentsToEnd is enabled) or MID phase.
     *
     * @param comment the comment text to add
     */
    public void addCommentStatement(String comment) {
        addStatement(comment, settings.isCommentsToEnd() ? SQLActionType.POST : SQLActionType.MID);
    }

    /**
     * Adds a statement from StringBuilder to the MID phase.
     *
     * @param sb the StringBuilder containing the SQL statement
     */
    public void addStatement(StringBuilder sb) {
        addStatement(sb.toString());
    }

    /**
     * Adds a SQL statement to the MID phase with separator.
     *
     * @param sql the SQL statement to add
     */
    public void addStatement(String sql) {
        addStatement(sql, SQLActionType.MID);
    }

    /**
     * Adds a SQL statement to specified execution phase with separator.
     *
     * @param sql        the SQL statement to add
     * @param actionType the execution phase for this statement
     */
    public void addStatement(String sql, SQLActionType actionType) {
        addStatement(sql, actionType, true);
    }

    /**
     * Adds a SQL statement to the MID phase without separator.
     *
     * @param sql the SQL statement to add
     */
    public void addStatementWithoutSeparator(String sql) {
        addStatementWithoutSeparator(sql, SQLActionType.MID);
    }

    /**
     * Adds a SQL statement to specified execution phase without separator.
     *
     * @param sql        the SQL statement to add
     * @param actionType the execution phase for this statement
     */
    public void addStatementWithoutSeparator(String sql, SQLActionType actionType) {
        addStatement(sql, actionType, false);
    }

    /**
     * Gets SQL statement with appropriate database-specific separator if needed.
     * Uses ';' for PostgreSQL and ClickHouse, 'GO' for Microsoft SQL.
     *
     * @param sql           the SQL statement
     * @param needSeparator whether separator should be added
     * @return SQL statement with separator if needed
     */
    public String getSQLWithSeparator(String sql, boolean needSeparator) {
        if (!needSeparator || sql.endsWith("*/") || sql.startsWith("--")) {
            return sql;
        }

        String separator = switch (settings.getDbType()) {
            case PG, CH -> PG_SEPARATOR;
            case MS -> MS_SEPARATOR;
        };

        return sql + separator;
    }

    /**
     * Adds a SQL statement to specified execution phase with optional separator.
     *
     * @param sql           the SQL statement to add
     * @param actionType    the execution phase for this statement
     * @param needSeparator whether to add database-specific separator
     */
    public void addStatement(String sql, SQLActionType actionType, boolean needSeparator) {
        statements.computeIfAbsent(actionType, e -> new LinkedHashSet<>()).add(getSQLWithSeparator(sql, needSeparator));
        count++;
    }

    /**
     * Merges all statements from another SQL script into this one.
     * Preserves execution phases and adds statements without additional separators.
     *
     * @param script the script to merge statements from
     */
    public void addAllStatements(SQLScript script) {
        for (var type : SQLActionType.values()) {
            Set<String> s = script.statements.get(type);
            if (null == s) {
                continue;
            }
            s.forEach(e -> addStatement(e, type, false));
        }
    }

    /**
     * Generates the complete SQL script with all statements in execution order.
     * Statements are ordered by phases: PRE, BEGIN, MID, END, POST.
     *
     * @return complete SQL script as string with double newline delimiters
     */
    public String getFullScript() {
        return statements.entrySet().stream()
                .flatMap(t -> t.getValue().stream())
                .collect(Collectors.joining(DELIMITER));
    }

    /**
     * Returns the total number of statements in this script.
     *
     * @return total statement count across all phases
     */
    public int getSize() {
        return count;
    }

    /**
     * Checks if this script contains any statements.
     *
     * @return true if script has no statements, false otherwise
     */
    public boolean isEmpty() {
        return 0 == count;
    }

    /**
     * Returns the settings used by this script.
     *
     * @return the settings instance
     */
    public ISettings getSettings() {
        return settings;
    }
}
