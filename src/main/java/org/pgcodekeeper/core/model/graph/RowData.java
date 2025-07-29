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
package org.pgcodekeeper.core.model.graph;

import org.pgcodekeeper.core.DatabaseType;

import java.util.*;

/**
 * Stores database row data for INSERT script generation.
 * Manages row information including table name, column data, foreign key relationships,
 * and handles cyclic dependency resolution through null value replacements.
 *
 * @author shamsutdinov_er
 */
public final class RowData {

    private static final String DELIMITER_AND = " AND ";

    /**
     * name of table which data need to transfer in the database recipient
     */
    private final String tableName;
    private final RowData parent;
    /**
     * store row data: column name = value
     */
    private final Map<String, String> data;

    /*
     * store column names from foreign keys which have option NOT NULL
     */
    private final List<String> fkCols;
    private final Collection<String> idCols;
    private final Set<String> replacements = new HashSet<>();

    /**
     * Creates a new row data instance.
     *
     * @param tableName qualified name of the table
     * @param parent    parent row data for dependency tracking
     * @param data      map of column names to values
     * @param fkCols    list of foreign key column names with NOT NULL constraint
     * @param idCols    collection of identity/primary key column names
     */
    public RowData(String tableName, RowData parent, Map<String, String> data, List<String> fkCols,
                   Collection<String> idCols) {
        this.tableName = tableName;
        this.parent = parent;
        this.data = data;
        this.fkCols = fkCols;
        this.idCols = idCols;
    }

    public String getTableName() {
        return tableName;
    }

    public RowData getParent() {
        return parent;
    }

    /**
     * Checks if this row has foreign key constraints with NOT NULL columns.
     *
     * @return true if has foreign key constraints, false otherwise
     */
    public boolean hasFk() {
        return !fkCols.isEmpty();
    }

    /**
     * Checks if this row contains all specified foreign key columns.
     *
     * @param columns collection of column names to check
     * @return true if all columns are present in foreign key columns
     */
    public boolean containsAllFkCols(Collection<String> columns) {
        return new HashSet<>(fkCols).containsAll(columns);
    }

    /**
     * Adds a column for null replacement to resolve cyclic dependencies.
     * Only adds columns that have non-null values in the original data.
     *
     * @param colName the column name to replace with null
     */
    public void addReplacement(String colName) {
        // add only columns with not null values
        if (data.containsKey(colName)) {
            replacements.add(colName);
        }
    }

    /**
     * Generates WHERE clause filter for foreign key relationship.
     *
     * @param foreignColumns array of foreign table column names
     * @param columns        array of local table column names
     * @return generated filter string, or null if any column value is null
     */
    public String generateFilter(Object[] foreignColumns, Object[] columns) {
        if (columns.length == 1 && data.get(columns[0]) == null) {
            return null;
        }

        var sb = new StringBuilder();
        for (int i = 0; columns.length > i; i++) {
            if (i != 0) {
                sb.append(DELIMITER_AND);
            }
            sb.append(foreignColumns[i]).append(" = ").append(data.get(columns[i]));
        }
        return sb.toString();
    }

    /**
     * Appends INSERT statement to the script builder.
     * Handles database-specific syntax for PostgreSQL and Microsoft SQL.
     *
     * @param dbType       the database type
     * @param haveIdentity whether table has identity columns
     * @param sb           the script builder to append to
     */
    public void appendInsert(DatabaseType dbType, boolean haveIdentity, StringBuilder sb) {
        switch (dbType) {
            case PG:
                appendPgInsert(haveIdentity, sb);
                break;
            case MS:
                appendMsInsert(haveIdentity, sb);
                break;
            default:
                break;
        }
    }

    private void appendPgInsert(boolean haveIdentity, StringBuilder sb) {
        appendCommonInsertPrefix(sb);
        if (haveIdentity) {
            sb.append("\nOVERRIDING SYSTEM VALUE");
        }
        sb.append("\nVALUES (");
        appendInsertData(sb);
        sb.append(")\nON CONFLICT DO NOTHING;\n\n");
    }

    private void appendMsInsert(boolean haveIdentity, StringBuilder sb) {
        if (haveIdentity) {
            sb.append("SET IDENTITY_INSERT ").append(tableName).append(" ON;\nGO\n\n");
        }
        appendCommonInsertPrefix(sb);
        sb.append("\nSELECT ");
        appendInsertData(sb);
        sb.append("\nWHERE NOT EXISTS (SELECT 1 FROM ").append(tableName).append(" WHERE (");
        appendColData(idCols, DELIMITER_AND, sb);
        sb.append("));\nGO\n\n");
        if (haveIdentity) {
            sb.append("SET IDENTITY_INSERT ").append(tableName).append(" OFF;\nGO\n\n");
        }
    }

    private void appendCommonInsertPrefix(StringBuilder sb) {
        sb.append("-- table: ").append(tableName).append(", filter: ");
        appendColData(idCols, DELIMITER_AND, sb);
        sb.append("\nINSERT INTO ").append(tableName).append(" (");
        for (var cols : data.keySet()) {
            sb.append(cols).append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(')');
    }

    private void appendInsertData(StringBuilder sb) {
        for (var columnData : data.entrySet()) {
            sb.append(replacements.contains(columnData.getKey()) ? "NULL" : columnData.getValue()).append(", ");
        }
        sb.setLength(sb.length() - 2);
    }

    /**
     * Appends UPDATE statement to fix cyclic dependency null replacements.
     * Only generates UPDATE if there are replacement columns.
     *
     * @param dbType the database type
     * @param sb     the script builder to append to
     */
    public void appendUpdate(DatabaseType dbType, StringBuilder sb) {
        if (replacements.isEmpty()) {
            return;
        }

        sb.append("UPDATE ").append(tableName).append(" SET ");
        appendColData(replacements, ", ", sb);

        sb.append(" WHERE ");
        appendColData(idCols, DELIMITER_AND, sb);

        sb.append(";\n").append(getPostfix(dbType));
    }

    private void appendColData(Collection<String> cols, String delimiter, StringBuilder sb) {
        if (cols.isEmpty()) {
            return;
        }

        for (var col : cols) {
            sb.append(col).append(" = ").append(data.get(col)).append(delimiter);
        }
        sb.setLength(sb.length() - delimiter.length());
    }

    private String getPostfix(DatabaseType dbType) {
        return dbType == DatabaseType.MS ? "GO\n\n" : "\n";
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append(tableName).append(' ');
        appendColData(idCols, DELIMITER_AND, sb);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, data);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RowData rd)) {
            return false;
        }
        return Objects.equals(tableName, rd.tableName)
                && Objects.equals(data, rd.data);
    }
}