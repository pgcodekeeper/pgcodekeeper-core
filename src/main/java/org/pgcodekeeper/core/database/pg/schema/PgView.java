/*******************************************************************************
 * Copyright 2017-2026 TAXTELECOM, LLC
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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.database.pg.schema;

import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.base.schema.AbstractView;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * PostgreSQL view implementation.
 * Views are virtual tables defined by queries that provide a way to present data
 * from one or more tables in a customized format without storing the data physically.
 */
public final class PgView extends PgAbstractView {

    private final Map<String, String> defaultValues = new LinkedHashMap<>();

    /**
     * Creates a new PostgreSQL view.
     *
     * @param name view name
     */
    public PgView(String name) {
        super(name);
    }

    @Override
    protected void appendDefaultValues(SQLScript script) {
        for (final Entry<String, String> defaultValue : defaultValues.entrySet()) {
            StringBuilder sql = new StringBuilder();
            sql.append("ALTER VIEW ");
            sql.append(getQualifiedName());
            sql.append(ALTER_COLUMN);
            sql.append(getQuotedName(defaultValue.getKey()));
            sql.append(" SET DEFAULT ");
            sql.append(defaultValue.getValue());
            script.addStatement(sql);
        }
    }

    @Override
    protected void appendIfNotExists(StringBuilder sb, ISettings settings) {
        // noimpl
    }

    @Override
    protected void alterViewOptions(SQLScript script, PgAbstractView newAbstractView) {
        PgView newView = (PgView) newAbstractView;
        alterDefaultValues(newView, script);
    }

    @Override
    protected void appendOptions(StringBuilder sbSQL) {
        super.appendOptions(sbSQL);
        sbSQL.append(" AS\n\t");
        sbSQL.append(query);
        if (options.containsKey(CHECK_OPTION)) {
            String chekOption = options.get(CHECK_OPTION);
            sbSQL.append("\nWITH ");
            if (chekOption != null) {
                sbSQL.append(chekOption.toUpperCase(Locale.ROOT));
            }
            sbSQL.append(" CHECK OPTION");
        }
    }

    /**
     * Compares default values with values in new view.
     *
     * @param script  for collect sql statements
     * @param newView new view
     */
    private void alterDefaultValues(final PgView newView, SQLScript script) {
        for (final Entry<String, String> columnComment : newView.defaultValues.entrySet()) {
            String newColumn = columnComment.getKey();
            String newValue = columnComment.getValue();

            String oldValue = defaultValues.get(newColumn);
            if (!Objects.equals(oldValue, newValue)) {
                script.addStatement(addAlterTable(newColumn, " SET").append(' ').append(newValue));
            }
        }

        for (final Entry<String, String> columnComment : defaultValues.entrySet()) {
            String oldColumn = columnComment.getKey();

            if (!newView.defaultValues.containsKey(oldColumn)) {
                script.addStatement(addAlterTable(oldColumn, " DROP"));
            }
        }
    }

    private StringBuilder addAlterTable(String column, String state) {
        return new StringBuilder(ALTER_TABLE).append(getQualifiedName())
                .append(ALTER_COLUMN)
                .append(getQuotedName(column))
                .append(state).append(" DEFAULT");
    }

    /**
     * Adds/replaces column default value specification.
     *
     * @param columnName   column name
     * @param defaultValue default value expression
     */
    public void addColumnDefaultValue(final String columnName, final String defaultValue) {
        defaultValues.put(columnName, defaultValue);
        resetHash();
    }

    /**
     * Removes column default value specification.
     *
     * @param columnName column name to remove default from
     */
    public void removeColumnDefaultValue(final String columnName) {
        defaultValues.remove(columnName);
        resetHash();
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj instanceof PgView view && super.compare(obj)) {
            return Objects.equals(defaultValues, view.defaultValues);
        }

        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(defaultValues);
    }

    @Override
    protected AbstractView getViewCopy() {
        PgView view = new PgView(name);
        view.defaultValues.putAll(defaultValues);
        return view;
    }

}
