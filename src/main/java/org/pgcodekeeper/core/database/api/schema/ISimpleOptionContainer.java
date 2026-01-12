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
 *******************************************************************************/
package org.pgcodekeeper.core.database.api.schema;

import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Map;

/**
 * Interface for database objects that support simple option management with ALTER statements.
 * Provides default implementation for comparing and updating options using SET and RESET syntax.
 */
public interface ISimpleOptionContainer extends IOptionContainer {

    @Override
    default void compareOptions(IOptionContainer newContainer, SQLScript script) {
        Map<String, String> oldOptions = getOptions();
        Map<String, String> newOptions = newContainer.getOptions();

        StringBuilder setOptions = new StringBuilder();
        StringBuilder resetOptions = new StringBuilder();

        if (!oldOptions.isEmpty() || !newOptions.isEmpty()) {
            oldOptions.forEach((key, value) -> {
                String newValue = newOptions.get(key);
                if (newValue != null) {
                    if (!value.equals(newValue)) {
                        setOptions.append(key);
                        if (!newValue.isEmpty()) {
                            setOptions.append('=');
                            setOptions.append(newValue);
                        }
                        setOptions.append(", ");
                    }
                } else {
                    resetOptions.append(key).append(", ");
                }
            });

            newOptions.forEach((key, value) -> {
                if (!oldOptions.containsKey(key)) {
                    setOptions.append(key);
                    if (!value.isEmpty()) {
                        setOptions.append('=');
                        setOptions.append(value);
                    }
                    setOptions.append(", ");
                }
            });
        }

        if (!setOptions.isEmpty() || !resetOptions.isEmpty()) {
            appendOptions(newContainer, setOptions, resetOptions, script);
        }
    }

    /**
     * Appends SQL options to set and reset option builders for ALTER statements.
     *
     * @param newContainer the new option container to compare against
     * @param setOptions   builder for SET option clauses
     * @param resetOptions builder for RESET option clauses
     * @param script       the SQL script context for generating statements
     */
    default void appendOptions(IOptionContainer newContainer, StringBuilder setOptions,
                               StringBuilder resetOptions, SQLScript script) {
        DbObjType type = getStatementType();
        String typeName = type == DbObjType.VIEW ? ((AbstractStatement) newContainer).getTypeName() : type.name();
        getAlterOptionAction(setOptions, " SET (", script, type, typeName);
        getAlterOptionAction(resetOptions, " RESET (", script, type, typeName);
    }

    private void getAlterOptionAction(StringBuilder option, String action, SQLScript script, DbObjType type,
                                      String typeName) {
        if (option.isEmpty()) {
            return;
        }
        StringBuilder sql = new StringBuilder();
        option.setLength(option.length() - 2);
        sql.append("ALTER ");
        if (type == DbObjType.COLUMN) {
            sql.append("TABLE ONLY ")
                    .append(PgDiffUtils.getQuotedName(getParent().getParent().getName()))
                    .append('.').append(PgDiffUtils.getQuotedName(getParent().getName()))
                    .append(" ALTER ");
        }
        sql.append(typeName).append(' ');
        if (type != DbObjType.COLUMN) {
            IStatement parent = getParent();
            if (type == DbObjType.INDEX) {
                parent = parent.getParent();
            }
            sql.append(PgDiffUtils.getQuotedName(parent.getName())).append('.');
        }
        sql.append(PgDiffUtils.getQuotedName(getName())).append(action).append(option).append(")");
        script.addStatement(sql);
    }
}
