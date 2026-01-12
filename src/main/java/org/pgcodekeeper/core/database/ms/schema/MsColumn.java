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
package org.pgcodekeeper.core.database.ms.schema;

import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.script.SQLActionType;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a Microsoft SQL table column with its properties and constraints.
 * Handles column-specific features like SPARSE, ROWGUIDCOL, PERSISTED, identity columns,
 * generated columns, and data masking functions.
 */
public final class MsColumn extends AbstractColumn implements IMsStatement {

    private static final String SPARSE = "SPARSE";
    private static final String ROWGUIDCOL = "ROWGUIDCOL";
    private static final String PERSISTED = "PERSISTED";

    private boolean isSparse;
    private boolean isRowGuidCol;
    private boolean isPersisted;
    private boolean isNotForRep;
    private boolean isIdentity;
    private boolean isHidden;
    private String seed;
    private String increment;
    private String defaultName;
    private String expression;
    private String maskingFunction;
    private MsGeneratedType generated;

    /**
     * Creates a new Microsoft SQL column with the specified name.
     *
     * @param name the column name
     */
    public MsColumn(String name) {
        super(name);
    }

    @Override
    public String getFullDefinition() {
        final StringBuilder sbDefinition = new StringBuilder();
        sbDefinition.append(getQuotedName(name));
        sbDefinition.append(' ');
        if (expression != null) {
            sbDefinition.append("AS ").append(expression);
        } else {
            sbDefinition.append(type);
        }

        if (collation != null) {
            sbDefinition.append(COLLATE).append(collation);
        }

        if (isSparse) {
            sbDefinition.append(" SPARSE");
        }

        if (isRowGuidCol) {
            sbDefinition.append(" ROWGUIDCOL");
        }

        if (isPersisted) {
            sbDefinition.append(" PERSISTED");
        }

        if (maskingFunction != null) {
            sbDefinition.append(" MASKED WITH (FUNCTION = ").append(maskingFunction).append(")");
        }

        if (generated != null) {
            appendGenerated(sbDefinition);
        }

        if (expression == null) {
            sbDefinition.append(notNull ? NOT_NULL : NULL);
        }

        if (isIdentity) {
            sbDefinition.append(" IDENTITY (").append(seed).append(',').append(increment).append(")");
            if (isNotForRep) {
                sbDefinition.append(" NOT FOR REPLICATION");
            }
        }

        if (defaultValue != null) {
            if (defaultName != null) {
                sbDefinition.append(" CONSTRAINT ");
                sbDefinition.append(getQuotedName(defaultName));
            }
            sbDefinition.append(" DEFAULT ");
            sbDefinition.append(defaultValue);
        }

        return sbDefinition.toString();
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        StringBuilder sql = new StringBuilder();
        sql.append(getAlterTable(false));
        sql.append("\n\tADD ").append(getQuotedName(name)).append(' ');
        if (expression != null) {
            sql.append("AS ").append(expression);
        } else {
            sql.append(type);
        }

        if (collation != null) {
            sql.append(COLLATE).append(collation);
        }

        if (isIdentity) {
            sql.append(" IDENTITY (").append(seed).append(',').append(increment).append(")");
            if (isNotForRep) {
                sql.append(" NOT FOR REPLICATION");
            }
        }

        if (maskingFunction != null) {
            sql.append(" MASKED WITH (FUNCTION = ").append(maskingFunction).append(")");
        }

        if (generated != null) {
            appendGenerated(sql);
        }

        boolean isJoinNotNull = expression == null && defaultValue == null && notNull;
        if (isJoinNotNull) {
            sql.append(NOT_NULL);
        }

        script.addStatement(sql);

        compareDefaults(null, null, defaultName, defaultValue, script);

        if (!isJoinNotNull && expression == null && notNull) {
            if (defaultValue != null) {
                addUpdateStatement(script);
            }

            StringBuilder sqlAlter = new StringBuilder();
            sqlAlter.append(getAlterColumn(name)).append(' ').append(type);

            if (collation != null) {
                sqlAlter.append(COLLATE).append(collation);
            }

            sqlAlter.append(NOT_NULL);
            script.addStatement(sqlAlter);
        }

        compareOption(false, isSparse, SPARSE, script);
        compareOption(false, isRowGuidCol, ROWGUIDCOL, script);
        compareOption(false, isPersisted, PERSISTED, script);

        appendPrivileges(script);
    }

    private void compareOption(boolean oldOption, boolean newOption, String optionName, SQLScript script) {
        compareOption(oldOption, newOption, optionName, null, script);
    }

    private void compareOption(boolean oldOption, boolean newOption, String optionName,
                               AtomicBoolean isNeedDepcies, SQLScript script) {
        if (oldOption == newOption) {
            return;
        }

        /*
         * we can set PERSISTED without drop dependencies, but can't simple drop this option
         * for first we have to drop dependencies
         */
        if (isNeedDepcies != null && (oldOption || !PERSISTED.equalsIgnoreCase(optionName))) {
            isNeedDepcies.set(true);
        }
        String sb = getAlterColumn(name) +
                (newOption ? " ADD " : " DROP ") +
                optionName;

        // before adding the ROWGUIDCOL option to a column, we must first remove it from another
        var orderType = !newOption && ROWGUIDCOL.equalsIgnoreCase(optionName) ? SQLActionType.BEGIN : SQLActionType.MID;
        script.addStatement(sb, orderType);
    }

    private void appendGenerated(StringBuilder sb) {
        sb.append(" GENERATED ALWAYS AS ").append(generated.getValue());
        if (isHidden) {
            sb.append(" HIDDEN");
        }
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        MsColumn newColumn = (MsColumn) newCondition;

        // recreate column to change identity or computed value
        if (!Objects.equals(newColumn.seed, seed)
                || !Objects.equals(newColumn.increment, increment)
                || !Objects.equals(newColumn.expression, expression)
                || newColumn.generated != generated
                || !Objects.equals(newColumn.isHidden, isHidden)) {
            return ObjectState.RECREATE;
        }

        boolean isNeedDropDefault = !Objects.equals(type, newColumn.type)
                && (!Objects.equals(defaultValue, newColumn.defaultValue)
                || !Objects.equals(defaultName, newColumn.defaultName));

        if (isNeedDropDefault) {
            compareDefaults(defaultName, defaultValue, null, null, script);
        }
        AtomicBoolean isNeedDepcies = new AtomicBoolean();
        compareTypes(newColumn, isNeedDepcies, script);

        String oldDefaultName = isNeedDropDefault ? null : defaultName;
        String oldDefault = isNeedDropDefault ? null : defaultValue;
        compareDefaults(oldDefaultName, oldDefault, newColumn.defaultName,
                newColumn.defaultValue, script);

        compareNullValues(newColumn, isNeedDepcies, script);
        compareMaskingFunctions(newColumn, script);

        compareOption(isNotForRep, newColumn.isNotForRep, "NOT FOR REPLICATION", script);
        compareOption(isSparse, newColumn.isSparse, SPARSE, isNeedDepcies, script);
        compareOption(isRowGuidCol, newColumn.isRowGuidCol, ROWGUIDCOL, script);
        compareOption(isPersisted, newColumn.isPersisted, PERSISTED, isNeedDepcies, script);

        alterPrivileges(newColumn, script);
        return getObjectState(isNeedDepcies.get(), script, startSize);
    }

    private void compareDefaults(String oldDefaultName, String oldDefault,
                                 String newDefaultName, String newDefault, SQLScript script) {
        if (Objects.equals(oldDefault, newDefault) && Objects.equals(oldDefaultName, newDefaultName)) {
            return;
        }

        if (oldDefault != null) {
            script.addStatement(getAlterTable(false) + "\n\tDROP CONSTRAINT " + getQuotedName(oldDefaultName));
        }

        if (newDefault != null) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterTable(false));
            sql.append("\n\tADD");
            if (newDefaultName != null) {
                sql.append(" CONSTRAINT ").append(getQuotedName(newDefaultName));
            }
            sql.append(" DEFAULT ").append(newDefault);
            sql.append(" FOR ").append(getQuotedName(name));
            script.addStatement(sql);
        }
    }

    private void compareTypes(MsColumn newColumn, AtomicBoolean isNeedDepcies, SQLScript script) {
        String newCollation = newColumn.collation;
        if (Objects.equals(type, newColumn.type) && Objects.equals(newCollation, collation)) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(getAlterColumn(newColumn.name)).append(' ').append(newColumn.type);
        if (newCollation != null) {
            sb.append(COLLATE).append(newCollation);
        }
        if (notNull == newColumn.notNull) {
            sb.append(newColumn.notNull ? NOT_NULL : NULL);
        }
        script.addStatement(sb);
        isNeedDepcies.set(true);
    }

    private void compareNullValues(MsColumn newColumn, AtomicBoolean isNeedDepcies, SQLScript script) {
        if (newColumn.notNull == notNull) {
            return;
        }

        if (newColumn.defaultValue != null && !notNull) {
            addUpdateStatement(script);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(getAlterColumn(newColumn.name)).append(' ').append(newColumn.type);
        if (newColumn.collation != null) {
            sb.append(COLLATE).append(newColumn.collation);
        }
        sb.append(newColumn.notNull ? NOT_NULL : NULL);
        script.addStatement(sb);
        isNeedDepcies.set(true);
    }

    private void compareMaskingFunctions(MsColumn newColumn, SQLScript script) {
        if (!Objects.equals(newColumn.maskingFunction, maskingFunction)) {
            StringBuilder sb = new StringBuilder();
            sb.append(getAlterColumn(newColumn.name));
            if (newColumn.maskingFunction != null) {
                sb.append(" ADD MASKED WITH (FUNCTION = ").append(newColumn.maskingFunction).append(")");
            } else {
                sb.append(" DROP MASKED");
            }
            script.addStatement(sb);
        }
    }

    private void addUpdateStatement(SQLScript script) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(parent.getQualifiedName())
                .append("\n\tSET ").append(getQuotedName(name))
                .append(" = DEFAULT WHERE ")
                .append(getQuotedName(name)).append(" IS").append(NULL);
        script.addStatement(sb);
    }

    private String getAlterColumn(String column) {
        return ((AbstractTable) parent).getAlterTable(false) + ALTER_COLUMN + getQuotedName(column);
    }

    @Override
    public void getDropSQL(SQLScript script, boolean optionExists) {
        final StringBuilder sb = new StringBuilder();
        // we need to drop default
        compareDefaults(defaultName, defaultValue, null, null, script);
        sb.append(getAlterTable(false)).append("\n\tDROP COLUMN ");
        if (optionExists) {
            sb.append(IF_EXISTS);
        }
        sb.append(getQuotedName(name));
        script.addStatement(sb);
    }

    public void setSparse(final boolean isSparse) {
        this.isSparse = isSparse;
        resetHash();
    }

    public void setRowGuidCol(final boolean isRowGuidCol) {
        this.isRowGuidCol = isRowGuidCol;
        resetHash();
    }

    public void setPersisted(final boolean isPersisted) {
        this.isPersisted = isPersisted;
        resetHash();
    }

    public void setNotForRep(final boolean isNotForRep) {
        this.isNotForRep = isNotForRep;
        resetHash();
    }

    public String getDefaultName() {
        return defaultName;
    }

    public void setDefaultName(final String defaultName) {
        this.defaultName = defaultName;
        resetHash();
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(final String expression) {
        this.expression = expression;
        resetHash();
    }

    public void setMaskingFunction(final String maskingFunction) {
        this.maskingFunction = maskingFunction;
        resetHash();
    }

    public boolean isIdentity() {
        return isIdentity;
    }

    /**
     * Configures this column as an identity column with the specified seed and increment values.
     *
     * @param seed      the starting value for the identity
     * @param increment the increment value for each new identity value
     */
    public void setIdentity(String seed, String increment) {
        this.seed = seed;
        this.increment = increment;
        this.isIdentity = true;
        resetHash();
    }

    public MsGeneratedType getGenerated() {
        return generated;
    }

    public void setGenerated(MsGeneratedType generated) {
        this.generated = generated;
        resetHash();
    }

    public void setHidden(boolean isHidden) {
        this.isHidden = isHidden;
        resetHash();
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj instanceof MsColumn col && super.compare(obj)) {
            return isSparse == col.isSparse
                    && isRowGuidCol == col.isRowGuidCol
                    && isPersisted == col.isPersisted
                    && isNotForRep == col.isNotForRep
                    && isIdentity == col.isIdentity
                    && isHidden == col.isHidden
                    && Objects.equals(seed, col.seed)
                    && Objects.equals(increment, col.increment)
                    && Objects.equals(defaultName, col.defaultName)
                    && Objects.equals(expression, col.expression)
                    && Objects.equals(maskingFunction, col.maskingFunction)
                    && generated == col.generated;
        }

        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(isSparse);
        hasher.put(isRowGuidCol);
        hasher.put(isPersisted);
        hasher.put(isNotForRep);
        hasher.put(isIdentity);
        hasher.put(isHidden);
        hasher.put(seed);
        hasher.put(increment);
        hasher.put(defaultName);
        hasher.put(expression);
        hasher.put(maskingFunction);
        hasher.put(generated);
    }

    @Override
    protected AbstractColumn getColumnCopy() {
        MsColumn copy = new MsColumn(name);
        copy.setSparse(isSparse);
        copy.setRowGuidCol(isRowGuidCol);
        copy.setPersisted(isPersisted);
        copy.setNotForRep(isNotForRep);
        copy.isIdentity = isIdentity;
        copy.isHidden = isHidden;
        copy.seed = seed;
        copy.increment = increment;
        copy.setDefaultName(defaultName);
        copy.setExpression(expression);
        copy.setMaskingFunction(maskingFunction);
        copy.setGenerated(generated);
        return copy;
    }
}
