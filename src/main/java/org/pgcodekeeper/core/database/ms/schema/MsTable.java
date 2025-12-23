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
package org.pgcodekeeper.core.database.ms.schema;

import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.MsDiffUtils;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.database.api.schema.ISimpleOptionContainer;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.script.SQLActionType;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.*;
import java.util.Map.Entry;

/**
 * Represents a Microsoft SQL table with support for memory-optimized tables,
 * temporal tables, filestream data, and other Microsoft SQL specific features.
 */
public final class MsTable extends AbstractTable implements ISimpleOptionContainer, IMsStatement {

    private static final String MEMORY_OPTIMIZED = "MEMORY_OPTIMIZED";

    /**
     * list of internal primary keys for memory optimized table
     */
    private List<AbstractConstraint> pkeys;

    private boolean ansiNulls;
    private Boolean isTracked;

    private String textImage;
    private String fileStream;
    private String tablespace;
    private String sysVersioning;

    private AbstractColumn periodStartCol;
    private AbstractColumn periodEndCol;

    private final Map<String, MsStatistics> statistics = new HashMap<>();

    /**
     * Creates a new Microsoft SQL table with the specified name.
     *
     * @param name the table name
     */
    public MsTable(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        appendName(sbSQL);
        appendColumns(sbSQL);
        appendOptions(sbSQL);
        script.addStatement(sbSQL);
        appendAlterOptions(script);
        appendOwnerSQL(script);
        appendPrivileges(script);
        appendColumnsPriliges(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        MsTable newTable = (MsTable) newCondition;

        if (isRecreated(newTable, script.getSettings())) {
            return ObjectState.RECREATE;
        }

        appendAlterOwner(newTable, script);
        compareTableOptions(newTable, script);
        alterPrivileges(newTable, script);

        return getObjectState(script, startSize);
    }

    private void appendAlterOptions(SQLScript script) {
        if (isTracked != null) {
            script.addStatement(enableTracking(), SQLActionType.END);
        }
        if (sysVersioning != null) {
            script.addStatement(enableSysVersioning(), SQLActionType.END);
        }
    }

    private void appendName(StringBuilder sbSQL) {
        sbSQL.append("SET QUOTED_IDENTIFIER ON").append(GO).append('\n');
        sbSQL.append("SET ANSI_NULLS ").append(ansiNulls ? "ON" : "OFF");
        sbSQL.append(GO).append('\n');
        sbSQL.append("CREATE TABLE ").append(getQualifiedName());
    }

    private void appendColumns(StringBuilder sbSQL) {
        sbSQL.append("(\n");

        for (AbstractColumn column : columns) {
            sbSQL.append("\t");
            sbSQL.append(column.getFullDefinition());
            sbSQL.append(",\n");
        }

        for (AbstractConstraint con : getPkeys()) {
            if (con.isPrimaryKey()) {
                sbSQL.append("\t");
                String name = con.getName();
                if (!name.isEmpty()) {
                    sbSQL.append("CONSTRAINT ").append(MsDiffUtils.quoteName(name)).append(' ');
                }
                sbSQL.append(con.getDefinition());
                sbSQL.append(",\n");
            }
        }
        sbSQL.setLength(sbSQL.length() - 2);
        appendPeriodSystem(sbSQL);
        sbSQL.append('\n').append(')');
    }

    private void appendPeriodSystem(StringBuilder sb) {
        if (periodStartCol != null && periodEndCol != null) {
            sb.append(",\n\tPERIOD FOR SYSTEM_TIME (");
            sb.append(MsDiffUtils.quoteName(periodStartCol.getName())).append(", ");
            sb.append(MsDiffUtils.quoteName(periodEndCol.getName()));
            sb.append(")");
        }
    }

    /**
     * Gets the list of primary key constraints for memory-optimized tables.
     *
     * @return unmodifiable list of primary key constraints
     */
    public List<AbstractConstraint> getPkeys() {
        return pkeys == null ? Collections.emptyList() : Collections.unmodifiableList(pkeys);
    }

    @Override
    public void addConstraint(AbstractConstraint constraint) {
        if (constraint.isPrimaryKey() && isMemoryOptimized()) {
            if (pkeys == null) {
                pkeys = new ArrayList<>();
            }
            pkeys.add(constraint);
        } else {
            super.addConstraint(constraint);
        }
    }

    private void appendOptions(StringBuilder sbSQL) {
        int startLength = sbSQL.length();
        if (tablespace != null) {
            // tablespace already quoted
            sbSQL.append(" ON ").append(tablespace).append(' ');
        }

        if (textImage != null) {
            sbSQL.append("TEXTIMAGE_ON ").append(MsDiffUtils.quoteName(textImage)).append(' ');
        }

        if (fileStream != null) {
            sbSQL.append("FILESTREAM_ON ").append(MsDiffUtils.quoteName(fileStream)).append(' ');
        }

        if (sbSQL.length() > startLength) {
            sbSQL.setLength(sbSQL.length() - 1);
        }

        StringBuilder sb = new StringBuilder();
        for (Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            sb.append(key);
            if (!value.isEmpty()) {
                sb.append(" = ").append(value);
            }
            sb.append(", ");
        }

        if (!sb.isEmpty()) {
            sb.setLength(sb.length() - 2);
            sbSQL.append("\nWITH (").append(sb).append(')');
        }
    }

    @Override
    protected boolean isNeedRecreate(AbstractTable newTable) {
        if (newTable instanceof MsTable smt) {
            return !Objects.equals(smt.tablespace, tablespace)
                    || ansiNulls != smt.ansiNulls
                    || !Utils.setLikeEquals(smt.getPkeys(), getPkeys())
                    || !Objects.equals(smt.options, options)
                    || !Objects.equals(smt.fileStream, fileStream)
                    || !Objects.equals(smt.periodStartCol, periodStartCol)
                    || !Objects.equals(smt.periodEndCol, periodEndCol)
                    || (smt.textImage != null && textImage != null
                    && !Objects.equals(smt.textImage, textImage));
        }

        return true;
    }

    /**
     * Compares table options between this table and the new table, generating appropriate SQL scripts
     * for change tracking and system versioning differences.
     *
     * @param newTable the new table to compare against
     * @param script the script to append SQL statements to
     */
    public void compareTableOptions(MsTable newTable, SQLScript script) {
        compareTracked(newTable, script);
        compareSysVersioning(newTable, script);
    }

    private void compareSysVersioning(MsTable newTable, SQLScript script) {
        if (Objects.equals(sysVersioning, newTable.sysVersioning)) {
            if (sysVersioning != null && pkChanged(newTable)) {
                script.addStatement(disableSysVersioning(), SQLActionType.BEGIN);
                script.addStatement(newTable.enableSysVersioning(), SQLActionType.END);
            }
            return;
        }

        if (sysVersioning != null) {
            script.addStatement(disableSysVersioning(), SQLActionType.MID);
        }

        if (newTable.sysVersioning != null) {
            script.addStatement(newTable.enableSysVersioning(), SQLActionType.MID);
        }
    }

    private void compareTracked(MsTable newTable, SQLScript script) {
        if (Objects.equals(isTracked, newTable.isTracked)) {
            if (isTracked != null && pkChanged(newTable)) {
                script.addStatement(disableTracking(), SQLActionType.BEGIN);
                script.addStatement(newTable.enableTracking(), SQLActionType.END);
            }
            return;
        }

        if (isTracked != null) {
            script.addStatement(disableTracking(), SQLActionType.MID);
        }

        if (newTable.isTracked != null) {
            script.addStatement(newTable.enableTracking(), SQLActionType.MID);
        }
    }

    private boolean pkChanged(MsTable table) {
        var oldPk = getConstraints().stream().filter(MsConstraintPk.class::isInstance).findAny().orElse(null);
        var newPk = table.getConstraints().stream().filter(MsConstraintPk.class::isInstance).findAny().orElse(null);
        return oldPk != null && newPk != null && !Objects.equals(oldPk, newPk);
    }

    private String disableTracking() {
        return getAlterTable(false) + " DISABLE CHANGE_TRACKING";
    }

    private String enableTracking() {
        return getAlterTable(false) +
                " ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = " +
                (isTracked() ? "ON" : "OFF") + ')';
    }

    private String disableSysVersioning() {
        return getAlterTable(false) + " SET (SYSTEM_VERSIONING = OFF)";
    }

    private String enableSysVersioning() {
        return getAlterTable(false) + " SET (SYSTEM_VERSIONING = " + sysVersioning + ')';
    }

    @Override
    public String getAlterTable(boolean only) {
        return ALTER_TABLE + getQualifiedName();
    }

    @Override
    public void getDropSQL(SQLScript script, boolean generateExists) {
        if (isTracked() && getConstraints().stream().anyMatch(MsConstraintPk.class::isInstance)) {
            script.addStatement(disableTracking(), SQLActionType.BEGIN);
        }
        if (sysVersioning != null) {
            script.addStatement(disableSysVersioning(), SQLActionType.BEGIN);
        }

        super.getDropSQL(script, generateExists);
    }

    public void setFileStream(String fileStream) {
        this.fileStream = fileStream;
        resetHash();
    }

    public void setTextImage(String textImage) {
        this.textImage = textImage;
        resetHash();
    }

    public void setAnsiNulls(boolean ansiNulls) {
        this.ansiNulls = ansiNulls;
        resetHash();
    }

    /**
     * Checks if change tracking is enabled for this table.
     *
     * @return true if change tracking is enabled
     */
    public boolean isTracked() {
        return isTracked != null && isTracked;
    }


    public void setTracked(final Boolean isTracked) {
        this.isTracked = isTracked;
        resetHash();
    }

    public String getTablespace() {
        return tablespace;
    }

    public void setTablespace(final String tablespace) {
        this.tablespace = tablespace;
        resetHash();
    }

    public void setPeriodStartCol(AbstractColumn periodStartCol) {
        this.periodStartCol = periodStartCol;
        resetHash();
    }

    public void setPeriodEndCol(AbstractColumn periodEndCol) {
        this.periodEndCol = periodEndCol;
        resetHash();
    }

    /**
     * Checks if this table is memory-optimized.
     *
     * @return true if the table is memory-optimized
     */
    public boolean isMemoryOptimized() {
        return "ON".equalsIgnoreCase(options.get(MEMORY_OPTIMIZED));
    }

    public void setSysVersioning(String sysVersioning) {
        this.sysVersioning = sysVersioning;
        resetHash();
    }

    @Override
    protected void writeInsert(SQLScript script, AbstractTable newTable, String tblTmpQName,
                               List<String> identityColsForMovingData, String cols) {
        String tblQName = newTable.getQualifiedName();
        boolean newHasIdentity = newTable.getColumns().stream().anyMatch(c -> ((MsColumn) c).isIdentity());
        if (newHasIdentity) {
            // There can only be one IDENTITY column per table in MSSQL.
            script.addStatement(getIdentInsertText(tblQName, true));
        }

        StringBuilder sbInsert = new StringBuilder();

        sbInsert.append("INSERT INTO ").append(tblQName).append('(').append(cols).append(")");
        sbInsert.append("\nSELECT ").append(cols).append(" FROM ").append(tblTmpQName);
        script.addStatement(sbInsert);

        if (newHasIdentity) {
            // There can only be one IDENTITY column per table in MSSQL.
            script.addStatement(getIdentInsertText(tblQName, false));
        }

        if (!identityColsForMovingData.isEmpty() && newHasIdentity) {
            // DECLARE'd var is only visible within its batch
            // so we shouldn't need unique names for them here
            // use the largest numeric type to fit any possible identity value
            StringBuilder sbSql = new StringBuilder();
            sbSql.append("DECLARE @restart_var numeric(38,0) = (SELECT IDENT_CURRENT (")
                    .append(PgDiffUtils.quoteString(tblTmpQName))
                    .append("));\nDBCC CHECKIDENT (")
                    .append(PgDiffUtils.quoteString(tblQName))
                    .append(", RESEED, @restart_var);");
            script.addStatement(sbSql);
        }
    }

    private String getIdentInsertText(String name, boolean isOn) {
        return "SET IDENTITY_INSERT " + name + (isOn ? " ON" : " OFF");
    }

    @Override
    protected List<String> getColsForMovingData(AbstractTable newTbl) {
        return newTbl.getColumns().stream()
                .filter(c -> containsColumn(c.getName()))
                .map(MsColumn.class::cast)
                .filter(msCol -> msCol.getExpression() == null && msCol.getGenerated() == null)
                .map(AbstractColumn::getName).toList();
    }

    @Override
    protected boolean compareTable(AbstractStatement obj) {
        return obj instanceof MsTable table
                && ansiNulls == table.ansiNulls
                && Objects.equals(textImage, table.textImage)
                && Objects.equals(fileStream, table.fileStream)
                && Objects.equals(isTracked, table.isTracked)
                && Objects.equals(tablespace, table.tablespace)
                && Objects.equals(periodStartCol, table.periodStartCol)
                && Objects.equals(periodEndCol, table.periodEndCol)
                && Objects.equals(sysVersioning, table.sysVersioning)
                && Utils.setLikeEquals(getPkeys(), table.getPkeys());
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(textImage);
        hasher.put(fileStream);
        hasher.put(ansiNulls);
        hasher.put(isTracked);
        hasher.put(tablespace);
        hasher.put(periodStartCol);
        hasher.put(periodEndCol);
        hasher.put(sysVersioning);
        hasher.putUnordered(getPkeys());
    }

    @Override
    protected MsTable getTableCopy() {
        MsTable table = new MsTable(name);
        table.setFileStream(fileStream);
        table.setTextImage(textImage);
        table.setAnsiNulls(ansiNulls);
        table.setTracked(isTracked);
        table.setTablespace(tablespace);
        table.setPeriodStartCol(periodStartCol);
        table.setPeriodEndCol(periodEndCol);
        table.setSysVersioning(sysVersioning);

        if (pkeys != null) {
            table.pkeys = new ArrayList<>();
            table.pkeys.addAll(pkeys);
        }
        return table;
    }

    @Override
    public DatabaseType getDbType() {
        return DatabaseType.MS;
    }

    @Override
    public void addChild(IStatement st) {
        if (st instanceof MsStatistics stat) {
            addStatistics(stat);
        } else {
            super.addChild(st);
        }
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        if (DbObjType.STATISTICS == type) {
            return getChildByName(statistics, name);
        }
        return super.getChild(name, type);
    }

    @Override
    protected void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        super.fillChildrenList(l);
        l.add(statistics.values());
    }

    private void addStatistics(final MsStatistics stat) {
        addUnique(statistics, stat);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof MsTable table && super.compareChildren(obj)) {
            return statistics.equals(table.statistics);
        }
        return false;
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        super.computeChildrenHash(hasher);
        hasher.putUnordered(statistics);
    }
}
