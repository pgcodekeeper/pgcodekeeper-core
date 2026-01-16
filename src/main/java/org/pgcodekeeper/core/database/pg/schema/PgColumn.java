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

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL column implementation.
 * Stores column information including data type, constraints, storage parameters,
 * statistics, compression settings, and identity properties.
 */
public final class PgColumn extends AbstractColumn
        implements ISimpleOptionContainer, ICompressOptionContainer, IPgStatement {

    private static final Logger LOG = LoggerFactory.getLogger(PgColumn.class);

    private static final String ALTER_FOREIGN_OPTION = "%s OPTIONS (%s %s %s)";
    private static final String COMPRESSION = " COMPRESSION ";

    private Integer statistics;
    private String storage;
    private final Map<String, String> options = new LinkedHashMap<>(0);
    private final Map<String, String> fOptions = new LinkedHashMap<>(0);
    private PgSequence sequence;
    private String identityType;
    private String compression;
    private boolean isInherit;
    private String generationOption;
    private PgConstraintNotNull notNullConstraint;

    // greenplum type fields
    private String compressType;
    private int compressLevel = -1;
    private int blockSize;

    /**
     * Creates a new PostgreSQL column.
     *
     * @param name column name
     */
    public PgColumn(String name) {
        super(name);
    }

    @Override
    public String getFullDefinition() {
        final StringBuilder sbDefinition = new StringBuilder();
        String cName = getQuotedName(name);
        sbDefinition.append(cName);

        if (type == null) {
            sbDefinition.append(" WITH OPTIONS");
        } else {
            sbDefinition.append(' ');
            sbDefinition.append(type);
            if (compression != null) {
                sbDefinition.append(COMPRESSION).append(getQuotedName(compression));
            }

            if (collation != null) {
                sbDefinition.append(COLLATE).append(collation);
            }
        }

        definitionDefaultNotNull(sbDefinition);

        generatedAlways(sbDefinition);

        appendCompressOptions(sbDefinition);
        return sbDefinition.toString();
    }

    private void definitionDefaultNotNull(StringBuilder sbDefinition) {
        if (defaultValue != null && generationOption == null) {
            sbDefinition
                    .append(" DEFAULT ")
                    .append(defaultValue);
        }

        if (notNullConstraint != null && !notNullConstraint.isNotValid()) {
            notNullConstraint.getDefinitionForColumn(sbDefinition);
        }
    }

    private void generatedAlways(StringBuilder sbDefinition) {
        if (generationOption != null) {
            sbDefinition.append(" GENERATED ALWAYS AS (")
                    .append(defaultValue)
                    .append(")");
            if (!"VIRTUAL".equals(generationOption)) {
                sbDefinition.append(" ")
                        .append(generationOption);
            }
        }
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        StringBuilder sb = new StringBuilder();

        boolean isMergeDefaultNotNull = false;
        if (type != null && getParentCol((PgAbstractTable) parent) == null) {
            sb.append(getAlterTable(false));
            sb.append("\n\tADD COLUMN ");
            appendIfNotExists(sb, script.getSettings());
            sb.append(getQuotedName(name))
                    .append(' ')
                    .append(type);
            if (compression != null) {
                sb.append(COMPRESSION).append(getQuotedName(compression));
            }
            if (collation != null) {
                sb.append(COLLATE).append(collation);
            }

            isMergeDefaultNotNull = notNullConstraint != null;
            if (isMergeDefaultNotNull) {
                // for NOT NULL columns we'd emit a time-consuming UPDATE column=DEFAULT anyway,
                // so we can merge DEFAULT with column definition with no performance loss
                // this operation also becomes fast on PostgreSQL 11+ (metadata only operation)
                definitionDefaultNotNull(sb);
            }

            generatedAlways(sb);
            appendCompressOptions(sb);

            script.addStatement(sb);
        }

        // column may have a default expression or a generation expression
        // (https://www.postgresql.org/docs/12/catalog-pg-attribute.html) (param - 'atthasdef')
        if (!isMergeDefaultNotNull && generationOption == null) {
            compareDefaults(null, defaultValue, new AtomicBoolean(), script);
            compareNotNull(null, this, script);
        }
        compareStorages(null, storage, script);

        appendPrivileges(script);

        compareForeignOptions(Collections.emptyMap(), fOptions, script);
        writeOptions(true, script);

        compareStats(null, statistics, script);
        compareIdentity(null, identityType, null, sequence, script);

        appendComments(script);
    }

    private void appendCompressOptions(StringBuilder sb) {
        if (compressType != null || compressLevel != -1 || blockSize != 0) {
            sb.append(" ENCODING (");
            if (compressType != null) {
                sb.append("COMPRESSTYPE = ").append(compressType);
            }

            if (compressLevel != -1) {
                sb.append(", COMPRESSLEVEL = ").append(compressLevel);
            }

            if (blockSize != 0) {
                sb.append(", BLOCKSIZE = ").append(blockSize);
            }
            sb.append(")");
        }
    }

    private String getAlterTableColumn(boolean only, String column) {
        return getAlterTableColumn(only, column, true);
    }

    private String getAlterTableColumn(boolean only, String column, boolean needAlterTable) {
        StringBuilder sb = new StringBuilder();
        if (needAlterTable) {
            sb.append(getAlterTable(only));
        }
        sb.append(ALTER_COLUMN).append(getQuotedName(column));
        return sb.toString();
    }

    @Override
    public void getDropSQL(SQLScript script, boolean optionExists) {
        if (type != null && getParentCol((PgAbstractTable) parent) == null) {
            StringBuilder dropSb = new StringBuilder();
            dropSb.append(getAlterTable(isNeedOnly())).append("\n\tDROP COLUMN ");
            if (optionExists) {
                dropSb.append(IF_EXISTS);
            }
            dropSb.append(getQuotedName(name));
            script.addStatement(dropSb);
            return;
        }

        compareDefaults(defaultValue, null, null, script);
        compareNotNull(this, null,script);
        compareStorages(storage, null, script);

        alterPrivileges(new PgColumn(name), script);

        compareForeignOptions(fOptions, Collections.emptyMap(), script);
        writeOptions(false, script);
        compareStats(statistics, null, script);
        compareIdentity(identityType, null, sequence, null, script);

        appendComments(script);
    }

    /**
     * @return true if parent table isn't partition table with existing partitions, false otherwise
     */
    private boolean isNeedOnly() {
        if (parent instanceof PgAbstractRegularTable regTable) {
            return regTable.getPartitionBy() == null;
        }
        return true;
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgColumn newColumn = (PgColumn) newCondition;

        if (!Objects.equals(generationOption, newColumn.generationOption)
                || (generationOption != null && !Objects.equals(defaultValue, newColumn.defaultValue))
                || !compareCompressOptions(newColumn)) {
            return ObjectState.RECREATE;
        }

        boolean isNeedDropDefault = !Objects.equals(type, newColumn.type)
                && !Objects.equals(defaultValue, newColumn.defaultValue);

        if (isNeedDropDefault) {
            compareDefaults(defaultValue, null, null, script);
        }
        AtomicBoolean isNeedDependencies = new AtomicBoolean();
        StringBuilder typeBuilder = new StringBuilder();
        compareTypes(this, newColumn, isNeedDependencies, typeBuilder, true, true, script.getSettings());
        if (!typeBuilder.isEmpty()) {
            script.addStatement(typeBuilder);
        }

        String oldDefault = isNeedDropDefault ? null : defaultValue;
        compareDefaults(oldDefault, newColumn.defaultValue, isNeedDependencies, script);
        compareNotNull(this, newColumn, script);
        compareStorages(storage, newColumn.storage, script);
        compareCompression(compression, newColumn.compression, script);

        alterPrivileges(newColumn, script);

        compareOptions(newColumn, script);
        compareForeignOptions(fOptions, newColumn.fOptions, script);
        compareStats(statistics, newColumn.statistics, script);

        compareIdentity(identityType, newColumn.identityType, sequence, newColumn.sequence, script);
        appendAlterComments(newColumn, script);
        return getObjectState(isNeedDependencies.get(), script, startSize);
    }

    private boolean compareCompressOptions(PgColumn newColumn) {
        return Objects.equals(compressType, newColumn.compressType)
                && compressLevel == newColumn.compressLevel
                && blockSize == newColumn.blockSize;
    }

    /**
     * Writes SET/RESET options for column to StringBuilder
     *
     * @param isCreate if true SET options, else RESET
     * @param script   for collect sql statements
     */
    private void writeOptions(boolean isCreate, SQLScript script) {
        if (!options.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(getAlterTableColumn(isNeedOnly(), name));
            sb.append(isCreate ? " SET (" : " RESET (");
            for (Entry<String, String> option : options.entrySet()) {
                sb.append(option.getKey());
                if (isCreate && !option.getValue().isEmpty()) {
                    sb.append('=').append(option.getValue());
                }
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append(")");
            script.addStatement(sb);
        }
    }

    /**
     * Compare columns identity and write difference to StringBuilder.
     *
     * @param oldIdentityType old column identity type
     * @param newIdentityType new column identity type
     * @param oldSequence     old column identity sequence
     * @param newSequence     new column identity sequence
     * @param script          for collect sql statements
     */
    private void compareIdentity(String oldIdentityType, String newIdentityType,
                                 PgSequence oldSequence, PgSequence newSequence, SQLScript script) {
        if (!Objects.equals(oldIdentityType, newIdentityType)) {
            StringBuilder sb = new StringBuilder();
            sb.append(getAlterTableColumn(false, name));

            if (newIdentityType == null) {
                sb.append(" DROP IDENTITY");
                if (script.getSettings().isGenerateExists()) {
                    sb.append(" IF EXISTS");
                }
            } else if (oldIdentityType == null) {
                sb.append(" ADD GENERATED ")
                        .append(newIdentityType)
                        .append(" AS IDENTITY (")
                        .append("\n\tSEQUENCE NAME ")
                        .append(newSequence.getQualifiedName());
                newSequence.fillSequenceBody(sb);
                sb.append("\n)");
            } else {
                sb.append(" SET GENERATED ").append(newIdentityType);
            }
            script.addStatement(sb);
        }

        if (oldSequence != null && newSequence != null &&
                !Objects.equals(oldSequence, newSequence)) {
            var newName = newSequence.getName();
            if (!oldSequence.getName().equals(newName)) {
                script.addStatement(oldSequence.getRenameCommand(newName));
            }

            oldSequence.appendAlterSQL(newSequence, script);
        }
    }

    /**
     * Checks if this column can be joined with another column in a single ALTER statement.
     *
     * @param newColumn column to compare with
     * @return true if columns can be joined in one ALTER statement
     */
    public boolean isJoinable(PgColumn newColumn) {
        return newColumn.type != null
                && (!Objects.equals(type, newColumn.type)
                || !Objects.equals(collation, newColumn.collation))
                && Objects.equals(defaultValue, newColumn.defaultValue)
                && Objects.equals(notNullConstraint != null, newColumn.notNullConstraint != null)
                && compareColOptions(newColumn)
                && Objects.equals(comment, newColumn.comment);
    }

    /**
     * Generates SQL for joining column changes in a single ALTER statement.
     *
     * @param sb               StringBuilder to append SQL to
     * @param newColumn        new column state
     * @param isNeedAlterTable whether to include ALTER TABLE prefix
     * @param isLastColumn     whether this is the last column in a multi-column ALTER
     * @param settings         generation settings
     */
    public void joinAction(StringBuilder sb, PgColumn newColumn, boolean isNeedAlterTable,
                           boolean isLastColumn, ISettings settings) {
        compareTypes(this, newColumn, new AtomicBoolean(), sb, isNeedAlterTable, isLastColumn, settings);
    }

    /**
     * Compares two columns types and collations and write difference to StringBuilder.
     * If the values are not equal, then the column will be changed with dependencies.
     * Adds warning as SQL comment.
     *
     * @param oldColumn           old column
     * @param newColumn           new column
     * @param isNeedDependencies  if set true, column will be changed with dependencies
     * @param sb                  StringBuilder for difference
     * @param isNeedAlterTable    if true ALTER TABLE sentence added before ALTER COLUMN
     * @param isLastColumn        if true will be added ";" in the end of ALTER COLUMN. If false then - ",".
     */
    private void compareTypes(PgColumn oldColumn, PgColumn newColumn, AtomicBoolean isNeedDependencies,
                              StringBuilder sb, boolean isNeedAlterTable, boolean isLastColumn, ISettings settings) {
        String oldType = oldColumn.type;
        String newType = newColumn.type;
        if (newType == null) {
            return;
        }

        String oldCollation = oldColumn.collation;
        String newCollation = newColumn.collation;

        if (!Objects.equals(oldType, newType) || (newCollation != null && !newCollation.equals(oldCollation))) {
            isNeedDependencies.set(true);
            sb.append(getAlterTableColumn(false, newColumn.name, isNeedAlterTable));
            sb.append(" TYPE ").append(newType);

            if (newCollation != null) {
                sb.append(COLLATE).append(newCollation);
            }

            if (settings.isPrintUsing() && !(parent instanceof IForeignTable)) {
                sb.append(" USING ").append(getQuotedName(newColumn.name))
                        .append("::").append(newType);
            }
            sb.append(isLastColumn ? ";" : ",");
            sb.append(" /* ").append(Messages.Table_TypeParameterChange.formatted(
                    newColumn.parent.getParent().getName() + '.' + newColumn.parent.getName(),
                    oldType, newType)).append(" */");
        }
    }

    /**
     * Compares two columns foreign options and write difference to StringBuilder.
     *
     * @param oldForeignOptions old column foreign options
     * @param newForeignOptions new column foreign options
     * @param script            collection for actions
     */
    private void compareForeignOptions(Map<String, String> oldForeignOptions, Map<String, String> newForeignOptions,
                                       SQLScript script) {
        if (!oldForeignOptions.isEmpty() || !newForeignOptions.isEmpty()) {
            oldForeignOptions.forEach((key, value) -> {
                if (newForeignOptions.containsKey(key)) {
                    String newValue = newForeignOptions.get(key);
                    if (!Objects.equals(value, newValue)) {
                        script.addStatement(getAlterOption("SET", key, newValue));
                    }
                } else {
                    script.addStatement(getAlterOption("DROP", key, ""));
                }
            });

            newForeignOptions.forEach((key, value) -> {
                if (!oldForeignOptions.containsKey(key)) {
                    script.addStatement(getAlterOption("ADD", key, value));
                }
            });
        }
    }

    private String getAlterOption(String action, String key, String value) {
        return ALTER_FOREIGN_OPTION.formatted(getAlterTableColumn(false, name), action, key, value);
    }

    /**
     * Compares not-null values of two columns and writes difference to script.
     *
     * @param oldColumn old column state
     * @param newColumn new column state
     * @param script    script for collect sql statements
     */
    private void compareNotNull(PgColumn oldColumn, PgColumn newColumn, SQLScript script) {
        boolean isOldNotNull = oldColumn != null && notNullConstraint != null;
        boolean isNewNotNull = newColumn != null && newColumn.notNullConstraint != null;

        if (!isNewNotNull && !isOldNotNull) {
            return;
        }

        // DROP
        if (isOldNotNull && !isNewNotNull) {
            script.addStatement(getAlterTableColumn(isNeedOnly(), name) + " DROP" + NOT_NULL);
            return;
        }

        // CREATE
        var newNotNullConstraint = newColumn.notNullConstraint;
        if (!isOldNotNull) {
            if (newColumn.defaultValue != null) {
                String sql = "UPDATE " + parent.getQualifiedName() +
                        "\n\tSET " + getQuotedName(name) +
                        " = DEFAULT WHERE " + getQuotedName(name) +
                        " IS" + NULL;
                script.addStatement(sql);
            }

            if (newNotNullConstraint.isComplexNotNull()) {
                newNotNullConstraint.getCreationSQL(script);
            } else {
                script.addStatement(getAlterTableColumn(false, name) + " SET" + NOT_NULL);
            }
            return;
        }

        // ALTER
        var newName = newNotNullConstraint.getName();
        if (!notNullConstraint.getName().equals(newName)) {
            script.addStatement(notNullConstraint.getRenameCommand(newName));
        }

        notNullConstraint.appendAlterSQL(newNotNullConstraint, script);
    }

    /**
     * Compares two columns default values and write difference to StringBuilder. If
     * the default values are not equal, and the new value is not null, then the
     * column will be changed with dependencies.
     *
     * @param oldDefault         old column default value
     * @param newDefault         new column default value
     * @param isNeedDependencies if set true, column will be changed with dependencies
     * @param script             for collect sql statements
     */
    private void compareDefaults(String oldDefault, String newDefault, AtomicBoolean isNeedDependencies, SQLScript script) {
        if (!Objects.equals(oldDefault, newDefault)) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterTableColumn(true, name));
            if (newDefault == null) {
                sql.append(" DROP DEFAULT");
            } else {
                sql.append(" SET DEFAULT ").append(newDefault);
                isNeedDependencies.set(true);
            }
            script.addStatement(sql);
        }
    }

    /**
     * Compares two columns statistics and write difference to StringBuilder.
     *
     * @param oldStat old column statistics
     * @param newStat new column statistics
     * @param script  for collect sql statements
     */
    private void compareStats(Integer oldStat, Integer newStat, SQLScript script) {
        Integer newStatValue = null;

        if (newStat != null && (!newStat.equals(oldStat))) {
            newStatValue = newStat;
        } else if (oldStat != null && newStat == null) {
            newStatValue = -1;
        }
        if (newStatValue != null) {
            script.addStatement(getAlterTableColumn(true, name) + " SET STATISTICS " + newStatValue);
        }
    }

    /**
     * Compares two columns storages and writes difference to StringBuilder. If new
     * column doesn't have storage, adds warning as SQL comment.
     *
     * @param oldStorage old column storage
     * @param newStorage new column storage
     * @param script     for collect sql statements
     */
    private void compareStorages(String oldStorage, String newStorage, SQLScript script) {
        StringBuilder sql;
        if (newStorage == null && oldStorage != null) {
            sql = new StringBuilder();
            sql.append(Messages.Storage_WarningUnableToDetermineStorageType.formatted(
                    parent.getName(), name));
            script.addStatement(sql);
        } else if (newStorage != null && !newStorage.equalsIgnoreCase(oldStorage)) {
            script.addStatement(getAlterTableColumn(true, name) + " SET STORAGE " + newStorage);
        }
    }

    private void compareCompression(String oldCompression, String newCompression, SQLScript script) {
        if (newCompression == null && oldCompression != null) {
            script.addStatement(getAlterTableColumn(true, name) + " SET COMPRESSION DEFAULT");
            return;
        }
        if (newCompression == null || newCompression.equalsIgnoreCase(oldCompression)) {
            return;
        }
        script.addStatement(getAlterTableColumn(true, name) + " SET COMPRESSION " + getQuotedName(newCompression));
    }

    /**
     * Returns the parent column for given column or null if given column hasn't
     * parent column.
     *
     * @param tbl table to search inheritance hierarchy from
     * @return parent column or null if no parent column exists
     */
    public AbstractColumn getParentCol(PgAbstractTable tbl) {
        for (Inherits in : tbl.getInherits()) {
            IStatement parent = getDatabase().getStatement(new GenericColumn(in.key(), in.value(), DbObjType.TABLE));
            if (parent == null) {
                var msg = "There is no such object of inheritance as table: %s".formatted(in.getQualifiedName());
                LOG.error(msg);
                continue;
            }

            PgAbstractTable parentTbl = (PgAbstractTable) parent;
            AbstractColumn parentCol = parentTbl.getColumn(name);
            if (parentCol == null) {
                parentCol = getParentCol(parentTbl);
            }
            if (parentCol != null) {
                // if not found continue searching through other inherit entries
                return parentCol;
            }
        }

        return null;
    }

    @Override
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    public void addOption(String attribute, String value) {
        this.options.put(attribute, value);
        resetHash();
    }

    public Map<String, String> getForeignOptions() {
        return Collections.unmodifiableMap(fOptions);
    }

    /**
     * Adds a foreign table option to this column.
     *
     * @param attribute option name
     * @param value     option value
     */
    public void addForeignOption(String attribute, String value) {
        this.fOptions.put(attribute, value);
        resetHash();
    }

    @Override
    public void setCompressType(String compressType) {
        this.compressType = compressType;
        resetHash();
    }

    @Override
    public void setCompressLevel(int compressLevel) {
        this.compressLevel = compressLevel;
        resetHash();
    }

    @Override
    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
        resetHash();
    }

    public boolean isInherit() {
        return isInherit;
    }

    public void setInherit(boolean isInherit) {
        this.isInherit = isInherit;
        resetHash();
    }

    @Override
    public boolean isNotNull() {
        return notNullConstraint != null;
    }

    @Override
    public void setNotNull(boolean notNull) {
        throw new UnsupportedOperationException("Not supported, add NOT NULL as a constraint instead");
    }

    public boolean isGenerated() {
        return generationOption != null;
    }

    public void setGenerationOption(String generationOption) {
        this.generationOption = generationOption;
        resetHash();
    }

    public void setStatistics(final Integer statistics) {
        this.statistics = statistics;
        resetHash();
    }

    public Integer getStatistics() {
        return statistics;
    }

    public String getStorage() {
        return storage;
    }

    public void setStorage(final String storage) {
        this.storage = storage;
        resetHash();
    }

    public PgSequence getSequence() {
        return sequence;
    }

    public void setSequence(final PgSequence sequence) {
        this.sequence = sequence;
        resetHash();
    }

    public void setIdentityType(final String identityType) {
        this.identityType = identityType;
        resetHash();
    }

    public String getIdentityType() {
        return identityType;
    }

    public void setCompression(String compression) {
        this.compression = compression;
        resetHash();
    }

    public PgConstraintNotNull getNotNullConstraint() {
        return notNullConstraint;
    }

    public void setNotNullConstraint(PgConstraintNotNull notNullConstraint) {
        this.notNullConstraint = notNullConstraint;
        resetHash();
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj instanceof PgColumn col && super.compare(obj)) {
            return compareColOptions(col);
        }

        return false;
    }

    private boolean compareColOptions(PgColumn col) {
        return Objects.equals(statistics, col.statistics)
                && Objects.equals(storage, col.storage)
                && Objects.equals(identityType, col.identityType)
                && isInherit == col.isInherit
                && options.equals(col.options)
                && fOptions.equals(col.fOptions)
                && compareCompressOptions(col)
                && Objects.equals(sequence, col.sequence)
                && Objects.equals(compression, col.compression)
                && Objects.equals(generationOption, col.generationOption)
                && Objects.equals(notNullConstraint, col.notNullConstraint);
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(statistics);
        hasher.put(storage);
        hasher.put(options);
        hasher.put(fOptions);
        hasher.put(compressType);
        hasher.put(compressLevel);
        hasher.put(blockSize);
        hasher.put(sequence);
        hasher.put(compression);
        hasher.put(identityType);
        hasher.put(isInherit);
        hasher.put(generationOption);
        hasher.put(notNullConstraint);
    }

    @Override
    public AbstractColumn shallowCopy() {
        AbstractColumn colDst = getColumnCopy();
        copyBaseFields(colDst);
        colDst.setType(type);
        colDst.setCollation(collation);
        colDst.setDefaultValue(defaultValue);
        return colDst;
    }

    @Override
    protected AbstractColumn getColumnCopy() {
        PgColumn copy = new PgColumn(name);
        copy.setStatistics(statistics);
        copy.setStorage(storage);
        copy.options.putAll(options);
        copy.fOptions.putAll(fOptions);
        copy.setCompressType(compressType);
        copy.setCompressLevel(compressLevel);
        copy.setBlockSize(blockSize);
        copy.setIdentityType(identityType);
        copy.setSequence(sequence);
        copy.setCompression(compression);
        copy.setInherit(isInherit);
        copy.setGenerationOption(generationOption);
        copy.setNotNullConstraint(notNullConstraint);
        return copy;
    }
}
