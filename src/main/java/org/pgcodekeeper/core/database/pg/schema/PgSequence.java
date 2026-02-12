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
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.*;
import org.pgcodekeeper.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL sequence implementation.
 * Sequences generate unique numeric identifiers, commonly used for auto-incrementing primary keys.
 * Supports various data types, caching, cycling, and ownership by table columns.
 */
public final class PgSequence extends PgAbstractStatement implements ISequence {

    private static final Logger LOG = LoggerFactory.getLogger(PgSequence.class);

    private static final String ALTER_SEQUENCE = "ALTER SEQUENCE ";
    private static final String BIGINT = "bigint";

    private static final List<Pair<String, String>> relationColumns = List.of(
            new Pair<>("sequence_name", "name"), new Pair<>("last_value", BIGINT),
            new Pair<>("start_value", BIGINT), new Pair<>("increment_by", BIGINT),
            new Pair<>("max_value", BIGINT), new Pair<>("min_value", BIGINT),
            new Pair<>("cache_value", BIGINT), new Pair<>("log_cnt", BIGINT),
            new Pair<>("is_cycled", "boolean"), new Pair<>("is_called", "boolean"));

    private GenericColumn ownedBy;
    private boolean isLogged = true;
    private String cache = "1";
    private String dataType = BIGINT;
    private String startWith;
    private String increment;
    private String maxValue;
    private String minValue;
    private boolean cycle;

    /**
     * Creates a new PostgreSQL sequence with default cache value of 1.
     *
     * @param name sequence name
     */
    public PgSequence(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE ");
        if (!isLogged) {
            sbSQL.append("UNLOGGED ");
        }
        sbSQL.append("SEQUENCE ");
        appendIfNotExists(sbSQL, script.getSettings());
        sbSQL.append(getQualifiedName());

        if (!BIGINT.equals(dataType)) {
            sbSQL.append("\n\tAS ").append(dataType);
        }

        fillSequenceBody(sbSQL);
        script.addStatement(sbSQL);

        getOwnedBySQL(script);
        appendOwnerSQL(script);
        appendPrivileges(script);
        appendComments(script);
    }

    public void fillSequenceBody(StringBuilder sbSQL) {
        if (startWith != null) {
            sbSQL.append("\n\tSTART WITH ");
            sbSQL.append(startWith);
        }

        if (increment != null) {
            sbSQL.append("\n\tINCREMENT BY ");
            sbSQL.append(increment);
        }

        sbSQL.append("\n\t");

        if (maxValue == null) {
            sbSQL.append("NO MAXVALUE");
        } else {
            sbSQL.append("MAXVALUE ");
            sbSQL.append(maxValue);
        }

        sbSQL.append("\n\t");

        if (minValue == null) {
            sbSQL.append("NO MINVALUE");
        } else {
            sbSQL.append("MINVALUE ");
            sbSQL.append(minValue);
        }

        if (cache != null) {
            sbSQL.append("\n\tCACHE ");
            sbSQL.append(cache);
        }

        if (cycle) {
            sbSQL.append("\n\tCYCLE");
        }
    }

    /**
     * Creates SQL statement for modification "OWNED BY" parameter.
     */
    private void getOwnedBySQL(SQLScript script) {
        if (ownedBy == null) {
            return;
        }

        String sbSQL = ALTER_SEQUENCE + getQualifiedName() +
                "\n\tOWNED BY " + ownedBy.getQualifiedName();
        script.addStatement(sbSQL, SQLActionType.END);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgSequence newSequence = (PgSequence) newCondition;
        StringBuilder sbSQL = new StringBuilder();

        if (compareSequenceBody(newSequence, sbSQL)) {
            script.addStatement(ALTER_SEQUENCE + newSequence.getQualifiedName() + sbSQL);
        }

        appendAlterOwner(newSequence, script);

        if (isLogged != newSequence.isLogged) {
            StringBuilder sql = new StringBuilder();
            sql.append(ALTER_SEQUENCE).append(newSequence.getQualifiedName())
                    .append(" SET")
                    .append(newSequence.isLogged ? " LOGGED" : " UNLOGGED");
            script.addStatement(sql);
        }

        alterPrivileges(newSequence, script);
        appendAlterComments(newSequence, script);

        if (!Objects.equals(ownedBy, newSequence.ownedBy)) {
            newSequence.getOwnedBySQL(script);
        }

        return getObjectState(script, startSize);
    }

    private boolean compareSequenceBody(PgSequence newSequence, StringBuilder sbSQL) {
        final String oldType = dataType;
        final String newType = newSequence.dataType;

        if (!oldType.equals(newType)) {
            sbSQL.append("\n\tAS ");
            sbSQL.append(newType);
        }

        final String newIncrement = newSequence.increment;
        if (newIncrement != null && !newIncrement.equals(increment)) {
            sbSQL.append("\n\tINCREMENT BY ");
            sbSQL.append(newIncrement);
        }

        final String newMinValue = newSequence.minValue;
        if (newMinValue == null && minValue != null) {
            sbSQL.append("\n\tNO MINVALUE");
        } else if (newMinValue != null && !newMinValue.equals(minValue)) {
            sbSQL.append("\n\tMINVALUE ");
            sbSQL.append(newMinValue);
        }

        final String newMaxValue = newSequence.maxValue;
        if (newMaxValue == null && maxValue != null) {
            sbSQL.append("\n\tNO MAXVALUE");
        } else if (newMaxValue != null && !newMaxValue.equals(maxValue)) {
            sbSQL.append("\n\tMAXVALUE ");
            sbSQL.append(newMaxValue);
        }

        final String newStart = newSequence.startWith;
        if (newStart != null && !newStart.equals(startWith)) {
            sbSQL.append("\n\tSTART WITH ");
            sbSQL.append(newStart);
        }

        final String newCache = newSequence.cache;
        if (newCache != null && !newCache.equals(cache)) {
            sbSQL.append("\n\tCACHE ");
            sbSQL.append(newCache);
        }

        final boolean newCycle = newSequence.cycle;
        if (cycle && !newCycle) {
            sbSQL.append("\n\tNO CYCLE");
        } else if (!cycle && newCycle) {
            sbSQL.append("\n\tCYCLE");
        }

        final GenericColumn newOwnedBy = newSequence.ownedBy;
        if (newOwnedBy == null && ownedBy != null) {
            sbSQL.append("\n\tOWNED BY NONE");
        }

        return !sbSQL.isEmpty();
    }

    public void setMinMaxInc(long inc, Long max, Long min, String dataType, long precision) {
        String type = dataType != null ? dataType : BIGINT;
        this.increment = Long.toString(inc);
        if (max == null || (inc > 0 && max == getBoundaryTypeVal(type, true, 0L))
                || (inc < 0 && max == -1)) {
            this.maxValue = null;
        } else {
            this.maxValue = "" + max;
        }
        if (min == null || (inc > 0 && min == 1)
                || (inc < 0 && min == getBoundaryTypeVal(type, false, 0L))) {
            this.minValue = null;
        } else {
            this.minValue = "" + min;
        }

        if (startWith == null) {
            setStartWith(Objects.requireNonNullElseGet(this.minValue, () -> inc < 0 ? "-1" : "1"));
        }
        resetHash();
    }

    public void setStartWith(String startWith) {
        this.startWith = startWith;
    }

    private long getBoundaryTypeVal(String type, boolean needMaxVal, long precision) {
        return switch (type) {
            case "tinyint" -> needMaxVal ? 255 : 0;
            case "smallint" -> needMaxVal ? Short.MAX_VALUE : Short.MIN_VALUE;
            case "int", "integer" -> needMaxVal ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            case BIGINT -> needMaxVal ? Long.MAX_VALUE : Long.MIN_VALUE;
            case "numeric", "decimal" -> {
                // It used for MS SQL.
                long boundaryTypeVal = (long) (Math.pow(10, precision)) - 1;
                yield needMaxVal ? boundaryTypeVal : -boundaryTypeVal;
            }
            default -> {
                var msg = "Unsupported sequence type: %s".formatted(type);
                LOG.warn(msg);
                yield needMaxVal ? Long.MAX_VALUE : Long.MIN_VALUE;
            }
        };
    }

    /**
     * Gets the table column that owns this sequence.
     *
     * @return column reference or null if not owned
     */
    public GenericColumn getOwnedBy() {
        return ownedBy;
    }

    public void setOwnedBy(final GenericColumn ownedBy) {
        this.ownedBy = ownedBy;
        resetHash();
    }

    public void setDataType(String dataType) {
        this.dataType = dataType.toLowerCase(Locale.ROOT);
        resetHash();
    }

    public String getDataType() {
        return dataType;
    }

    /**
     * Checks if this sequence is logged (written to WAL).
     *
     * @return true if logged, false if unlogged
     */
    public boolean isLogged() {
        return isLogged;
    }

    public void setLogged(boolean isLogged) {
        this.isLogged = isLogged;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(ownedBy == null ? 0 : ownedBy.hashCode());
        hasher.put(cache);
        hasher.put(cycle);
        hasher.put(increment);
        hasher.put(maxValue);
        hasher.put(minValue);
        hasher.put(dataType);
        hasher.put(startWith);
        hasher.put(isLogged);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj instanceof PgSequence seq && super.compare(obj)) {
            return Objects.equals(ownedBy, seq.ownedBy)
                    && Objects.equals(cache, seq.cache)
                    && cycle == seq.cycle
                    && Objects.equals(increment, seq.increment)
                    && Objects.equals(maxValue, seq.maxValue)
                    && Objects.equals(minValue, seq.minValue)
                    && Objects.equals(dataType, seq.dataType)
                    && Objects.equals(startWith, seq.startWith)
                    && isLogged == seq.isLogged;
        }

        return false;
    }

    @Override
    protected PgSequence getCopy() {
        PgSequence copy = new PgSequence(name);
        copy.setOwnedBy(ownedBy);
        copy.setCache(cache);
        copy.setCycle(cycle);
        copy.increment = increment;
        copy.maxValue = maxValue;
        copy.minValue = minValue;
        copy.dataType = dataType;
        copy.setStartWith(startWith);
        copy.setLogged(isLogged);
        return copy;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
        resetHash();
    }

    public void setCache(String cache) {
        this.cache = cache;
        resetHash();
    }

    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        return relationColumns.stream();
    }
}
