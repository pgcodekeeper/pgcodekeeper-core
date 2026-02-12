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

import java.util.*;
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a Microsoft SQL sequence object.
 * Sequences generate sequential numeric values and are commonly used for primary keys and unique identifiers.
 */
public final class MsSequence extends MsAbstractStatement implements ISequence {

    private static final Logger LOG = LoggerFactory.getLogger(MsSequence.class);

    private static final String BIGINT = "bigint";

    private static final List<Pair<String, String>> relationColumns = List.of(
            new Pair<>("sequence_name", "name"), new Pair<>("last_value", BIGINT),
            new Pair<>("start_value", BIGINT), new Pair<>("increment_by", BIGINT),
            new Pair<>("max_value", BIGINT), new Pair<>("min_value", BIGINT),
            new Pair<>("cache_value", BIGINT), new Pair<>("log_cnt", BIGINT),
            new Pair<>("is_cycled", "boolean"), new Pair<>("is_called", "boolean"));

    private boolean isCached;
    private String dataType = BIGINT;
    private String startWith;
    private String increment;
    private String maxValue;
    private String minValue;
    private String cache;
    private boolean cycle;

    /**
     * Creates a new Microsoft SQL sequence with BIGINT data type as default.
     *
     * @param name the sequence name
     */
    public MsSequence(String name) {
        super(name);
        setDataType(BIGINT);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE SEQUENCE ");
        sbSQL.append(getQualifiedName());

        sbSQL.append("\n\tAS ").append(dataType);

        fillSequenceBody(sbSQL);
        script.addStatement(sbSQL);
        appendOwnerSQL(script);
        appendPrivileges(script);
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

        sbSQL.append("\n\tMAXVALUE ");
        sbSQL.append(maxValue);

        sbSQL.append("\n\tMINVALUE ");
        sbSQL.append(minValue);

        if (isCached) {
            sbSQL.append("\n\tCACHE ");
            if (cache != null) {
                sbSQL.append(cache);
            }
        }

        if (cycle) {
            sbSQL.append("\n\tCYCLE");
        }
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        MsSequence newSequence = (MsSequence) newCondition;

        if (!newSequence.dataType.equals(dataType)) {
            return ObjectState.RECREATE;
        }

        StringBuilder sbSQL = new StringBuilder();
        if (compareSequenceBody(newSequence, sbSQL)) {
            script.addStatement("ALTER SEQUENCE " + getQualifiedName() + sbSQL);
        }

        appendAlterOwner(newSequence, script);
        alterPrivileges(newSequence, script);
        return getObjectState(script, startSize);
    }

    private boolean compareSequenceBody(MsSequence newSequence, StringBuilder sbSQL) {
        final String oldIncrement = increment;
        final String newIncrement = newSequence.increment;

        if (newIncrement != null
                && !newIncrement.equals(oldIncrement)) {
            sbSQL.append("\n\tINCREMENT BY ");
            sbSQL.append(newIncrement);
        }

        final String oldMinValue = minValue;
        final String newMinValue = newSequence.minValue;

        if (newMinValue == null && oldMinValue != null) {
            sbSQL.append("\n\tNO MINVALUE");
        } else if (newMinValue != null
                && !newMinValue.equals(oldMinValue)) {
            sbSQL.append("\n\tMINVALUE ");
            sbSQL.append(newMinValue);
        }

        final String oldMaxValue = maxValue;
        final String newMaxValue = newSequence.maxValue;

        if (newMaxValue == null && oldMaxValue != null) {
            sbSQL.append("\n\tNO MAXVALUE");
        } else if (newMaxValue != null
                && !newMaxValue.equals(oldMaxValue)) {
            sbSQL.append("\n\tMAXVALUE ");
            sbSQL.append(newMaxValue);
        }

        final String oldStart = startWith;
        final String newStart = newSequence.startWith;

        if (newStart != null && !newStart.equals(oldStart)) {
            sbSQL.append("\n\tRESTART WITH ");
            sbSQL.append(newStart);
        }

        final String oldCache = cache;
        final String newCache = newSequence.cache;

        if (newSequence.isCached && !Objects.equals(newCache, oldCache)) {
            sbSQL.append("\n\tCACHE ");
            if (newCache != null) {
                sbSQL.append(newCache);
            }
        } else if (!newSequence.isCached) {
            sbSQL.append("\n\tNO CACHE");
        }

        final boolean oldCycle = cycle;
        final boolean newCycle = newSequence.cycle;

        if (oldCycle && !newCycle) {
            sbSQL.append("\n\tNO CYCLE");
        } else if (!oldCycle && newCycle) {
            sbSQL.append("\n\tCYCLE");
        }

        return !sbSQL.isEmpty();
    }

    public void setDataType(String dataType) {
        String type = dataType.toLowerCase(Locale.ROOT);
        switch (type) {
            case "tinyint":
            case "smallint":
            case "int":
            case BIGINT:
            case "numeric":
            case "decimal":
                // set lowercased version for simple system types
                break;
            default:
                // set exactly as given
                type = dataType;
        }
        this.dataType = type;
        resetHash();
    }

    public void setMinMaxInc(long inc, Long max, Long min, String dataType, long precision) {
        String type = dataType != null ? dataType : BIGINT;
        this.increment = Long.toString(inc);
        this.maxValue = Long.toString(max == null ? getBoundaryTypeVal(type, true, precision) : max);
        this.minValue = Long.toString(min == null ? getBoundaryTypeVal(type, false, precision) : min);
        resetHash();
    }

    public void setCached(boolean isCached) {
        this.isCached = isCached;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(isCached);
        hasher.put(cache);
        hasher.put(cycle);
        hasher.put(increment);
        hasher.put(maxValue);
        hasher.put(minValue);
        hasher.put(dataType);
        hasher.put(startWith);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj instanceof MsSequence seq && super.compare(obj)) {
            return isCached == seq.isCached
                    && Objects.equals(cache, seq.cache)
                    && cycle == seq.cycle
                    && Objects.equals(increment, seq.increment)
                    && Objects.equals(maxValue, seq.maxValue)
                    && Objects.equals(minValue, seq.minValue)
                    && Objects.equals(dataType, seq.dataType)
                    && Objects.equals(startWith, seq.startWith);
        }

        return false;
    }

    @Override
    protected MsSequence getCopy() {
        MsSequence copy = new MsSequence(name);
        copy.setCached(isCached);
        copy.setCache(cache);
        copy.setCycle(cycle);
        copy.increment = increment;
        copy.maxValue = maxValue;
        copy.minValue = minValue;
        copy.dataType = dataType;
        copy.setStartWith(startWith);
        return copy;
    }

    public void setStartWith(String startWith) {
        this.startWith = startWith;
        resetHash();
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
        resetHash();
    }

    public void setCache(String cache) {
        this.cache = cache;
        resetHash();
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

    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        return relationColumns.stream();
    }
}
