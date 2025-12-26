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
package org.pgcodekeeper.core.database.base.schema;

import org.pgcodekeeper.core.database.api.schema.IRelation;
import org.pgcodekeeper.core.database.api.schema.ISearchPath;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Abstract base class for database sequences.
 * Provides common functionality for auto-incrementing sequences across different database types.
 */
public abstract class AbstractSequence extends AbstractStatement implements IRelation, ISearchPath {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSequence.class);

    protected static final String BIGINT = "bigint";

    private static final List<Pair<String, String>> relationColumns = List.of(
            new Pair<>("sequence_name", "name"), new Pair<>("last_value", BIGINT),
            new Pair<>("start_value", BIGINT), new Pair<>("increment_by", BIGINT),
            new Pair<>("max_value", BIGINT), new Pair<>("min_value", BIGINT),
            new Pair<>("cache_value", BIGINT), new Pair<>("log_cnt", BIGINT),
            new Pair<>("is_cycled", "boolean"), new Pair<>("is_called", "boolean"));

    protected String cache;
    protected String increment;
    protected String maxValue;
    protected String minValue;
    protected String startWith;
    protected boolean cycle;
    protected String dataType = BIGINT;

    @Override
    public DbObjType getStatementType() {
        return DbObjType.SEQUENCE;
    }

    protected AbstractSequence(String name) {
        super(name);
    }

    public void setCache(final String cache) {
        this.cache = cache;
        resetHash();
    }

    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        return relationColumns.stream();
    }

    public void setCycle(final boolean cycle) {
        this.cycle = cycle;
        resetHash();
    }

    /**
     * Sets the minimum, maximum, and increment values for this sequence.
     *
     * @param inc       the increment value
     * @param max       the maximum value
     * @param min       the minimum value
     * @param dataType  the data type of the sequence
     * @param precision the precision for numeric types
     */
    public abstract void setMinMaxInc(long inc, Long max, Long min, String dataType, long precision);

    protected long getBoundaryTypeVal(String type, boolean needMaxVal, long precision) {
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

    public void setStartWith(final String startWith) {
        this.startWith = startWith;
        resetHash();
    }

    public void setDataType(final String dataType) {
        this.dataType = dataType;
        resetHash();
    }

    public String getDataType() {
        return dataType;
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AbstractSequence seq && super.compare(obj)) {
            return cycle == seq.cycle
                    && Objects.equals(increment, seq.increment)
                    && Objects.equals(minValue, seq.minValue)
                    && Objects.equals(maxValue, seq.maxValue)
                    && Objects.equals(startWith, seq.startWith)
                    && Objects.equals(cache, seq.cache)
                    && Objects.equals(dataType, seq.dataType);
        }

        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(cache);
        hasher.put(cycle);
        hasher.put(increment);
        hasher.put(maxValue);
        hasher.put(minValue);
        hasher.put(startWith);
        hasher.put(dataType);
    }

    @Override
    public AbstractSequence shallowCopy() {
        AbstractSequence sequenceDst = getSequenceCopy();
        copyBaseFields(sequenceDst);
        sequenceDst.setCache(cache);
        sequenceDst.setCycle(cycle);
        sequenceDst.increment = increment;
        sequenceDst.maxValue = maxValue;
        sequenceDst.minValue = minValue;
        sequenceDst.dataType = dataType;
        sequenceDst.setStartWith(startWith);
        return sequenceDst;
    }

    protected abstract AbstractSequence getSequenceCopy();

    /**
     * Fills the sequence body SQL definition.
     *
     * @param sbSQL the StringBuilder to append the sequence body to
     */
    public abstract void fillSequenceBody(StringBuilder sbSQL);
}
