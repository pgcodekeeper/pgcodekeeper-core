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
package org.pgcodekeeper.core.database.pg.schema;

import java.util.Objects;

import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * PostgreSQL range type implementation.
 * Range types store ranges of values for a given subtype, such as int4range for integers
 * or tsrange for timestamps. They support inclusion/exclusion bounds and various operations.
 */
public class PgRangeType extends PgAbstractType {

    private String subtype;
    private String subtypeOpClass;
    private String collation;
    private String canonical;
    private String subtypeDiff;
    private String multirange;

    /**
     * Creates a new PostgreSQL range type.
     *
     * @param name range type name
     */
    public PgRangeType(String name) {
        super(name);
    }

    @Override
    protected void appendDef(StringBuilder sb) {
        // lowercase keywords follow pg_dump format here
        sb.append(" AS RANGE (")
                .append("\n\tsubtype = ").append(subtype);
        appendStringOption(sb, "subtype_opclass", subtypeOpClass);
        appendStringOption(sb, "collation", collation);
        appendStringOption(sb, "canonical", canonical);
        appendStringOption(sb, "subtype_diff", subtypeDiff);
        appendStringOption(sb, "multirange_type_name", multirange);
        sb.append("\n)");
    }

    /**
     * Gets the underlying data type for this range.
     *
     * @return subtype name
     */
    public String getSubtype() {
        return subtype;
    }

    public void setSubtype(String subtype) {
        this.subtype = subtype;
        resetHash();
    }

    public void setSubtypeOpClass(String subtypeOpClass) {
        this.subtypeOpClass = subtypeOpClass;
        resetHash();
    }

    public void setCollation(String collation) {
        this.collation = collation;
        resetHash();
    }

    public void setCanonical(String canonical) {
        this.canonical = canonical;
        resetHash();
    }

    public void setSubtypeDiff(String subtypeDiff) {
        this.subtypeDiff = subtypeDiff;
        resetHash();
    }

    public void setMultirange(String multirange) {
        this.multirange = multirange;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(subtype);
        hasher.put(subtypeOpClass);
        hasher.put(collation);
        hasher.put(canonical);
        hasher.put(subtypeDiff);
        hasher.put(multirange);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof PgRangeType type && super.compare(type)
                && compareUnalterable(type);
    }

    @Override
    protected boolean compareUnalterable(PgAbstractType newType) {
        PgRangeType type = (PgRangeType) newType;
        return Objects.equals(subtype, type.subtype)
                && Objects.equals(subtypeOpClass, type.subtypeOpClass)
                && Objects.equals(collation, type.collation)
                && Objects.equals(canonical, type.canonical)
                && Objects.equals(subtypeDiff, type.subtypeDiff)
                && Objects.equals(multirange, type.multirange);
    }

    @Override
    protected PgAbstractType getCopy() {
        PgRangeType copy = new PgRangeType(name);
        copy.setSubtype(subtype);
        copy.setSubtypeOpClass(subtypeOpClass);
        copy.setCollation(collation);
        copy.setCanonical(canonical);
        copy.setSubtypeDiff(subtypeDiff);
        copy.setMultirange(multirange);
        return copy;
    }
}
