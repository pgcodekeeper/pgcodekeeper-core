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

import java.util.*;

import org.pgcodekeeper.core.hasher.*;
import org.pgcodekeeper.core.utils.Utils;

/**
 * Container for Greenplum partition template information.
 * Manages subpartition template definitions for Greenplum partitioned tables.
 */
public final class GpPartitionTemplateContainer implements IHashable {

    private static final String SET_SUBPARTITION = "\nSET SUBPARTITION TEMPLATE (";

    private final String partitionName;
    private final List<String> subElements = new ArrayList<>();
    private final List<String> normalizedSubElements = new ArrayList<>();

    /**
     * Adds a subpartition element to this template.
     *
     * @param subElement           raw subpartition element
     * @param normalizedSubElement normalized subpartition element for comparison
     */
    public void setSubElems(String subElement, String normalizedSubElement) {
        this.subElements.add(subElement);
        this.normalizedSubElements.add(normalizedSubElement);
    }

    /**
     * Creates a new partition template container.
     *
     * @param partitionName name of the partition, can be null for table-level templates
     */
    public GpPartitionTemplateContainer(String partitionName) {
        this.partitionName = partitionName;
    }

    /**
     * Gets the partition name for this template.
     *
     * @return partition name, or null for table-level templates
     */
    public String getPartitionName() {
        return partitionName;
    }

    /**
     * Checks if this template has any subpartition elements.
     *
     * @return true if template contains subpartition elements
     */
    public boolean hasSubElements() {
        return !subElements.isEmpty();
    }

    void appendCreateSQL(StringBuilder sql) {
        appendPartitionName(sql);
        sql.append(SET_SUBPARTITION).append("\n");
        appendTemplateOptions(sql);
    }

    void appendDropSql(StringBuilder sql) {
        appendPartitionName(sql);
        sql.append(SET_SUBPARTITION).append(")");
    }

    private void appendPartitionName(StringBuilder sql) {
        if (partitionName != null) {
            sql.append(" ALTER PARTITION ").append(partitionName);
        }
    }

    private void appendTemplateOptions(StringBuilder sbSQL) {
        for (var elem : subElements) {
            sbSQL.append("  ").append(elem).append(",").append("\n");
        }
        sbSQL.setLength(sbSQL.length() - 2);
        sbSQL.append("\n");
        sbSQL.append(")");
    }

    @Override
    public int hashCode() {
        JavaHasher hasher = new JavaHasher();
        computeHash(hasher);
        return hasher.getResult();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(partitionName);
        hasher.put(normalizedSubElements);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof GpPartitionTemplateContainer template) {
            return Objects.equals(partitionName, template.partitionName)
                    && Utils.setLikeEquals(normalizedSubElements, template.normalizedSubElements);
        }

        return false;
    }
}
