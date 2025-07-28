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
package org.pgcodekeeper.core.schema;

/**
 * Interface for partitioned tables.
 * Defines functionality for table partitioning including partition bounds and parent table references.
 */
public interface IPartitionTable extends IStatement {
    /**
     * Gets the partition bounds specification for this partition.
     *
     * @return the partition bounds as SQL string
     */
    String getPartitionBounds();

    /**
     * Gets the name of the parent table that this partition belongs to.
     *
     * @return the parent table name
     */
    String getParentTable();
}
