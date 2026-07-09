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
package org.pgcodekeeper.core.database.api.parser;

/**
 * Enumeration of parser listener modes for controlling SQL parsing behavior.
 * Determines how the parser processes SQL statements and what information is extracted.
 */
public enum ParserListenerMode {
    /**
     * Standard (full) parsing mode.
     * Used for the main structural analysis of the database. In this mode,
     * the parser extracts complete object metadata to build schemas, tables,
     * functions, and views for further comparison or deployment.
     */
    NORMAL,
    /**
     * Reference and dependency analysis mode.
     * In this mode, the parser focuses on extracting relationships, cross-references,
     * and dependencies between database entities (e.g., tables referenced inside views
     * or functions) to build an accurate dependency graph for migrations.
     */
    REF,
    /**
     * User script and migration script processing mode.
     * Designed for analyzing raw SQL files and deployment playbooks. It focuses on
     * splitting scripts into individual executable statements, validating syntax, and
     * checking basic constraints without full schema binding.
     */
    SCRIPT,
    /**
     * Single project file isolated parsing mode.
     * Processes only one file, creating mock {@code ISchema} instances for missing references.
     */
    SINGLE
}
