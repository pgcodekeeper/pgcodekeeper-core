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
package org.pgcodekeeper.core.settings;

import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.formatter.FormatConfiguration;
import org.pgcodekeeper.core.model.difftree.DbObjType;

import java.util.Collection;

/**
 * Interface defining configuration settings for database comparison and migration operations.
 * Provides access to database type, formatting options, file paths, and various behavioral flags
 * that control how database schema comparisons and migrations are performed.
 */
public interface ISettings {

    /**
     * Gets the database type for operations.
     *
     * @return the database type
     */
    DatabaseType getDbType();

    /**
     * Checks if concurrent mode is enabled.
     *
     * @return true if concurrent mode is enabled
     */
    boolean isConcurrentlyMode();

    /**
     * Checks if migration scripts should be wrapped in transactions.
     *
     * @return true if transaction wrapping is enabled
     */
    boolean isAddTransaction();

    /**
     * Checks if existence checks should be generated in migration scripts.
     *
     * @return true if existence checks are enabled
     */
    boolean isGenerateExists();

    /**
     * Checks if constraints should be generated with NOT VALID option.
     *
     * @return true if NOT VALID constraints are enabled
     */
    boolean isGenerateConstraintNotValid();

    /**
     * Checks if existence check DO blocks should be generated.
     *
     * @return true if DO block generation is enabled
     */
    boolean isGenerateExistDoBlock();

    /**
     * Checks if USING clauses should be printed in generated SQL.
     *
     * @return true if USING clause printing is enabled
     */
    boolean isPrintUsing();

    /**
     * Checks if newlines should be preserved in generated SQL.
     *
     * @return true if newline preservation is enabled
     */
    boolean isKeepNewlines();

    /**
     * Checks if comments should be moved to the end of generated scripts.
     *
     * @return true if comments are moved to end
     */
    boolean isCommentsToEnd();

    /**
     * Checks if object code should be automatically formatted.
     *
     * @return true if auto-formatting is enabled
     */
    boolean isAutoFormatObjectCode();

    /**
     * Checks if privileges should be ignored during comparison.
     *
     * @return true if privileges are ignored
     */
    boolean isIgnorePrivileges();

    /**
     * Checks if column order should be ignored during comparison.
     *
     * @return true if column order is ignored
     */
    boolean isIgnoreColumnOrder();

    /**
     * Checks if function body dependencies analysis is enabled.
     *
     * @return true if function body dependencies are enabled
     */
    boolean isEnableFunctionBodiesDependencies();

    /**
     * Checks if data movement mode is enabled for migrations.
     *
     * @return true if data movement mode is enabled
     */
    boolean isDataMovementMode();

    /**
     * Checks if objects should be dropped before creating in migrations.
     *
     * @return true if drop-before-create is enabled
     */
    boolean isDropBeforeCreate();

    /**
     * Checks if migration should stop when encountering not-allowed operations.
     *
     * @return true if stop-on-not-allowed is enabled
     */
    boolean isStopNotAllowed();

    /**
     * Checks if only selected objects should be processed.
     *
     * @return true if selected-only mode is enabled
     */
    boolean isSelectedOnly();

    /**
     * Checks if concurrent modifications should be ignored.
     *
     * @return true if concurrent modifications are ignored
     */
    boolean isIgnoreConcurrentModification();

    /**
     * Checks if view definitions should be simplified.
     *
     * @return true if view simplification is enabled
     */
    boolean isSimplifyView();

    /**
     * Checks if function body checking should be disabled.
     *
     * @return true if function body checking is disabled
     */
    boolean isDisableCheckFunctionBodies();

    /**
     * Gets the input character encoding name.
     *
     * @return the character encoding name
     */
    String getInCharsetName();

    /**
     * Gets the time zone setting.
     *
     * @return the time zone string
     */
    String getTimeZone();

    /**
     * Gets the format configuration for code formatting.
     *
     * @return the format configuration instance
     */
    FormatConfiguration getFormatConfiguration();

    /**
     * Gets the collection of allowed database object types for processing.
     *
     * @return collection of allowed object types
     */
    Collection<DbObjType> getAllowedTypes();

    /**
     * Gets the collection of pre-processing file paths.
     *
     * @return collection of pre-processing file paths
     */
    Collection<String> getPreFilePath();

    /**
     * Gets the collection of post-processing file paths.
     *
     * @return collection of post-processing file paths
     */
    Collection<String> getPostFilePath();

    /**
     * Creates a copy of this settings instance.
     *
     * @return a new settings instance with the same configuration
     */
    ISettings copy();

    /**
     * Sets whether privileges should be ignored during comparison.
     *
     * @param ignorePrivileges true to ignore privileges
     */
    void setIgnorePrivileges(boolean ignorePrivileges);
}
