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
package org.pgcodekeeper.core.database.api.schema;

import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Base interface for all database statements and objects.
 * Provides common functionality for identifying and accessing database objects.
 */
public interface IStatement {

    /**
     * Gets the name of this statement.
     *
     * @return the statement name
     */
    String getName();

    /**
     * Gets the type of this database object.
     *
     * @return the database object type
     */
    DbObjType getStatementType();

    /**
     * Gets the database that contains this statement.
     *
     * @return the containing database
     */
    default IDatabase getDatabase() {
        return (IDatabase) getParent();
    };

    /**
     * Gets the type name of this statement for SQL generation.
     *
     * @return the type name
     */
     default String getTypeName() {
        return getStatementType().getTypeName();
    }

    /**
     * Checks if this statement type supports ownership.
     *
     * @return true if the statement can have an owner
     */
    default boolean isOwned() {
        return false;
    }

    /**
     * Gets the parent statement that contains this statement.
     *
     * @return the parent statement, or null if this is a top-level object
     */
    IStatement getParent();

    /**
     * Gets the fully qualified name of this statement.
     *
     * @return the qualified name
     */
    String getQualifiedName();

    /**
     * Gets the comment associated with this statement.
     *
     * @return the comment, or null if no comment is set
     */
    String getComment();

    /**
     * Gets the bare name without qualifiers or arguments.
     *
     * @return the bare name
     */
    String getBareName();

    /**
     * @return an element in another db sharing the same name and location
     */
    IStatement getTwin(IDatabase newDb);

    /**
     * Performs {@link #shallowCopy()} on this object and all its children.
     *
     * @return a fully recursive copy of this statement.
     */
    IStatement deepCopy();

    /**
     * Copies all object properties into a new object and leaves all its children empty.
     *
     * @return shallow copy of a DB object.
     */
    IStatement shallowCopy();

    boolean compare(IStatement statement);

    /**
     * Adds a dependency to this statement.
     *
     * @param dependency the dependency to add
     */
    void addDependency(ObjectReference dependency);

    /**
     * @return all object dependencies
     */
    Set<ObjectReference> getDependencies();

    /**
     * Gets an unmodifiable set of privileges for this statement.
     *
     * @return unmodifiable set of privileges
     */
    Set<IPrivilege> getPrivileges();

    /**
     * Removes all privileges from this statement.
     */
    void clearPrivileges();

    /**
     * Gets the SQL representation of this statement with optional formatting.
     *
     * @param isFormatted whether to apply formatting to the SQL
     * @param settings    the settings to use for SQL generation and formatting
     * @return the SQL string representation of this statement
     */
    String getSQL(boolean isFormatted, ISettings settings);

    /**
     * Returns all subelements of current element
     */
    Stream<? extends IStatement> getChildren();

    /**
     * Returns all subtree elements
     */
    Stream<? extends IStatement> getDescendants();

    /**
     * Returns owner of the object
     */
    String getOwner();

    void setOwner(String owner);

    /**
     * Gets the location information for this statement.
     *
     * @return the location where this statement is defined
     */
    ObjectLocation getLocation();

    /**
     * @return true if this statement is from a library
     */
    boolean isLib();

    /**
     * Gets the name of the library this statement comes from.
     *
     * @return the library name, or null if not from a library
     */
    String getLibName();

    void setLibName(String libName);

    /**
     * Fill script with object changes and return change type
     *
     * @param newCondition new object state
     * @param script       script to collect changes
     * @return object change type
     */
    ObjectState appendAlterSQL(IStatement newCondition, SQLScript script);

    /**
     * Generates the SQL statements needed to create this database object.
     * This is an abstract method that must be implemented by subclasses
     * to provide the specific CREATE SQL for each object type.
     *
     * @param script the SQL script to append creation statements to
     */
    void getCreationSQL(SQLScript script);

    /**
     * Appends ALTER OWNER SQL statement to the script for this database object.
     *
     * @param script the SQL script to append the owner statement to
     * @throws IllegalArgumentException if database type is unsupported
     */
    void appendOwnerSQL(SQLScript script);

    /**
     * Generates DROP SQL for this statement.
     *
     * @param script         the SQL script to append the DROP statement to
     * @param generateExists whether to include "IF EXISTS" in the DROP statement
     */
    void getDropSQL(SQLScript script, boolean generateExists);

    /**
     * @return true if the statement can be dropped
     */
    boolean canDrop();

    String getSeparator();

    /**
     * Checks if this statement can be dropped before being recreated.
     * Override in subclasses that support drop-before-create behavior.
     *
     * @return true if the statement can be dropped before recreation
     */
    boolean canDropBeforeCreate();

     default ObjectReference toObjectReference() {
         return new ObjectReference(getName(), getStatementType());
     }

    /**
     * Formats string
     *
     * @param sql The source SQL text to format
     * @param offset Starting offset in the source text
     * @param length Length of text to format
     * @param formatConfiguration Formatting configuration options
     * @return formatted string
     */
    String formatSql(String sql, int offset, int length, IFormatConfiguration formatConfiguration);

    /**
     * @return the quoted name
     */
    default String getQuotedName() {
       return quote(getBareName());
    }

    /**
     * @param name string to quote
     * @return the quoted string
     */
    default String quote(String name) {
        return getQuoter().apply(name);
    }

    /**
     * @return a function that quotes name
     */
    UnaryOperator<String> getQuoter();

    /**
     * Returns sql command to rename the given object.
     *
     * @param newName the new name for given object
     * @return sql command to rename the given object
     */
    String getRenameCommand(String newName);

    void setLocation(ObjectLocation loc);

    boolean hasChildren();
}