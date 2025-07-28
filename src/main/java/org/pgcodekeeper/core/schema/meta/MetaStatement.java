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
package org.pgcodekeeper.core.schema.meta;

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.IStatement;
import org.pgcodekeeper.core.schema.PgObjLocation;

import java.io.Serializable;

/**
 * Base class for all database metadata statement objects.
 * Provides common functionality for accessing object location, names, and comments.
 */
public class MetaStatement implements IStatement, Serializable {

    private static final long serialVersionUID = 5744769530265917940L;

    private final PgObjLocation object;
    private String comment = "";

    /**
     * Creates a new metadata statement with location information.
     *
     * @param object the object location information
     */
    public MetaStatement(PgObjLocation object) {
        this.object = object;
    }

    /**
     * Creates a new metadata statement from a generic column.
     *
     * @param column the generic column information
     */
    public MetaStatement(GenericColumn column) {
        this(new PgObjLocation.Builder().setObject(column).build());
    }

    @Override
    public String getName() {
        return getBareName();
    }

    @Override
    public String getBareName() {
        return object.getObjName();
    }

    @Override
    public DbObjType getStatementType() {
        return object.getType();
    }

    /**
     * Returns the generic column information for this statement.
     *
     * @return the generic column
     */
    public GenericColumn getGenericColumn() {
        return object.getObj();
    }

    /**
     * Returns the object location information.
     *
     * @return the object location
     */
    public PgObjLocation getObject() {
        return object;
    }

    @Override
    public String getQualifiedName() {
        return getGenericColumn().getQualifiedName();
    }

    @Override
    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * Returns the length of the object in the source file.
     *
     * @return the object length
     */
    public int getObjLength() {
        return object.getObjLength();
    }

    /**
     * Returns the offset of the object in the source file.
     *
     * @return the object offset
     */
    public int getOffset() {
        return object.getOffset();
    }

    /**
     * Returns the file path where this object is defined.
     *
     * @return the file path
     */
    public String getFilePath() {
        return object.getFilePath();
    }

    /**
     * Returns the line number where this object is defined.
     *
     * @return the line number
     */
    public int getLineNumber() {
        return object.getLineNumber();
    }

    /**
     * Returns the parent statement of this object.
     * This operation is not supported for metadata statements.
     *
     * @return never returns normally
     * @throws IllegalStateException always thrown as this operation is unsupported
     */
    @Override
    public MetaStatement getParent() {
        throw new IllegalStateException("Unsupported operation");
    }

    /**
     * Returns the database containing this statement.
     * This operation is not supported for metadata statements.
     *
     * @return never returns normally
     * @throws IllegalStateException always thrown as this operation is unsupported
     */
    @Override
    public AbstractDatabase getDatabase() {
        throw new IllegalStateException("Unsupported operation");
    }
}
