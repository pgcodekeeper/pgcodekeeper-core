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
package org.pgcodekeeper.core.database.base.schema.meta;

import java.io.*;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Base class for all database metadata statement objects.
 * Provides common functionality for accessing object location, names, and comments.
 */
public class MetaStatement implements IStatement, Serializable {

    @Serial
    private static final long serialVersionUID = 8504077236938059732L;

    private final ObjectLocation object;
    private String comment = "";

    /**
     * Creates a new metadata statement with location information.
     *
     * @param object the object location information
     */
    public MetaStatement(ObjectLocation object) {
        this.object = object;
    }

    /**
     * Creates a new metadata statement from object reference.
     *
     * @param reference object reference
     */
    public MetaStatement(ObjectReference reference) {
        this(new ObjectLocation.Builder().setReference(reference).build());
    }

    @Override
    public String getName() {
        return getBareName();
    }

    @Override
    public String getBareName() {
        return object.getName();
    }

    @Override
    public DbObjType getStatementType() {
        return object.getType();
    }

    /**
     * @return object reference for this statement
     */
    public ObjectReference getObjectReference() {
        return object.getObjectReference();
    }

    /**
     * Returns the object location information.
     *
     * @return the object location
     */
    public ObjectLocation getObject() {
        return object;
    }

    @Override
    public String getQualifiedName() {
        return getObjectReference().toString();
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

    @Override
    public MetaStatement getParent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IDatabase getDatabase() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IStatement getTwin(IDatabase newDb) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IStatement deepCopy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IStatement shallowCopy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compare(IStatement statement) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addDependency(ObjectReference dependency) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ObjectReference> getDependencies() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<IPrivilege> getPrivileges() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSQL(boolean isFormatted, ISettings settings) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<? extends IStatement> getChildren() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<? extends IStatement> getDescendants() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getOwner() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendOwnerSQL(SQLScript script) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getDropSQL(SQLScript script, boolean generateExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectLocation getLocation() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLib() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getLibName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canDrop() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canDropBeforeCreate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String formatSql(String sql, int offset, int length, IFormatConfiguration formatConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UnaryOperator<String> getQuoter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRenameCommand(String newName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOwner(String owner) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLibName(String libName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLocation(ObjectLocation loc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasChildren() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSeparator() {
        throw new UnsupportedOperationException();
    }
}
