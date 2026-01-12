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
package org.pgcodekeeper.core.database.base.schema;

import org.pgcodekeeper.core.database.api.schema.IArgument;
import org.pgcodekeeper.core.database.api.schema.IFunction;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for database functions, procedures, and aggregates.
 * Provides common functionality for callable database objects across different database types.
 */
public abstract class AbstractFunction extends AbstractStatement implements IFunction {

    protected final List<Argument> arguments = new ArrayList<>();

    protected AbstractFunction(String name) {
        super(name);
    }

    @Override
    public String getReturns() {
        // subclasses may override if needed
        return null;
    }

    @Override
    public void setReturns(String returns) {
        throw new IllegalStateException();
    }

    @Override
    public Map<String, String> getReturnsColumns() {
        // subclasses may override if needed
        return Collections.emptyMap();
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        appendFunctionFullSQL(sbSQL, true);
        script.addStatement(sbSQL);
        appendOwnerSQL(script);
        appendPrivileges(script);
        appendComments(script);
    }

    @Override
    public boolean canDropBeforeCreate() {
        return true;
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        boolean isNeedDepcies = false;
        AbstractFunction newFunction = (AbstractFunction) newCondition;

        if (!compareUnalterable(newFunction)) {
            if (needDrop(newFunction)) {
                return ObjectState.RECREATE;
            }

            isNeedDepcies = isNeedDepcies(newFunction);

            StringBuilder sbSQL = new StringBuilder();
            newFunction.appendFunctionFullSQL(sbSQL, false);
            script.addStatement(sbSQL);
        }

        appendAlterOwner(newFunction, script);
        alterPrivileges(newFunction, script);
        appendAlterComments(newFunction, script);

        return getObjectState(isNeedDepcies, script, startSize);
    }

    protected boolean isNeedDepcies(AbstractFunction newFunction) {
        return !deps.equals(newFunction.deps);
    }

    protected abstract void appendFunctionFullSQL(StringBuilder sb, boolean isCreate);

    /**
     * Getter for {@link #arguments}. List cannot be modified.
     */
    @Override
    public List<IArgument> getArguments() {
        return Collections.unmodifiableList(arguments);
    }

    /**
     * Adds an argument to this function.
     *
     * @param argument the argument to add
     */
    public void addArgument(final Argument argument) {
        arguments.add(argument);
        resetHash();
    }

    /**
     * Compares two objects whether they are equal. If both objects are of the same class, but they not equal just in
     * whitespace in function body, they are considered being equal.
     *
     * @param func object to be compared
     * @return true if {@code object} is PgFunction and the function code is the same when compared ignoring whitespace,
     * otherwise returns false
     */
    protected boolean compareUnalterable(AbstractFunction func) {
        return arguments.equals(func.arguments);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AbstractFunction func && super.compare(obj)) {
            return compareUnalterable(func);
        }

        return false;
    }

    /**
     * Determines whether this function needs to be dropped before creating the new version.
     *
     * @param newFunction the new function version to compare against
     * @return true if the function needs to be dropped and recreated
     */
    public abstract boolean needDrop(AbstractFunction newFunction);

    @Override
    public void computeHash(Hasher hasher) {
        hasher.putOrdered(arguments);
    }

    @Override
    public AbstractFunction shallowCopy() {
        AbstractFunction functionDst = getFunctionCopy();
        copyBaseFields(functionDst);
        for (Argument argSrc : arguments) {
            Argument argDst = new Argument(argSrc.getMode(), argSrc.getName(), argSrc.getDataType());
            argDst.setDefaultExpression(argSrc.getDefaultExpression());
            argDst.setReadOnly(argSrc.isReadOnly());
            functionDst.addArgument(argDst);
        }
        return functionDst;
    }

    protected abstract AbstractFunction getFunctionCopy();
}
