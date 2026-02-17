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
package org.pgcodekeeper.core.database.ms.schema;

import org.pgcodekeeper.core.database.api.schema.IArgument;
import org.pgcodekeeper.core.database.api.schema.IFunction;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.Argument;
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
public abstract class MsAbstractCommonFunction extends MsAbstractStatement implements IFunction {

    protected final List<Argument> arguments = new ArrayList<>();

    protected MsAbstractCommonFunction(String name) {
        super(name);
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
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        boolean isNeedDepcies = false;
        MsAbstractCommonFunction newFunction = (MsAbstractCommonFunction) newCondition;

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

    @Override
    public String getReturns() {
        // subclasses may override if needed
        return null;
    }

    @Override
    public boolean canDropBeforeCreate() {
        return true;
    }

    protected boolean isNeedDepcies(MsAbstractCommonFunction newFunction) {
        return !deps.equals(newFunction.deps);
    }

    protected abstract void appendFunctionFullSQL(StringBuilder sb, boolean isCreate);

    @Override
    public void setReturns(String returns) {
        throw new IllegalStateException();
    }

    @Override
    public Map<String, String> getReturnsColumns() {
        // subclasses may override if needed
        return Collections.emptyMap();
    }

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

    @Override
    public void computeHash(Hasher hasher) {
        hasher.putOrdered(arguments);
    }

    @Override
    public boolean compare(IStatement obj) {
        return obj instanceof MsAbstractCommonFunction func
                && super.compare(func)
                && compareUnalterable(func);
    }

    /**
     * Compares two objects whether they are equal. If both objects are of the same class, but they not equal just in
     * whitespace in function body, they are considered being equal.
     *
     * @param func object to be compared
     * @return true if {@code object} is PgFunction and the function code is the same when compared ignoring whitespace,
     * otherwise returns false
     */
    protected boolean compareUnalterable(MsAbstractCommonFunction func) {
        return arguments.equals(func.arguments);
    }

    @Override
    protected MsAbstractCommonFunction getCopy() {
        MsAbstractCommonFunction functionDst = getFunctionCopy();
        for (Argument argSrc : arguments) {
            functionDst.addArgument(argSrc.getCopy());
        }
        return functionDst;
    }

    protected abstract MsAbstractCommonFunction getFunctionCopy();
}
