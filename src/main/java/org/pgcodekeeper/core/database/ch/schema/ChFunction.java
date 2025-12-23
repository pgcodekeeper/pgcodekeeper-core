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
package org.pgcodekeeper.core.database.ch.schema;

import org.pgcodekeeper.core.ChDiffUtils;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.Argument;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents a ClickHouse user-defined function.
 * ClickHouse functions are lambda expressions with parameters and a body.
 */
public final class ChFunction extends AbstractStatement implements IFunction, IChStatement {

    private String body;
    private final List<Argument> arguments = new ArrayList<>();

    /**
     * Creates a new ClickHouse function with the specified name.
     *
     * @param name the name of the function
     */
    public ChFunction(String name) {
        super(name);
    }

    /**
     * Returns the function body expression.
     *
     * @return the function body
     */
    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
        resetHash();
    }

    @Override
    public List<IArgument> getArguments() {
        return Collections.unmodifiableList(arguments);
    }

    /**
     * Adds an argument to this function.
     *
     * @param argument the argument to add
     */
    public void addArgument(Argument argument) {
        arguments.add(argument);
        resetHash();
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FUNCTION;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sb = new StringBuilder();
        sb.append("CREATE FUNCTION ").append(ChDiffUtils.getQuotedName(name)).append(" AS ");
        fillArgs(sb);
        sb.append(" -> ").append(body);
        script.addStatement(sb);
    }

    private void fillArgs(StringBuilder sb) {
        sb.append("(");
        sb.append(arguments.stream().map(Argument::getName).collect(Collectors.joining(", ")));
        sb.append(")");
    }

    @Override
    protected StringBuilder appendFullName(StringBuilder sb) {
        sb.append(getQualifiedName());
        return sb;
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        var newFunction = (ChFunction) newCondition;
        if (!compareUnalterable(newFunction)) {
            return ObjectState.RECREATE;
        }
        return ObjectState.NOTHING;
    }

    @Override
    public DatabaseType getDbType() {
        return DatabaseType.CH;
    }

    private boolean compareUnalterable(ChFunction newFunc) {
        return Objects.equals(body, newFunc.getBody())
                && arguments.equals(newFunc.arguments);
    }

    @Override
    public ChDatabase getDatabase() {
        return (ChDatabase) parent;
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(body);
        hasher.putOrdered(arguments);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        return obj instanceof ChFunction func && super.compare(func)
                && compareUnalterable(func);
    }

    @Override
    public AbstractStatement shallowCopy() {
        ChFunction copy = new ChFunction(name);
        copyBaseFields(copy);
        copy.arguments.addAll(arguments);
        copy.setBody(body);
        return copy;
    }

    @Override
    public String getReturns() {
        return null;
    }

    @Override
    public Map<String, String> getReturnsColumns() {
        return Collections.emptyMap();
    }

    @Override
    public void setReturns(String returns) {
        //unused
    }

    @Override
    public ISchema getContainingSchema() {
        return null;
    }
}