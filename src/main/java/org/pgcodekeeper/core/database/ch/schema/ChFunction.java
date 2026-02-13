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
package org.pgcodekeeper.core.database.ch.schema;

import java.util.*;
import java.util.stream.Collectors;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Represents a ClickHouse user-defined function.
 * ClickHouse functions are lambda expressions with parameters and a body.
 */
public final class ChFunction extends ChAbstractStatement {

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
        sb.append("CREATE FUNCTION ").append(getQuotedName()).append(" AS ");
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
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        var newFunction = (ChFunction) newCondition;
        if (!compareUnalterable(newFunction)) {
            return ObjectState.RECREATE;
        }
        return ObjectState.NOTHING;
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

    private boolean compareUnalterable(ChFunction newFunc) {
        return Objects.equals(body, newFunc.getBody())
                && arguments.equals(newFunc.arguments);
    }

    @Override
    protected AbstractStatement getCopy() {
        ChFunction copy = new ChFunction(name);
        for (Argument argSrc : arguments) {
            copy.addArgument(argSrc.getCopy());
        }
        copy.setBody(body);
        return copy;
    }
}