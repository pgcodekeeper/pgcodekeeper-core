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
package org.pgcodekeeper.core.database.base.schema.meta;

import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.Argument;

import java.io.Serial;
import java.util.*;

/**
 * Represents a database function metadata object.
 * Stores information about function signatures, arguments, return types,
 * and special properties like SETOF functions and aggregate order by clauses.
 */
public final class MetaFunction extends MetaStatement implements IFunction {

    @Serial
    private static final long serialVersionUID = -5384625158372975821L;

    private final String bareName;
    private List<IArgument> arguments;
    private transient String signatureCache;

    /**
     * Order by for aggregate functions
     */
    private List<Argument> orderBy;

    /**
     * Contains table's columns, if function returns table.
     */
    private Map<String, String> returnsColumns;

    /**
     * Function return type name, if null columns contains columns
     */
    private String returns;
    private boolean setof;

    /**
     * Creates a new function metadata object with location information.
     *
     * @param object   the object location information
     * @param bareName the bare function name without signature
     */
    public MetaFunction(ObjectLocation object, String bareName) {
        super(object);
        this.bareName = bareName;
    }

    /**
     * Creates a new function metadata object.
     *
     * @param schemaName the schema name
     * @param name       the function name (typically the signature)
     * @param bareName   the bare function name without signature
     */
    public MetaFunction(String schemaName, String name, String bareName) {
        super(new GenericColumn(schemaName, name, DbObjType.FUNCTION));
        this.bareName = bareName;
    }

    @Override
    public Map<String, String> getReturnsColumns() {
        return returnsColumns == null ? Collections.emptyMap() : Collections.unmodifiableMap(returnsColumns);
    }

    /**
     * Adds a column to the returns table for table-returning functions.
     *
     * @param name the column name
     * @param type the column type
     */
    public void addReturnsColumn(String name, String type) {
        if (returnsColumns == null) {
            returnsColumns = new LinkedHashMap<>();
        }
        returnsColumns.put(name, type);
    }

    @Override
    public List<IArgument> getArguments() {
        return arguments == null ? Collections.emptyList() : Collections.unmodifiableList(arguments);
    }

    /**
     * Adds an argument to this function.
     *
     * @param arg the argument to add
     */
    public void addArgument(final IArgument arg) {
        if (arguments == null) {
            arguments = new ArrayList<>();
        }
        arguments.add(arg);
    }

    /**
     * Returns whether this function returns a set of values.
     *
     * @return true if this is a SETOF function, false otherwise
     */
    public boolean isSetof() {
        return setof;
    }

    public void setSetof(final boolean setof) {
        this.setof = setof;
    }

    /**
     * Adds an ORDER BY argument for aggregate functions.
     *
     * @param type the order by argument
     */
    public void addOrderBy(final Argument type) {
        if (orderBy == null) {
            orderBy = new ArrayList<>();
        }
        orderBy.add(type);
    }

    @Override
    public String getReturns() {
        return returns;
    }

    @Override
    public void setReturns(String returns) {
        this.returns = returns;
    }

    /**
     * Alias for {@link #getSignature()} which provides a unique function ID.
     * <p>
     * Use {@link #getBareName()} to get just the function name.
     */
    @Override
    public String getName() {
        return getSignature();
    }

    @Override
    public String getBareName() {
        return bareName;
    }

    /**
     * Returns function signature. It consists of unquoted name and argument
     * data types.
     *
     * @return function signature
     */
    public String getSignature() {
        if (signatureCache == null) {
            signatureCache = getFunctionSignature();
        }
        return signatureCache;
    }

    private String getFunctionSignature() {
        StringBuilder sb = new StringBuilder();

        sb.append(PgDiffUtils.getQuotedName(getBareName())).append('(');
        boolean addComma = false;
        for (final IArgument argument : getArguments()) {
            ArgMode mode = argument.getMode();
            if (ArgMode.OUT == mode) {
                continue;
            }
            if (addComma) {
                sb.append(", ");
            }

            sb.append(argument.getDataType());
            addComma = true;
        }
        sb.append(')');

        return sb.toString();
    }

    /**
     * Returns the containing schema of this function.
     * This operation is not supported for metadata functions.
     *
     * @return never returns normally
     * @throws IllegalStateException always thrown as this operation is unsupported
     */
    @Override
    public ISchema getContainingSchema() {
        throw new IllegalStateException("Unsupported operation");
    }

    /**
     * Returns the schema name of this function.
     *
     * @return the schema name
     */
    @Override
    public String getSchemaName() {
        return getObject().getSchema();
    }
}
