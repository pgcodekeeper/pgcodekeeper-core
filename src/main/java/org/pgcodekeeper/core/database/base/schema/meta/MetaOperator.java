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

import java.io.Serial;

import org.pgcodekeeper.core.database.api.schema.*;

/**
 * Represents a database operator metadata object.
 * Stores information about operator signatures including left and right argument types
 * and return type.
 */
public final class MetaOperator extends MetaStatement implements IOperator {

    @Serial
    private static final long serialVersionUID = -1610910953392795647L;

    private String left;
    private String right;
    private String returns;

    /**
     * Creates a new operator metadata object with location information.
     *
     * @param object the object location information
     */
    public MetaOperator(ObjectLocation object) {
        super(object);
    }

    /**
     * Creates a new operator metadata object.
     *
     * @param schemaName the schema name
     * @param name       the operator name
     */
    public MetaOperator(String schemaName, String name) {
        super(new ObjectReference(schemaName, name, DbObjType.OPERATOR));
    }

    @Override
    public String getName() {
        return getSignature();
    }

    /**
     * Returns the operator signature including argument types.
     *
     * @return the operator signature in format: name(leftType, rightType)
     */
    public String getSignature() {
        return getBareName() +
                '(' +
                (left == null ? "NONE" : left) +
                ", " +
                (right == null ? "NONE" : right) +
                ')';
    }

    @Override
    public String getRightArg() {
        return right;
    }

    @Override
    public String getLeftArg() {
        return left;
    }

    @Override
    public String getReturns() {
        return returns;
    }

    public void setLeftArg(String left) {
        this.left = left;
    }

    public void setRightArg(String right) {
        this.right = right;
    }

    @Override
    public void setReturns(String returns) {
        this.returns = returns;
    }

    /**
     * Returns the containing schema of this operator.
     * This operation is not supported for metadata operators.
     *
     * @return never returns normally
     * @throws IllegalStateException always thrown as this operation is unsupported
     */
    @Override
    public ISchema getContainingSchema() {
        throw new IllegalStateException("Unsupported operation");
    }

    /**
     * Returns the schema name of this operator.
     *
     * @return the schema name
     */
    @Override
    public String getSchemaName() {
        return getObject().getSchema();
    }
}
