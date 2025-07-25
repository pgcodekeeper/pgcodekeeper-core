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
package org.pgcodekeeper.core.schema.ms;

import java.util.Objects;
import java.util.stream.Collectors;

import org.pgcodekeeper.core.MsDiffUtils;
import org.pgcodekeeper.core.hashers.Hasher;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.AbstractFunction;
import org.pgcodekeeper.core.schema.ArgMode;
import org.pgcodekeeper.core.schema.Argument;
import org.pgcodekeeper.core.schema.FuncTypes;

public final class MsClrFunction extends AbstractMsClrFunction {

    private String returns;
    private FuncTypes funcType = FuncTypes.SCALAR;

    public MsClrFunction(String name, String assembly, String assemblyClass,
            String assemblyMethod) {
        super(name, assembly, assemblyClass, assemblyMethod);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FUNCTION;
    }

    @Override
    public String getDeclaration(Argument arg, boolean includeDefaultValue,
            boolean includeArgName) {
        final StringBuilder sbString = new StringBuilder();
        sbString.append(arg.getName()).append(' ').append(arg.getDataType());

        String def = arg.getDefaultExpression();

        if (includeDefaultValue && def != null && !def.isEmpty()) {
            sbString.append(" = ");
            sbString.append(def);
        }

        ArgMode mode = arg.getMode();
        if (ArgMode.IN != mode) {
            sbString.append(' ').append(mode);
        }

        return sbString.toString();
    }

    @Override
    protected void appendFunctionFullSQL(StringBuilder sbSQL, boolean isCreate) {
        sbSQL.append("SET QUOTED_IDENTIFIER OFF").append(GO).append('\n');
        sbSQL.append("SET ANSI_NULLS OFF").append(GO).append('\n');
        sbSQL.append(isCreate ? "CREATE" : "ALTER");
        sbSQL.append(" FUNCTION ");
        sbSQL.append(getQualifiedName()).append('(');
        sbSQL.append(arguments.stream().map(arg -> getDeclaration(arg, true, true))
                .collect(Collectors.joining(", ")));
        sbSQL.append(')');

        sbSQL.append("\nRETURNS ").append(getReturns());

        if (!options.isEmpty()) {
            sbSQL.append("\nWITH ").append(String.join(", ", options)).append('\n');
        }

        sbSQL.append("AS\nEXTERNAL NAME ");
        sbSQL.append(MsDiffUtils.quoteName(assembly)).append('.');
        sbSQL.append(MsDiffUtils.quoteName(assemblyClass)).append('.');
        sbSQL.append(MsDiffUtils.quoteName(assemblyMethod));
    }

    public void setFuncType(FuncTypes funcType) {
        this.funcType = funcType;
        resetHash();
    }

    /**
     * @return the returns
     */
    @Override
    public String getReturns() {
        return returns;
    }

    /**
     * @param returns the returns to set
     */
    @Override
    public void setReturns(String returns) {
        this.returns = returns;
        resetHash();
    }

    @Override
    protected boolean compareUnalterable(AbstractFunction func) {
        return func instanceof MsClrFunction newClrFunc && super.compareUnalterable(func)
                && Objects.equals(returns, func.getReturns())
                && Objects.equals(funcType, newClrFunc.funcType);
    }

    @Override
    public boolean needDrop(AbstractFunction newFunction) {
        if (newFunction instanceof MsClrFunction newClrFunc) {
            return funcType != newClrFunc.funcType;
        }

        return true;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(getReturns());
        hasher.put(funcType);
    }

    @Override
    protected AbstractMsClrFunction getFunctionCopy() {
        MsClrFunction func = new MsClrFunction(name, assembly, assemblyClass, assemblyMethod);
        func.setFuncType(funcType);
        func.setReturns(returns);
        return func;
    }
}
