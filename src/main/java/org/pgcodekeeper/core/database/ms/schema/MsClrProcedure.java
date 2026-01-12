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

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.AbstractFunction;
import org.pgcodekeeper.core.database.api.schema.ArgMode;
import org.pgcodekeeper.core.database.base.schema.Argument;

import java.util.stream.Collectors;

/**
 * Represents a Microsoft SQL CLR stored procedure.
 * CLR procedures are implemented in managed code and executed within the SQL Server runtime.
 */
public final class MsClrProcedure extends MsAbstractClrFunction {

    /**
     * Creates a new Microsoft SQL CLR procedure.
     *
     * @param name the procedure name
     * @param assembly the assembly name containing the implementation
     * @param assemblyClass the class within the assembly
     * @param assemblyMethod the method within the class
     */
    public MsClrProcedure(String name, String assembly, String assemblyClass,
                          String assemblyMethod) {
        super(name, assembly, assemblyClass, assemblyMethod);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.PROCEDURE;
    }

    @Override
    protected void appendFunctionFullSQL(StringBuilder sbSQL, boolean isCreate) {
        sbSQL.append("SET QUOTED_IDENTIFIER OFF").append(GO).append('\n');
        sbSQL.append("SET ANSI_NULLS OFF").append(GO).append('\n');
        sbSQL.append(isCreate ? "CREATE" : "ALTER");
        sbSQL.append(" PROCEDURE ");
        sbSQL.append(getQualifiedName()).append('\n');

        if (!arguments.isEmpty()) {
            sbSQL.append(arguments.stream().map(this::getDeclaration)
                    .collect(Collectors.joining(",\n"))).append('\n');
        }

        if (!options.isEmpty()) {
            sbSQL.append("WITH ").append(String.join(", ", options)).append('\n');
        }

        sbSQL.append("AS\nEXTERNAL NAME ");
        sbSQL.append(getQuotedName(assembly)).append('.');
        sbSQL.append(getQuotedName(assemblyClass)).append('.');
        sbSQL.append(getQuotedName(assemblyMethod));
    }

    @Override
    public boolean needDrop(AbstractFunction newFunction) {
        return newFunction instanceof MsProcedure;
    }

    @Override
    public String getDeclaration(Argument arg) {
        final StringBuilder sbString = new StringBuilder();

        String name = arg.getName();
        if (name != null && !name.isEmpty()) {
            sbString.append(name);
            sbString.append(' ');
        }

        sbString.append(arg.getDataType());

        String def = arg.getDefaultExpression();

        if (def != null && !def.isEmpty()) {
            sbString.append(" = ");
            sbString.append(def);
        }

        ArgMode mode = arg.getMode();
        if (ArgMode.IN != mode) {
            sbString.append(' ').append(mode);
        }

        if (arg.isReadOnly()) {
            sbString.append(" READONLY");
        }

        return sbString.toString();
    }

    @Override
    protected MsClrProcedure getFunctionCopy() {
        return new MsClrProcedure(name, assembly, assemblyClass, assemblyMethod);
    }
}
