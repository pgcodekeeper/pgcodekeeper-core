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

import org.pgcodekeeper.core.database.base.schema.AbstractFunction;
import org.pgcodekeeper.core.database.base.schema.Argument;
import org.pgcodekeeper.core.hasher.Hasher;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Abstract base class for Microsoft SQL CLR (Common Language Runtime) functions.
 * Represents functions implemented in .NET assemblies that can be called from SQL.
 */
public abstract class MsAbstractClrFunction extends AbstractFunction implements IMsStatement {

    protected final List<String> options = new ArrayList<>();
    protected final String assembly;
    protected final String assemblyClass;
    protected final String assemblyMethod;

    protected MsAbstractClrFunction(String name, String assembly, String assemblyClass,
                                    String assemblyMethod) {
        super(name);
        this.assembly = assembly;
        this.assemblyClass = assemblyClass;
        this.assemblyMethod = assemblyMethod;
    }

    protected abstract String getDeclaration(Argument arg);

    /**
     * Adds a CLR function option.
     *
     * @param option the option to add
     */
    public void addOption(final String option) {
        options.add(option);
        resetHash();
    }

    @Override
    protected boolean compareUnalterable(AbstractFunction function) {
        if (function instanceof MsAbstractClrFunction func && super.compareUnalterable(function)) {
            return Objects.equals(assembly, func.assembly)
                    && Objects.equals(assemblyClass, func.assemblyClass)
                    && Objects.equals(assemblyMethod, func.assemblyMethod)
                    && options.equals(func.options);
        }

        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(options);
        hasher.put(assembly);
        hasher.put(assemblyClass);
        hasher.put(assemblyMethod);
    }

    @Override
    public MsAbstractClrFunction shallowCopy() {
        MsAbstractClrFunction functionDst = (MsAbstractClrFunction) super.shallowCopy();
        functionDst.options.addAll(options);
        return functionDst;
    }

    @Override
    protected boolean isNeedDepcies(AbstractFunction newFunction) {
        return true;
    }
}
