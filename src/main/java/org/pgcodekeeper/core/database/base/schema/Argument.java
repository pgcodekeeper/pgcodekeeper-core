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
package org.pgcodekeeper.core.database.base.schema;

import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.database.api.schema.ArgMode;
import org.pgcodekeeper.core.database.api.schema.IArgument;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.hasher.IHashable;
import org.pgcodekeeper.core.hasher.JavaHasher;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a function argument with its mode, name, data type, and default value.
 * Used for storing parameter information for functions, procedures, and aggregates
 * across different database types.
 */
public class Argument implements IArgument, Serializable, IHashable {

    @Serial
    private static final long serialVersionUID = -2427789431834197575L;

    private final ArgMode mode;
    private final String name;
    private final String dataType;
    private String defaultExpression;
    private boolean isReadOnly;

    public Argument(String name, String dataType) {
        this(ArgMode.IN, name, dataType);
    }

    public Argument(ArgMode mode, String name, String dataType) {
        this.mode = mode;
        this.name = (name != null && name.isEmpty()) ? null : name;
        this.dataType = dataType;
    }

    public String getDataType() {
        return dataType;
    }

    public String getDefaultExpression() {
        return defaultExpression;
    }

    public void setDefaultExpression(final String defaultExpression) {
        this.defaultExpression = defaultExpression;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public void setReadOnly(final boolean isReadOnly) {
        this.isReadOnly = isReadOnly;
    }

    public ArgMode getMode() {
        return mode;
    }

    public String getName() {
        return name;
    }

    /**
     * Appends the argument declaration to a StringBuilder.
     *
     * @param sbString            the StringBuilder to append to
     * @param includeDefaultValue whether to include the default value
     * @param includeArgName      whether to include the argument name
     */
    public void appendDeclaration(StringBuilder sbString,
                                  boolean includeDefaultValue, boolean includeArgName) {
        if (includeArgName) {
            if (mode != ArgMode.IN) {
                sbString.append(mode);
                sbString.append(' ');
            }

            if (name != null && !name.isEmpty()) {
                sbString.append(PgDiffUtils.getQuotedName(name));
                sbString.append(' ');
            }
        }

        sbString.append(dataType);

        String def = defaultExpression;

        if (includeDefaultValue && def != null && !def.isEmpty()) {
            sbString.append(" = ");
            sbString.append(def);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof Argument arg) {
            return Objects.equals(dataType, arg.dataType)
                    && Objects.equals(defaultExpression, arg.defaultExpression)
                    && mode == arg.mode
                    && isReadOnly == arg.isReadOnly
                    && Objects.equals(name, arg.name);
        }

        return false;
    }

    @Override
    public int hashCode() {
        JavaHasher hasher = new JavaHasher();
        computeHash(hasher);
        return hasher.getResult();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(dataType);
        hasher.put(defaultExpression);
        hasher.put(mode);
        hasher.put(name);
        hasher.put(isReadOnly);
    }
}
