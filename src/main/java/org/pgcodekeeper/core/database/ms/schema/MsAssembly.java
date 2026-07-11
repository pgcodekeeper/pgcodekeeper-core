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

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Represents a Microsoft SQL assembly.
 * Assemblies allow running .NET code within SQL Server.
 */
public class MsAssembly extends MsAbstractStatement {

    private static final int PREVIEW_LENGTH = 256 * 4;

    private final List<String> binaries = new ArrayList<>();

    private String permission = "SAFE";
    private boolean isVisible = true;

    public MsAssembly(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.ASSEMBLY;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        getAssemblyFullSQL(false, script);
    }

    /**
     * Returns assembly definition without full binaries for preview purposes.
     *
     * @param settings the settings for SQL generation
     * @return the preview SQL string with truncated binaries
     */
    public String getPreview(ISettings settings) {
        SQLScript script = new SQLScript(settings, getSeparator());
        getAssemblyFullSQL(true, script);
        return script.getFullScript();
    }

    private void getAssemblyFullSQL(boolean isPreview, SQLScript script) {
        StringBuilder sql = new StringBuilder();

        sql.append("CREATE ASSEMBLY ").append(getQuotedName());
        if (owner != null) {
            sql.append("\nAUTHORIZATION ").append(quote(owner));
        }

        sql.append("\nFROM ");
        String bin = String.join(",\n", binaries);

        if (isPreview && bin.length() > PREVIEW_LENGTH) {
            sql.append(bin, 0, PREVIEW_LENGTH).append("\n<... PREVIEW TRIMMED>");
        } else {
            sql.append(bin);
        }

        sql.append("\nWITH PERMISSION_SET = ").append(permission);
        script.addStatement(sql);
        if (!isVisible) {
            script.addStatement(getAlterAssebly() + " WITH VISIBILITY = OFF");
        }

        appendPrivileges(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        MsAssembly newAss = (MsAssembly) newCondition;

        // https://docs.microsoft.com/ru-ru/sql/t-sql/statements/alter-assembly-transact-sql?view=sql-server-2016
        // TODO add/drop binary as file name. What is filename?
        if (!Objects.equals(newAss.binaries, binaries)) {
            return ObjectState.RECREATE;
        }

        appendAlterOwner(newAss, script);

        if (newAss.isVisible != isVisible) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterAssebly()).append(" WITH VISIBILITY = ").append(newAss.isVisible ? "ON" : "OFF");
            script.addStatement(sql);
        }

        if (!Objects.equals(newAss.permission, permission)) {
            StringBuilder sb = new StringBuilder();
            sb.append(getAlterAssebly()).append(" WITH PERMISSION_SET = ").append(newAss.permission);
            script.addStatement(sb);
        }

        return getObjectState(script, startSize);
    }

    private String getAlterAssebly() {
        return "ALTER ASSEMBLY " + getQuotedName();
    }

    @Override
    public void getDropSQL(SQLScript script, boolean optionExists) {
        StringBuilder dropSb = new StringBuilder();
        dropSb.append("DROP ASSEMBLY ");
        if (optionExists) {
            dropSb.append(IF_EXISTS);
        }
        dropSb.append(getQuotedName()).append(" WITH NO DEPENDENTS");
        script.addStatement(dropSb);
    }

    public void setPermission(final String permission) {
        this.permission = permission;
        resetHash();
    }

    /**
     * Adds a binary to this assembly.
     *
     * @param binary the binary data or file path
     */
    public void addBinary(final String binary) {
        binaries.add(binary);
        resetHash();
    }

    public void setVisible(final boolean isVisible) {
        this.isVisible = isVisible;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(isVisible);
        hasher.put(permission);
        hasher.put(binaries);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof MsAssembly as
                && super.compare(as)
                && Objects.equals(isVisible, as.isVisible)
                && Objects.equals(permission, as.permission)
                && binaries.equals(as.binaries);
    }

    @Override
    protected MsAssembly getCopy() {
        MsAssembly assDst = new MsAssembly(name);
        assDst.setVisible(isVisible);
        assDst.setPermission(permission);
        assDst.binaries.addAll(binaries);
        return assDst;
    }
}
