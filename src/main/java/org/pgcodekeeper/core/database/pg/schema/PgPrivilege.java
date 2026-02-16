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
package org.pgcodekeeper.core.database.pg.schema;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.AbstractPrivilege;
import org.pgcodekeeper.core.database.pg.utils.PgDiffUtils;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Represents a database privilege (GRANT/REVOKE) for PostgreSQL database object.
 * Handles privilege operations including creation, dropping, and SQL generation.
 */
public class PgPrivilege extends AbstractPrivilege {

    /**
     * Creates a new privilege instance.
     *
     * @param state the privilege state (GRANT or REVOKE)
     * @param permission the permission type (e.g., SELECT, INSERT, ALL)
     * @param name the object name the privilege applies to
     * @param role the role receiving or losing the privilege
     * @param isGrantOption whether this privilege includes GRANT OPTION
     */
    public PgPrivilege(String state, String permission, String name, String role, boolean isGrantOption) {
        super(state, permission, name, role, isGrantOption);
    }

    /**
     * Appends default PostgreSQL privileges for a database object.
     *
     * @param newObj the database object to set default privileges for
     * @param script the script to append privileges to
     */
    public static void appendDefaultPostgresPrivileges(IStatement newObj, SQLScript script) {
        DbObjType type = newObj.getStatementType();
        boolean isFunctionOrTypeOrDomain = false;
        String typeName;
        switch (type) {
            case FUNCTION:
            case PROCEDURE:
            case TYPE:
            case DOMAIN:
                isFunctionOrTypeOrDomain = true;
                typeName = type.name();
                break;
            case AGGREGATE:
                isFunctionOrTypeOrDomain = true;
                typeName = DbObjType.FUNCTION.name();
                break;
            case FOREIGN_DATA_WRAPPER:
                typeName = "FOREIGN DATA WRAPPER";
                break;
            case SERVER:
                typeName = "FOREIGN SERVER";
                break;
            case VIEW:
                typeName = DbObjType.TABLE.name();
                break;
            case SCHEMA:
            case SEQUENCE:
            case TABLE:
                typeName = type.name();
                break;
            default:
                return;
        }

        StringBuilder sbName = new StringBuilder()
                .append(typeName)
                .append(' ');
        if (newObj instanceof PgAbstractFunction func) {
            sbName.append(PgDiffUtils.getQuotedName(func.getParent().getName())).append('.');
            func.appendFunctionSignature(sbName, false, true);
        } else {
            sbName.append(newObj.getQualifiedName());
        }
        String name = sbName.toString();

        // FUNCTION/PROCEDURE/AGGREGATE/TYPE/DOMAIN by default has "GRANT ALL to PUBLIC".
        // That's why for them set "GRANT ALL to PUBLIC".
        PgPrivilege priv = new PgPrivilege(isFunctionOrTypeOrDomain ? GRANT : REVOKE,
                "ALL", name, "PUBLIC", false);
        script.addStatement(priv.getCreationSQL());

        String owner = newObj.getOwner();
        if (owner == null) {
            return;
        }
        owner = PgDiffUtils.getQuotedName(owner);

        addDefPostgresPrivileges(script, REVOKE, name, owner);
        addDefPostgresPrivileges(script, GRANT, name, owner);
    }

    private static void addDefPostgresPrivileges(SQLScript script, String state, String name, String owner) {
        var priv = new PgPrivilege(state, "ALL", name, owner, false);
        script.addStatement(priv.getCreationSQL());
    }
}
