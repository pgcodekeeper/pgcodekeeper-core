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
package org.pgcodekeeper.core.database.ms.jdbc;

import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;

public interface IMsJdbcReader {

    QueryBuilder MS_PRIVILEGES_JOIN_SUBSELECT = new QueryBuilder()
            .column("perm.state_desc AS sd")
            .column("perm.permission_name AS pn")
            .column("roleprinc.name AS r")
            .from("sys.database_principals roleprinc WITH (NOLOCK)")
            .join("JOIN sys.database_permissions perm WITH (NOLOCK) ON perm.grantee_principal_id = roleprinc.principal_id")
            .join("LEFT JOIN sys.columns col WITH (NOLOCK) ON col.object_id = perm.major_id AND col.column_id = perm.minor_id")
            .postAction("FOR XML RAW, ROOT");

    default void addMsPrivilegesPart(QueryBuilder builder) {
        var subSelect = formatMsPrivileges(MS_PRIVILEGES_JOIN_SUBSELECT.copy());
        builder
                .column("aa.acl")
                .join("CROSS APPLY", subSelect, "aa (acl)");
    }

    default QueryBuilder formatMsPrivileges(QueryBuilder privileges) {
        return privileges
                .column("col.name AS c")
                .where("major_id = res.object_id")
                .where("perm.class = 1");
    }

    default void addMsOwnerPart(QueryBuilder builder) {
        addMsOwnerPart("res.principal_id", builder);
    }

    default void addMsOwnerPart(String field, QueryBuilder builder) {
        builder.column("p.name AS owner");
        builder.join(getMsOwnerPartJoin() + "sys.database_principals p WITH (NOLOCK) ON p.principal_id=" + field);
    }

    default String getMsOwnerPartJoin() {
        return "LEFT JOIN ";
    }
}
