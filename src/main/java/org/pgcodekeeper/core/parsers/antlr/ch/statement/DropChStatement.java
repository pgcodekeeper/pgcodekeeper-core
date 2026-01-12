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
package org.pgcodekeeper.core.parsers.antlr.ch.statement;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.DangerStatement;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Drop_stmtContext;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Collections;
import java.util.List;

/**
 * Parser for ClickHouse DROP statements.
 * Handles dropping of databases, views, tables, functions, roles, and users.
 * Applies appropriate danger warnings for destructive operations like DROP TABLE.
 */
public final class DropChStatement extends ChParserAbstract {

    private final Drop_stmtContext ctx;

    /**
     * Creates a parser for ClickHouse DROP statements.
     *
     * @param ctx      the ANTLR parse tree context for the DROP statement
     * @param db       the ClickHouse database schema being processed
     * @param settings parsing configuration settings
     */
    public DropChStatement(Drop_stmtContext ctx, ChDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        var element = ctx.drop_element();
        if (element.DATABASE() != null) {
            addObjReference(Collections.singletonList(ctx.drop_element().name_with_cluster().identifier()),
                    DbObjType.SCHEMA, ACTION_DROP);
        } else if (element.VIEW() != null) {
            addObjReference(getIdentifiers(element.qualified_name()), DbObjType.VIEW, ACTION_DROP);
        } else if (element.TABLE() != null) {
            ObjectLocation loc = addObjReference(getIdentifiers(element.qualified_name()), DbObjType.TABLE, ACTION_DROP);
            loc.setWarning(DangerStatement.DROP_TABLE);
        } else if (element.FUNCTION() != null) {
            addObjReference(Collections.singletonList(ctx.drop_element().name_with_cluster().identifier()),
                    DbObjType.FUNCTION, ACTION_DROP);
        } else if (element.ROLE() != null) {
            for (var elementCtx : element.identifier_list().identifier()) {
                addObjReference(Collections.singletonList(elementCtx), DbObjType.ROLE, ACTION_DROP);
            }
        } else if (element.USER() != null) {
            for (var elementCtx : element.identifier_list().identifier()) {
                addObjReference(Collections.singletonList(elementCtx), DbObjType.USER, ACTION_DROP);
            }
        }
    }

    @Override
    protected String getStmtAction() {
        var element = ctx.drop_element();
        DbObjType type = null;
        List<ParserRuleContext> ids = null;
        if (element.DATABASE() != null) {
            type = DbObjType.SCHEMA;
            ids = Collections.singletonList(element.name_with_cluster().identifier());
        } else if (element.VIEW() != null) {
            type = DbObjType.VIEW;
            ids = getIdentifiers(element.qualified_name());
        } else if (element.TABLE() != null) {
            type = DbObjType.TABLE;
            ids = getIdentifiers(element.qualified_name());
        } else if (element.FUNCTION() != null) {
            type = DbObjType.FUNCTION;
            ids = Collections.singletonList(element.name_with_cluster().identifier());
        } else if (element.ROLE() != null) {
            return "DROP ROLE";
        } else if (element.USER() != null) {
            return "DROP USER";
        }

        return type != null ? getStrForStmtAction(ACTION_DROP, type, ids) : null;
    }

}