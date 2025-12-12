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
package org.pgcodekeeper.core.parsers.antlr.ms.statement;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Alter_assemblyContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Assembly_optionContext;
import org.pgcodekeeper.core.database.ms.schema.MsAssembly;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Collections;
import java.util.List;

/**
 * Parser for Microsoft SQL ALTER ASSEMBLY statements.
 * Handles assembly modifications including visibility settings and other assembly options.
 */
public final class AlterMsAssembly extends MsParserAbstract {

    private final Alter_assemblyContext ctx;

    /**
     * Creates a parser for Microsoft SQL ALTER ASSEMBLY statements.
     *
     * @param ctx      the ANTLR parse tree context for the ALTER ASSEMBLY statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public AlterMsAssembly(Alter_assemblyContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        MsAssembly assembly = getSafe(MsDatabase::getAssembly, db, ctx.name);
        addObjReference(Collections.singletonList(ctx.name), DbObjType.ASSEMBLY, ACTION_ALTER);

        List<Assembly_optionContext> options = ctx.assembly_option();
        if (options != null) {
            for (Assembly_optionContext option : options) {
                if (option.VISIBILITY() != null) {
                    doSafe(MsAssembly::setVisible, assembly, option.on_off().ON() != null);
                }
            }
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_ALTER, DbObjType.ASSEMBLY, ctx.name);
    }
}
