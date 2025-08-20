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

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Assembly_permissionContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Create_assemblyContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.ExpressionContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.IdContext;
import org.pgcodekeeper.core.schema.ms.MsAssembly;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Parser for Microsoft SQL CREATE ASSEMBLY statements.
 * Handles assembly creation including binary data, owner settings, and permission levels
 * with proper formatting of binary hexadecimal data.
 */
public final class CreateMsAssembly extends MsParserAbstract {

    private static final Pattern BINARY_NEWLINE = Pattern.compile("\\\\\\r?\\n");
    private static final int BINARY_LINE_LENGTH = 256;

    private final Create_assemblyContext ctx;

    /**
     * Creates a parser for Microsoft SQL CREATE ASSEMBLY statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE ASSEMBLY statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateMsAssembly(Create_assemblyContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        IdContext nameCtx = ctx.assembly_name;
        MsAssembly ass = new MsAssembly(ctx.assembly_name.getText());
        IdContext owner = ctx.owner_name;
        if (owner != null && !settings.isIgnorePrivileges()) {
            ass.setOwner(owner.getText());
        }

        for (ExpressionContext binary : ctx.expression()) {
            ass.addBinary(formatBinary(getFullCtxText(binary)));
        }

        Assembly_permissionContext permission = ctx.assembly_permission();
        if (permission != null) {
            ass.setPermission(getFullCtxText(permission).toUpperCase(Locale.ROOT));
        }

        addSafe(db, ass, List.of(nameCtx));
    }

    /**
     * Formats binary hexadecimal data for assembly storage.
     * Removes newlines from input and reformats with proper line breaks for readability.
     *
     * @param hex the hexadecimal string to format
     * @return formatted hexadecimal string with proper line breaks
     */
    public static String formatBinary(String hex) {
        if (!hex.startsWith("0x")) {
            return hex;
        }
        hex = hex.toLowerCase(Locale.ROOT);
        if (hex.indexOf('\\') != -1) {
            hex = BINARY_NEWLINE.matcher(hex).replaceAll("");
        }
        StringBuilder sb = new StringBuilder(hex.length() + hex.length() / (BINARY_LINE_LENGTH / 2));
        sb.append("0x");
        int i = 2;
        do {
            int end = i + BINARY_LINE_LENGTH;
            if (end > hex.length()) {
                end = hex.length();
            }
            sb.append(hex, i, end)
                    .append("\\\n");
            i = end;
        } while (i < hex.length());
        // remove trailing newline
        sb.setLength(sb.length() - 2);
        return sb.toString();
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.ASSEMBLY, ctx.assembly_name);
    }
}
