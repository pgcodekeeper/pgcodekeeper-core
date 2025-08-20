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
package org.pgcodekeeper.core.parsers.antlr.base.statement;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.pgcodekeeper.core.*;
import org.pgcodekeeper.core.exception.MisplacedObjectException;
import org.pgcodekeeper.core.loader.ParserListenerMode;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.model.exporter.ModelExporter;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.exception.UnresolvedReferenceException;
import org.pgcodekeeper.core.schema.*;
import org.pgcodekeeper.core.schema.PgObjLocation.LocationType;
import org.pgcodekeeper.core.settings.ISettings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Abstract base class for database object parsers that provides common parsing functionality
 * and utilities for working with ANTLR-generated parse trees.
 */
public abstract class ParserAbstract<S extends AbstractDatabase> {

    private static final String SCHEMA_ERROR = "Object must be schema qualified: ";
    private static final String LOCATION_ERROR = "The object {0} must be defined in the file: {1}";

    protected static final String ACTION_CREATE = "CREATE";
    protected static final String ACTION_ALTER = "ALTER";
    protected static final String ACTION_DROP = "DROP";
    protected static final String ACTION_UPDATE = "UPDATE";
    protected static final String ACTION_INSERT = "INSERT";
    protected static final String ACTION_DELETE = "DELETE";
    protected static final String ACTION_MERGE = "MERGE";
    protected static final String ACTION_COMMENT = "COMMENT";

    protected final S db;
    protected final ISettings settings;

    private boolean refMode;
    protected String fileName;

    protected ParserAbstract(S db, ISettings settings) {
        this.db = db;
        this.settings = settings;
    }

    /**
     * Parses a database object from the given parse tree context.
     *
     * @param fileName the source file being parsed
     * @param mode     the parsing mode (REF, SCRIPT, etc.)
     * @param ctx      the ANTLR parse tree context to parse from
     */
    public void parseObject(String fileName, ParserListenerMode mode, ParserRuleContext ctx) {
        this.fileName = fileName;
        refMode = ParserListenerMode.REF == mode;
        if (ParserListenerMode.SCRIPT == mode) {
            fillQueryLocation(ctx);
        } else {
            parseObject();
        }
    }

    protected boolean isRefMode() {
        return refMode;
    }

    /**
     * Parses a database object from the current context. Must be implemented
     * by concrete subclasses to handle specific object types.
     */
    public abstract void parseObject();

    /**
     * Extracts raw text from context
     *
     * @param ctx context
     * @return raw string
     */
    public static String getFullCtxText(ParserRuleContext ctx) {
        return getFullCtxText(ctx, ctx);
    }

    /**
     * Extracts raw text from context with new lines check according to current settings
     *
     * @param ctx context
     * @return raw string
     */
    protected String getFullCtxTextWithCheckNewLines(ParserRuleContext ctx) {
        String text = getFullCtxText(ctx, ctx);
        return checkNewLines(text);
    }

    protected String checkNewLines(String text) {
        return Utils.checkNewLines(text, settings.isKeepNewlines());
    }


    /**
     * Extracts raw text from list of IdentifierContext
     *
     * @param ids list of IdentifierContext
     *            context
     * @return raw string
     */
    protected String getFullCtxText(List<? extends ParserRuleContext> ids) {
        return getFullCtxText(ids.get(0), ids.get(ids.size() - 1));
    }

    /**
     * Extracts raw text between two parse tree contexts.
     *
     * @param start the starting context
     * @param end   the ending context
     * @return the text between the contexts
     */
    public static String getFullCtxText(ParserRuleContext start, ParserRuleContext end) {
        return getFullCtxText(start.getStart(), end.getStop());
    }

    /**
     * Extracts raw text between two tokens.
     *
     * @param start the starting token
     * @param end   the ending token
     * @return the text between the tokens
     */
    public static String getFullCtxText(Token start, Token end) {
        if (start.getStartIndex() > end.getStopIndex()) { // safe to use code point methods
            // broken ctx
            return "";
        }
        return start.getInputStream().getText(
                Interval.of(start.getStartIndex(), end.getStopIndex()));
    }

    protected String getHiddenLeftCtxText(ParserRuleContext ctx, CommonTokenStream stream) {
        List<Token> startTokens = stream.getHiddenTokensToLeft(ctx.getStart().getTokenIndex());
        if (startTokens != null) {
            return ctx.getStart().getInputStream().getText(Interval.of(
                    startTokens.get(0).getStartIndex(),
                    ctx.getStart().getStartIndex() - 1));
        }

        return "";
    }

    protected String getExpressionText(ParserRuleContext def, CommonTokenStream stream) {
        String expression = getFullCtxTextWithCheckNewLines(def);
        String whitespace = getHiddenLeftCtxText(def, stream);
        int newline = whitespace.indexOf('\n');
        return newline != -1 ? (whitespace.substring(newline) + expression) : expression;
    }

    /**
     * Parses an argument mode from a parse tree context.
     *
     * @param mode the mode parse tree context
     * @return the parsed ArgMode
     */
    public static ArgMode parseArgMode(ParserRuleContext mode) {
        if (mode == null) {
            return ArgMode.IN;
        }

        return ArgMode.of(mode.getText());
    }

    protected PgObjLocation addObjReference(List<? extends ParserRuleContext> ids,
                                            DbObjType type, String action, String signature) {
        PgObjLocation loc = getLocation(ids, type, action, false, signature, LocationType.REFERENCE);
        if (loc != null) {
            db.addReference(fileName, loc);
        }

        return loc;
    }

    protected PgObjLocation addObjReference(List<? extends ParserRuleContext> ids,
                                            DbObjType type, String action) {
        return addObjReference(ids, type, action, null);
    }

    /**
     * Safely retrieves a database statement with validation.
     * <p>
     * Note: Always returns null if parser is in ref mode.
     *
     * @param <T>       the container statement type
     * @param <R>       the child statement type
     * @param getter    the getter function to retrieve the child
     * @param container the containing statement
     * @param ctx       the parse tree context
     * @return the found statement or null if parser is in ref mode
     * @throws UnresolvedReferenceException if statement not found
     */
    public <T extends IStatement, R extends IStatement> R getSafe(
            BiFunction<T, String, R> getter, T container, ParserRuleContext ctx) {
        return getSafe(getter, container, ctx.getText(), ctx.start);
    }

    /**
     * Safely retrieves a database statement by name with validation.
     * <p>
     * Note: Always returns null if parser is in ref mode.
     *
     * @param <T>       the container statement type
     * @param <R>       the child statement type
     * @param getter    the getter function to retrieve the child
     * @param container the containing statement
     * @param name      the name of the statement to find
     * @param errToken  the token for error reporting
     * @return the found statement
     * @throws UnresolvedReferenceException if statement not found
     */
    public <T extends IStatement, R extends IStatement> R getSafe(BiFunction<T, String, R> getter,
                                                                  T container, String name, Token errToken) {
        if (isRefMode()) {
            return null;
        }
        R statement = getter.apply(container, name);
        if (statement == null) {
            throw new UnresolvedReferenceException("Cannot find object in database: "
                    + name, errToken);
        }

        checkLocation((PgStatement) statement, errToken);

        return statement;
    }

    protected void addSafe(IStatementContainer parent, PgStatement child,
                           List<? extends ParserRuleContext> ids) {
        addSafe(parent, child, ids, null);
    }

    protected void addSafe(IStatementContainer parent, PgStatement child,
                           List<? extends ParserRuleContext> ids, String signature) {
        doSafe(IStatementContainer::addChild, parent, child);
        PgObjLocation loc = getLocation(ids, child.getStatementType(),
                ACTION_CREATE, false, signature, LocationType.DEFINITION);
        if (loc != null) {
            child.setLocation(loc);
            db.addReference(fileName, loc);
        }

        // TODO move to beginning of the method later
        checkLocation(child, QNameParser.getFirstNameCtx(ids).getStart());
    }

    protected void checkLocation(PgStatement statement, Token errToken) {
        if (isRefMode() || fileName == null) {
            return;
        }

        String filePath = ModelExporter.getRelativeFilePath(statement).toString();
        if (!Utils.endsWithIgnoreCase(fileName, filePath)
                && isInProject(statement.getDbType())) {
            throw new MisplacedObjectException(MessageFormat.format(LOCATION_ERROR,
                    statement.getBareName(), filePath), errToken);
        }
    }

    private boolean isInProject(DatabaseType dbType) {
        List<String> dirs = WorkDirs.getDirectoryNames(dbType);
        Path parent = Paths.get(fileName).toAbsolutePath().getParent();
        while (true) {
            Path folder = parent.getFileName();
            parent = parent.getParent();

            // file name for root is null
            if (folder == null || parent == null) {
                return false;
            }

            // if we find the project directory, then we check the marker at the level above
            if (dirs.contains(folder.toString())
                    && Files.exists(parent.resolve(Consts.FILENAME_WORKING_DIR_MARKER))) {
                return true;
            }
        }
    }

    protected PgObjLocation getLocation(List<? extends ParserRuleContext> ids,
                                        DbObjType type, String action, boolean isDep, String signature,
                                        LocationType locationType) {
        ParserRuleContext nameCtx = QNameParser.getFirstNameCtx(ids);
        switch (type) {
            case ASSEMBLY:
            case EXTENSION:
            case EVENT_TRIGGER:
            case FOREIGN_DATA_WRAPPER:
            case SERVER:
            case SCHEMA:
            case ROLE:
            case USER:
            case DATABASE:
                return buildLocation(nameCtx, action, locationType, new GenericColumn(nameCtx.getText(), type));
            default:
                break;
        }

        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);
        String schemaName;
        if (schemaCtx != null) {
            addObjReference(List.of(schemaCtx), DbObjType.SCHEMA, null);
            schemaName = schemaCtx.getText();
        } else if (refMode && !isDep) {
            schemaName = null;
        } else if (refMode || isDep) {
            return null;
        } else {
            throw new UnresolvedReferenceException(SCHEMA_ERROR + getFullCtxText(nameCtx),
                    nameCtx.getStart());
        }

        String name = nameCtx.getText();
        if (signature != null) {
            name = PgDiffUtils.getQuotedName(name) + signature;
        }
        return switch (type) {
            case DOMAIN, FTS_CONFIGURATION, FTS_DICTIONARY, FTS_PARSER, FTS_TEMPLATE, OPERATOR, SEQUENCE, TABLE,
                 DICTIONARY, TYPE, VIEW, INDEX, STATISTICS, COLLATION, FUNCTION, PROCEDURE, AGGREGATE ->
                    buildLocation(nameCtx, action, locationType,
                            new GenericColumn(schemaName, name, type));
            case CONSTRAINT, TRIGGER, RULE, POLICY, COLUMN -> buildLocation(nameCtx, action, locationType,
                    new GenericColumn(schemaName, QNameParser.getSecondName(ids), name, type));
            default -> null;
        };
    }

    protected PgObjLocation buildLocation(ParserRuleContext nameCtx, String action, LocationType locationType,
                                          GenericColumn object) {
        return new PgObjLocation.Builder()
                .setFilePath(fileName)
                .setCtx(nameCtx)
                .setObject(object)
                .setAction(action)
                .setLocationType(locationType)
                .build();
    }

    protected <T extends IStatement, U> void doSafe(BiConsumer<T, U> adder,
                                                    T statement, U object) {
        if (!refMode) {
            adder.accept(statement, object);
        }
    }

    protected void addDepSafe(PgStatement st, List<? extends ParserRuleContext> ids, DbObjType type) {
        addDepSafe(st, ids, type, null);
    }

    protected void addDepSafe(PgStatement st, List<? extends ParserRuleContext> ids, DbObjType type, String signature) {
        PgObjLocation loc = getLocation(ids, type, null, true, signature, LocationType.REFERENCE);
        if (loc != null && !Utils.isSystemSchema(loc.getSchema(), getDbType())) {
            if (!refMode) {
                st.addDep(loc.getObj());
            }
            db.addReference(fileName, loc);
        }
    }

    protected final DatabaseType getDbType() {
        return db.getDbType();
    }

    protected AbstractSchema getSchemaSafe(List<? extends ParserRuleContext> ids) {
        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);

        if (schemaCtx == null) {
            if (refMode) {
                return null;
            }
            throw new UnresolvedReferenceException(SCHEMA_ERROR + QNameParser.getFirstName(ids),
                    QNameParser.getFirstNameCtx(ids).start);
        }

        AbstractSchema schema = db.getSchema(schemaCtx.getText());

        if (schema != null || refMode) {
            return schema;
        }

        ParserRuleContext firstNameCtx = QNameParser.getFirstNameCtx(ids);
        throw new UnresolvedReferenceException("Schema not found for " +
                getFullCtxText(ids), firstNameCtx.start);
    }

    protected String getSchemaNameSafe(List<? extends ParserRuleContext> ids) {
        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);
        if (schemaCtx != null) {
            return schemaCtx.getText();
        }
        if (refMode) {
            return null;
        }

        throw new UnresolvedReferenceException(SCHEMA_ERROR + QNameParser.getFirstName(ids),
                QNameParser.getFirstNameCtx(ids).start);
    }

    /**
     * Processes option parameters into key-value pairs.
     *
     * @param options    the option strings to parse
     * @param c          the consumer to receive each key-value pair
     * @param isToast    whether these are TOAST options
     * @param forceQuote whether to force quoting of values
     * @param isQuoted   whether values are already quoted
     */
    public static void fillOptionParams(String[] options, BiConsumer<String, String> c,
                                        boolean isToast, boolean forceQuote, boolean isQuoted) {
        for (String pair : options) {
            int sep = pair.indexOf('=');
            String option;
            String value;
            if (sep == -1) {
                option = pair;
                value = "";
            } else {
                option = pair.substring(0, sep);
                value = pair.substring(sep + 1);
            }
            if (!isQuoted && (forceQuote || !PgDiffUtils.isValidId(value, false, false))) {
                // only quote non-ids, do not quote columns
                // pg_dump behavior
                value = PgDiffUtils.quoteString(value);
            }
            fillOptionParams(value, option, isToast, c);
        }
    }

    /**
     * Processes a single option parameter.
     *
     * @param value   the option value
     * @param option  the option name
     * @param isToast whether this is a TOAST option
     * @param c       the consumer to receive the key-value pair
     */
    public static void fillOptionParams(String value, String option, boolean isToast,
                                        BiConsumer<String, String> c) {
        String quotedOption = PgDiffUtils.getQuotedName(option);
        if (isToast) {
            quotedOption = "toast." + quotedOption;
        }
        c.accept(quotedOption, value);
    }

    /**
     * Fills the 'PgObjLocation'-object with action information, query of statement,
     * and it's position in the script from statement context, and then puts
     * filled 'PgObjLocation'-object to the storage of queries.
     */
    protected PgObjLocation fillQueryLocation(ParserRuleContext ctx) {
        String act = getStmtAction();
        PgObjLocation loc = new PgObjLocation.Builder()
                .setAction(act != null ? act : ctx.getStart().getText().toUpperCase(Locale.ROOT))
                .setSql(getFullCtxText(ctx))
                .setCtx(ctx)
                .build();

        db.addReference(fileName, loc);
        return loc;
    }

    /**
     * Adds missing COMMENT/RULE refs for correct showing them in Outline.
     * (In the case of COMMENT : used for COLUMN comments and comments
     * for objects which undefined in DbObjType).
     */
    protected void addOutlineRefForCommentOrRule(String action, ParserRuleContext ctx) {
        PgObjLocation loc = new PgObjLocation.Builder()
                .setAction(action)
                .setCtx(ctx)
                .build();
        db.addReference(fileName, loc);
    }

    protected static String getStrForStmtAction(String action, DbObjType type, ParseTree id) {
        return getStrForStmtAction(action, type, List.of(id));
    }

    /**
     * Used in general cases in {@link #getStmtAction()} for get action information.
     */
    protected static String getStrForStmtAction(String action, DbObjType type, List<? extends ParseTree> ids) {
        return action + ' ' + type.getTypeName() + ' ' +
                ids.stream().map(ParseTree::getText).collect(Collectors.joining("."));
    }

    /**
     * Returns action information which will later be used for showing in console,
     * in 'Outline' and in 'outline of Project explorer files'.
     */
    protected abstract String getStmtAction();

    protected AbstractSchema createAndAddSchemaWithCheck(ParserRuleContext nameCtx) {
        String name = nameCtx.getText();
        var defaultSchema = db.getDefaultSchema();
        // override the default schema location if we created it
        if (defaultSchema != null && defaultSchema.getBareName().equals(name)
                && defaultSchema.getLocation().getFilePath() == null) {
            var location = getLocation(List.of(nameCtx), DbObjType.SCHEMA, ACTION_CREATE, false, null,
                    LocationType.DEFINITION);
            defaultSchema.setLocation(location);
            return defaultSchema;
        }

        AbstractSchema schema = createSchema(name);
        addSafe(db, schema, List.of(nameCtx));
        return schema;
    }

    protected abstract AbstractSchema createSchema(String name);
}
