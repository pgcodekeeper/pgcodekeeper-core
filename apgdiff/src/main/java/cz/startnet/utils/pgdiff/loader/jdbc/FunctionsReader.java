package cz.startnet.utils.pgdiff.loader.jdbc;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import cz.startnet.utils.pgdiff.PgDiffUtils;
import cz.startnet.utils.pgdiff.loader.SupportedVersion;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.VexContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Vex_eofContext;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.UtilExpr;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.ValueExpr;
import cz.startnet.utils.pgdiff.parsers.antlr.rulectx.Vex;
import cz.startnet.utils.pgdiff.parsers.antlr.statements.ParserAbstract;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.IArgument;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgFunction;
import cz.startnet.utils.pgdiff.schema.PgFunction.Argument;
import cz.startnet.utils.pgdiff.schema.PgSchema;
import cz.startnet.utils.pgdiff.wrappers.ResultSetWrapper;
import cz.startnet.utils.pgdiff.wrappers.WrapperAccessException;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class FunctionsReader extends JdbcReader {

    public static class FunctionsReaderFactory extends JdbcReaderFactory {

        public FunctionsReaderFactory(long hasHelperMask, String helperFunction, Map<SupportedVersion, String> queries) {
            super(hasHelperMask, helperFunction, queries);
        }

        @Override
        public JdbcReader getReader(JdbcLoaderBase loader) {
            return new FunctionsReader(this, loader);
        }
    }

    private static final float DEFAULT_PROCOST = 100.0f;
    private static final float DEFAULT_PROROWS = 1000.0f;

    private FunctionsReader(JdbcReaderFactory factory, JdbcLoaderBase loader) {
        super(factory, loader);
    }

    @Override
    protected void processResult(ResultSetWrapper res, PgSchema schema) throws WrapperAccessException {
        String schemaName = schema.getName();
        String functionName = res.getString("proname");
        loader.setCurrentObject(new GenericColumn(schemaName, functionName, DbObjType.FUNCTION));
        PgFunction f = new PgFunction(functionName, "");

        f.setBody(loader.args, getFunctionBody(res, schemaName));

        // RETURN TYPE

        boolean returnsTable = false;
        StringBuilder returnedTableArguments = new StringBuilder();
        String[] argModes = res.getArray("proargmodes", String.class);
        String[] argNames = res.getArray("proargnames", String.class);
        Long[] argTypeOids = res.getArray("proallargtypes", Long.class);
        if (argModes != null && Arrays.asList(argModes).contains("t")) {
            for (int i = 0; i < argModes.length; i++) {
                String type = argModes[i];
                if ("t".equals(type)) {
                    returnsTable = true;
                    returnedTableArguments.append(returnedTableArguments.length() > 0 ? ", " : "");
                    returnedTableArguments.append(argNames[i]).append(" ");

                    JdbcType returnType = loader.cachedTypesByOid.get(argTypeOids[i]);
                    returnedTableArguments.append(returnType.getFullName(schemaName));
                    returnType.addTypeDepcy(f);
                }
            }
        }

        if (returnsTable) {
            f.setReturns("TABLE(" + returnedTableArguments + ")");
        } else {
            JdbcType returnType = loader.cachedTypesByOid.get(res.getLong("prorettype"));
            String retType = returnType.getFullName(schemaName);
            f.setReturns(res.getBoolean("proretset") ? "SETOF " + retType : retType);
            returnType.addTypeDepcy(f);
        }

        // OWNER
        loader.setOwner(f, res.getLong("proowner"));

        // COMMENT
        String comment = res.getString("comment");
        if (comment != null && !comment.isEmpty()) {
            f.setComment(loader.args, PgDiffUtils.quoteString(comment));
        }

        StringBuilder argsWithoutDefault = new StringBuilder();

        Long[] argtypes = res.getArray("argtypes", Long.class);

        if(argTypeOids != null || argtypes != null) {
            if (argTypeOids == null) {
                for (int i = 0; argtypes.length > i; i++) {
                    Argument a = f.new Argument(argNames != null ? argNames[i] : null,
                            loader.cachedTypesByOid.get(argtypes[i]).getFullName(schemaName));

                    f.addArgument(a);

                    if (a.getName() != null) {
                        argsWithoutDefault.append(a.getName()).append(" ");
                    } else {
                        argsWithoutDefault.append("");
                    }
                    argsWithoutDefault.append(a.getDataType())
                    .append(argtypes.length - 1  > i ? ", " : "");
                }
            } else {
                int tableModesCount = 0;

                for (int i = 0; argTypeOids.length > i; i++) {
                    if("t".equals(argModes[i])) {
                        tableModesCount++;
                    }
                }

                for (int i = 0; argTypeOids.length > i; i++) {
                    String aMode = argModes[i];
                    if(!"t".equals(aMode)) {
                        switch(aMode) {
                        case "i":
                            aMode = "IN";
                            break;
                        case "o":
                            aMode = "OUT";
                            break;
                        case "b":
                            aMode = "INOUT";
                            break;
                        case "v":
                            aMode = "VARIADIC";
                            break;
                        }

                        Argument a = f.new Argument(aMode,
                                argNames != null ? argNames[i] : null,
                                        loader.cachedTypesByOid.get(argTypeOids[i]).getFullName(schemaName));

                        f.addArgument(a);

                        if (!"IN".equals(a.getMode())) {
                            argsWithoutDefault.append(a.getMode()).append(" ");
                        }
                        if (a.getName() != null) {
                            argsWithoutDefault.append(a.getName()).append(" ");
                        } else {
                            argsWithoutDefault.append("");
                        }
                        argsWithoutDefault.append(a.getDataType())
                        .append(argTypeOids.length - 1 - tableModesCount > i ? ", " : "");
                    }
                }
            }

            String defaultValuesAsString = res.getString("default_values_as_string");
            if (defaultValuesAsString != null) {
                loader.submitAntlrTask(defaultValuesAsString, (PgDatabase)schema.getParent(),
                        p -> p.vex_eof(),
                        (ctx, db) -> {
                            db.getContextsForAnalyze().add(new AbstractMap.SimpleEntry<>(f, ctx));

                            functionDefaultsAnalyze(ctx, f, schemaName);
                        });
            }
        }

        // PRIVILEGES
        String signatureWithoutDefaults = PgDiffUtils.getQuotedName(functionName) + "("
                + argsWithoutDefault.toString() + ")";
        loader.setPrivileges(f, signatureWithoutDefaults, res.getString("aclarray"), f.getOwner(), null);

        schema.addFunction(f);
    }

    public static void functionDefaultsAnalyze(Vex_eofContext ctx, PgFunction f, String schemaName) {
        List<VexContext> vexCtxList = ctx.vex();

        Deque<String> defultsQueue = new ArrayDeque<>();
        for (VexContext vx : vexCtxList) {
            defultsQueue.offerLast(ParserAbstract.getFullCtxText(vx));
        }

        for (int i = (f.getArguments().size() - 1); i >= 0; i--) {
            if (defultsQueue.isEmpty()) {
                break;
            }
            IArgument a = f.getArguments().get(i);
            if ("IN".equals(a.getMode()) || "INOUT".equals(a.getMode())) {
                a.setDefaultExpression(defultsQueue.pollLast());
            }
        }

        ValueExpr vex = new ValueExpr(schemaName);
        for (VexContext vx : vexCtxList) {
            UtilExpr.analyze(new Vex(vx), vex, f);
        }
    }

    private String getFunctionBody(ResultSetWrapper res, String schemaName) throws WrapperAccessException {
        StringBuilder body = new StringBuilder();

        String lanName = res.getString("lang_name");
        body.append("LANGUAGE ").append(PgDiffUtils.getQuotedName(lanName));

        // since 9.5 PostgreSQL
        if (SupportedVersion.VERSION_9_5.checkVersion(loader.version)) {
            Long[] protrftypes = res.getArray("protrftypes", Long.class);
            if (protrftypes != null) {
                body.append(" TRANSFORM ");
                for (Long s : protrftypes) {
                    body.append("FOR TYPE ")
                    .append(loader.cachedTypesByOid.get(s).getFullName(schemaName));
                    body.append(", ");
                }
                body.setLength(body.length() - 2);
            }
        }

        if (res.getBoolean("proiswindow")) {
            body.append(" WINDOW");
        }

        // VOLATILE is default
        switch (res.getString("provolatile")) {
        case "i":
            body.append(" IMMUTABLE");
            break;
        case "s":
            body.append(" STABLE");
            break;
        default :
            break;
        }

        // CALLED ON NULL INPUT is default
        if (res.getBoolean("proisstrict")) {
            body.append(" STRICT");
        }

        // SECURITY INVOKER is default
        if (res.getBoolean("prosecdef")) {
            body.append(" SECURITY DEFINER");
        }

        if (res.getBoolean("proleakproof")) {
            body.append(" LEAKPROOF");
        }

        // since 9.6 PostgreSQL
        // parallel mode: s - safe, r - restricted, u - unsafe
        if (SupportedVersion.VERSION_9_6.checkVersion(loader.version)) {
            String parMode = res.getString("proparallel");
            switch (parMode) {
            case "s":
                body.append(" PARALLEL SAFE");
                break;
            case "r":
                body.append(" PARALLEL RESTRICTED");
                break;
            default :
                break;
            }
        }

        float cost = res.getFloat("procost");
        if ("internal".equals(lanName) || "c".equals(lanName)) {
            /* default cost is 1 */
            if (cost != 1) {
                body.append(" COST ").append((int) cost);
            }
        } else {
            /* default cost is 100 */
            if (cost != DEFAULT_PROCOST) {
                body.append(" COST ").append((int) cost);
            }
        }

        float rows = res.getFloat("prorows");
        if (rows != 0.0f && rows != DEFAULT_PROROWS) {
            body.append(" ROWS ").append((int) rows);
        }

        String [] proconfig = res.getArray("proconfig", String.class);
        if (proconfig != null) {
            for (String param : proconfig) {
                String[] params = param.split("=");
                String par = params[0];
                String val = params[1];
                if (!"DateStyle".equals(par) && !"search_path".equals(par)) {
                    par = PgDiffUtils.getQuotedName(par);
                    val = PgDiffUtils.quoteString(val);
                }
                body.append("\n    SET ").append(par).append(" TO ")
                .append(val);
            }
        }

        String definition = res.getString("prosrc");
        String quote = getStringLiteralDollarQuote(definition);
        String probin = res.getString("probin");
        if (probin != null && !probin.isEmpty()) {
            body.append("\n    AS ").append(PgDiffUtils.quoteString(probin));
            if (!"-".equals(definition)) {
                body.append(", ");
                if (!definition.contains("\'") && !definition.contains("\\")) {
                    body.append(PgDiffUtils.quoteString(definition));
                } else {
                    body.append(quote).append(definition).append(quote);
                }
            }
        } else {
            if (!"-".equals(definition)) {
                body.append("\n    AS ").append(quote).append(definition).append(quote);
            }
        }
        return body.toString();
    }

    /**
     * Function equivalent to appendStringLiteralDQ in dumputils.c
     */
    public static String getStringLiteralDollarQuote(String definition) {
        final String suffixes = "_XXXXXXX";
        String quote = "$";
        int counter = 0;
        while (definition.contains(quote)) {
            quote = quote.concat(String.valueOf(suffixes.charAt(counter++)));
            counter %= suffixes.length();
        }

        return quote.concat("$");
    }
}
