package cz.startnet.utils.pgdiff.loader.callables;

import cz.startnet.utils.pgdiff.AbstractErrorLocation;

/**
 * The class of the query to execute. It contains the query, information about
 * the position of the query in the parsed script and information about the
 * type of command.
 */
public class QueryLocation extends AbstractErrorLocation {

    private final String stmtAction;
    private final String sql;

    public QueryLocation(String stmtAction, int offsetInFullTxt, int lineNumber,
            String sql) {
        super(offsetInFullTxt, lineNumber);
        this.stmtAction = stmtAction;
        this.sql = sql;
    }

    public String getStmtAction() {
        return stmtAction;
    }

    public String getSql() {
        return sql;
    }
}
