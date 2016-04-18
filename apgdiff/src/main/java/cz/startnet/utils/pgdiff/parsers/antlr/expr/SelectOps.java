package cz.startnet.utils.pgdiff.parsers.antlr.expr;

import java.util.List;

import org.antlr.v4.runtime.tree.TerminalNode;

import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Select_opsContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Select_ops_no_parensContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Select_primaryContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Set_qualifierContext;

/**
 * Merging wrapper for select_ops/select_ops_no_parens
 *
 * @author levsha_aa
 */
public class SelectOps {

    private final Select_opsContext ops;
    private final Select_ops_no_parensContext opsNp;

    public SelectOps(Select_opsContext ops) {
        this.ops = ops;
        this.opsNp = null;
    }

    public SelectOps(Select_ops_no_parensContext ops) {
        this.opsNp = ops;
        this.ops = null;
    }

    private boolean isNp() {
        return opsNp != null;
    }

    public TerminalNode leftParen() {
        return isNp() ? null : ops.LEFT_PAREN();
    }

    public TerminalNode rightParen() {
        return isNp() ? null : ops.RIGHT_PAREN();
    }

    public SelectStmt selectStmt() {
        return isNp() ? null : new SelectStmt(ops.select_stmt());
    }

    public List<Select_opsContext> selectOps() {
        return isNp() ? opsNp.select_ops() : ops.select_ops();
    }

    public SelectOps selectOps(int i) {
        return new SelectOps(isNp() ? opsNp.select_ops(i) : ops.select_ops(i));
    }

    public TerminalNode intersect() {
        return isNp() ? opsNp.INTERSECT() : ops.INTERSECT();
    }

    public TerminalNode union() {
        return isNp() ? opsNp.UNION() : ops.UNION();
    }

    public TerminalNode except() {
        return isNp() ? opsNp.EXCEPT() : ops.EXCEPT();
    }

    public Set_qualifierContext setQualifier() {
        return isNp() ? opsNp.set_qualifier() : ops.set_qualifier();
    }

    public Select_primaryContext selectPrimary() {
        return isNp() ? opsNp.select_primary() : ops.select_primary();
    }
}
