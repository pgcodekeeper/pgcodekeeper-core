package ru.taximaxim.codekeeper.core.schema;

import java.util.Objects;

import ru.taximaxim.codekeeper.core.hashers.Hasher;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;

public class MsFunction extends AbstractMsFunction {

    private FuncTypes funcType = FuncTypes.SCALAR;

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FUNCTION;
    }

    public MsFunction(String name) {
        super(name);
    }

    @Override
    public String getCreationSQL() {
        final StringBuilder sbSQL = new StringBuilder();
        appendDropBeforeCreate(sbSQL);
        sbSQL.append(getFunctionFullSQL(true));

        appendOwnerSQL(sbSQL);
        appendPrivileges(sbSQL);
        return sbSQL.toString();
    }

    @Override
    protected String getFunctionFullSQL(boolean isCreate) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("SET QUOTED_IDENTIFIER ").append(isQuotedIdentified() ? "ON" : "OFF");
        sbSQL.append(GO).append('\n');
        sbSQL.append("SET ANSI_NULLS ").append(isAnsiNulls() ? "ON" : "OFF");
        sbSQL.append(GO).append('\n');
        appendSourceStatement(isCreate, sbSQL);
        sbSQL.append(GO);
        return sbSQL.toString();
    }

    @Override
    protected boolean compareUnalterable(AbstractFunction func) {
        return func instanceof AbstractMsFunction && super.compareUnalterable(func)
                && Objects.equals(getFuncType(), ((MsFunction) func).getFuncType());
    }

    @Override
    public boolean needDrop(AbstractFunction newFunction) {
        if (newFunction instanceof MsFunction) {
            return getFuncType() != ((MsFunction) newFunction).getFuncType();
        }

        return true;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(getFuncType());
    }

    @Override
    protected AbstractMsFunction getFunctionCopy() {
        MsFunction func = new MsFunction(getName());
        func.setFuncType(getFuncType());
        return func;
    }

    public FuncTypes getFuncType() {
        return funcType;
    }

    public void setFuncType(FuncTypes funcType) {
        this.funcType = funcType;
        resetHash();
    }
}
