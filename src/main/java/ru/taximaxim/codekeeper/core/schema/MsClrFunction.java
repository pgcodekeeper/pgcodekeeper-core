package ru.taximaxim.codekeeper.core.schema;

import java.util.Objects;
import java.util.stream.Collectors;

import ru.taximaxim.codekeeper.core.MsDiffUtils;
import ru.taximaxim.codekeeper.core.hashers.Hasher;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;

public class MsClrFunction extends AbstractMsClrFunction {

    private String returns;
    private FuncTypes funcType = FuncTypes.SCALAR;

    public MsClrFunction(String name, String assembly, String assemblyClass,
            String assemblyMethod) {
        super(name, assembly, assemblyClass, assemblyMethod);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FUNCTION;
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
    public String getDeclaration(Argument arg, boolean includeDefaultValue,
            boolean includeArgName) {
        final StringBuilder sbString = new StringBuilder();
        sbString.append(arg.getName()).append(' ').append(arg.getDataType());

        String def = arg.getDefaultExpression();

        if (includeDefaultValue && def != null && !def.isEmpty()) {
            sbString.append(" = ");
            sbString.append(def);
        }

        ArgMode mode = arg.getMode();
        if (ArgMode.IN != mode) {
            sbString.append(' ').append(mode);
        }

        return sbString.toString();
    }

    @Override
    protected String getFunctionFullSQL(boolean isCreate) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("SET QUOTED_IDENTIFIER OFF").append(GO).append('\n');
        sbSQL.append("SET ANSI_NULLS OFF").append(GO).append('\n');
        sbSQL.append(isCreate ? "CREATE" : "ALTER");
        sbSQL.append(" FUNCTION ");
        sbSQL.append(getQualifiedName()).append('(');
        sbSQL.append(arguments.stream().map(arg -> getDeclaration(arg, true, true))
                .collect(Collectors.joining(", ")));
        sbSQL.append(')');

        sbSQL.append("\nRETURNS ").append(getReturns());

        if (!options.isEmpty()) {
            sbSQL.append("\nWITH ").append(String.join(", ", options)).append('\n');
        }

        sbSQL.append("AS\nEXTERNAL NAME ");
        sbSQL.append(MsDiffUtils.quoteName(getAssembly())).append('.');
        sbSQL.append(MsDiffUtils.quoteName(getAssemblyClass())).append('.');
        sbSQL.append(MsDiffUtils.quoteName(getAssemblyMethod()));
        sbSQL.append(GO);

        return sbSQL.toString();
    }

    public FuncTypes getFuncType() {
        return funcType;
    }

    public void setFuncType(FuncTypes funcType) {
        this.funcType = funcType;
        resetHash();
    }

    /**
     * @return the returns
     */
    @Override
    public String getReturns() {
        return returns;
    }

    /**
     * @param returns the returns to set
     */
    public void setReturns(String returns) {
        this.returns = returns;
        resetHash();
    }

    @Override
    protected boolean compareUnalterable(AbstractFunction func) {
        return func instanceof MsClrFunction && super.compareUnalterable(func)
                && Objects.equals(returns, func.getReturns())
                && Objects.equals(getFuncType(), ((MsClrFunction) func).getFuncType());
    }

    @Override
    public boolean needDrop(AbstractFunction newFunction) {
        if (newFunction instanceof MsClrFunction) {
            return getFuncType() != ((MsClrFunction) newFunction).getFuncType();
        }

        return true;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(getReturns());
        hasher.put(getFuncType());
    }

    @Override
    protected AbstractMsClrFunction getFunctionCopy() {
        MsClrFunction func =  new MsClrFunction(getName(), getAssembly(),
                getAssemblyClass(), getAssemblyMethod());
        func.setFuncType(getFuncType());
        func.setReturns(getReturns());
        return func;
    }
}
