package ru.taximaxim.codekeeper.core.schema;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import ru.taximaxim.codekeeper.core.hashers.Hasher;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;

public class PgFtsTemplate extends PgStatementWithSearchPath {

    private String initFunction;
    private String lexizeFunction;

    public PgFtsTemplate(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FTS_TEMPLATE;
    }

    @Override
    public AbstractSchema getContainingSchema() {
        return (AbstractSchema)getParent();
    }

    @Override
    public String getCreationSQL() {
        StringBuilder sbSql = new StringBuilder();
        sbSql.append("CREATE TEXT SEARCH TEMPLATE ")
        .append(getQualifiedName()).append(" (\n\t");

        if (initFunction != null) {
            sbSql.append("INIT = ").append(initFunction).append(",\n\t");
        }

        sbSql.append("LEXIZE = ").append(lexizeFunction).append(" );");

        if (comment != null && !comment.isEmpty()) {
            sbSql.append("\n\n");
            appendCommentSql(sbSql);
        }

        return sbSql.toString();
    }

    @Override
    protected String getTypeName() {
        return "TEXT SEARCH TEMPLATE";
    }

    @Override
    public boolean appendAlterSQL(PgStatement newCondition, StringBuilder sb,
            AtomicBoolean isNeedDepcies) {
        final int startLength = sb.length();

        if (!compareUnalterable((PgFtsTemplate) newCondition)) {
            isNeedDepcies.set(true);
            return true;
        }

        if (!Objects.equals(getComment(), newCondition.getComment())) {
            sb.append("\n\n");
            newCondition.appendCommentSql(sb);
        }

        return sb.length() > startLength;
    }

    @Override
    public PgFtsTemplate shallowCopy() {
        PgFtsTemplate templateDst = new PgFtsTemplate(getName());
        copyBaseFields(templateDst);
        templateDst.setInitFunction(getInitFunction());
        templateDst.setLexizeFunction(getLexizeFunction());
        return templateDst;
    }

    @Override
    public boolean compare(PgStatement obj) {
        if (this == obj) {
            return true;
        }

        return obj instanceof PgFtsTemplate && super.compare(obj)
                && compareUnalterable((PgFtsTemplate) obj);
    }

    private boolean compareUnalterable(PgFtsTemplate template) {
        return Objects.equals(initFunction, template.initFunction)
                && Objects.equals(lexizeFunction, template.lexizeFunction);
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(initFunction);
        hasher.put(lexizeFunction);
    }

    public String getInitFunction() {
        return initFunction;
    }

    public void setInitFunction(final String initFunction) {
        this.initFunction = initFunction;
        resetHash();
    }

    public String getLexizeFunction() {
        return lexizeFunction;
    }

    public void setLexizeFunction(final String lexizeFunction) {
        this.lexizeFunction = lexizeFunction;
        resetHash();
    }
}
