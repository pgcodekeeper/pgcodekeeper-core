package ru.taximaxim.codekeeper.core.schema;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import ru.taximaxim.codekeeper.core.PgDiffUtils;
import ru.taximaxim.codekeeper.core.hashers.Hasher;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;

/**
 * Stores extension information.
 *
 * @author Alexander Levsha
 */
public class PgExtension extends PgStatement {

    private String schema;
    private boolean relocatable;

    @Override
    public DbObjType getStatementType() {
        return DbObjType.EXTENSION;
    }

    public PgExtension(String name) {
        super(name);
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(final String schema) {
        this.schema = schema;
        resetHash();
    }

    public boolean isRelocatable() {
        return relocatable;
    }

    public void setRelocatable(boolean relocatable) {
        this.relocatable = relocatable;
    }

    @Override
    public PgDatabase getDatabase() {
        return (PgDatabase)getParent();
    }

    @Override
    public String getCreationSQL() {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE EXTENSION ");
        appendIfNotExists(sbSQL);
        sbSQL.append(getQualifiedName());

        if (getSchema() != null && !getSchema().isEmpty()) {
            sbSQL.append(" SCHEMA ");
            sbSQL.append(getSchema());
        }

        sbSQL.append(';');

        if(getComment() != null && !getComment().isEmpty()) {
            sbSQL.append("\n\n");
            appendCommentSql(sbSQL);
        }

        return sbSQL.toString();
    }

    @Override
    public boolean appendAlterSQL(PgStatement newCondition, StringBuilder sb,
            AtomicBoolean isNeedDepcies) {
        final int startLength = sb.length();
        PgExtension newExt = (PgExtension) newCondition;

        if (!Objects.equals(newExt.getSchema(), getSchema())) {
            if (!isRelocatable()) {
                return true;
            }
            sb.append("\n\nALTER EXTENSION ")
            .append(PgDiffUtils.getQuotedName(getName()))
            .append(" SET SCHEMA ")
            .append(newExt.getSchema())
            .append(';');
            isNeedDepcies.set(true);
        }
        // TODO ALTER EXTENSION UPDATE TO ?
        if (!Objects.equals(getComment(), newExt.getComment())) {
            sb.append("\n\n");
            newExt.appendCommentSql(sb);
        }
        return sb.length() > startLength;
    }

    @Override
    public boolean compare(PgStatement obj) {
        if (this == obj) {
            return true;
        }

        return obj instanceof PgExtension && super.compare(obj)
                && Objects.equals(schema, ((PgExtension) obj).getSchema());
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(schema);
    }

    @Override
    public PgExtension shallowCopy() {
        PgExtension extDst = new PgExtension(getName());
        copyBaseFields(extDst);
        extDst.setSchema(getSchema());
        return extDst;
    }
}
