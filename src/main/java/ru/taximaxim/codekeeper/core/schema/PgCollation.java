package ru.taximaxim.codekeeper.core.schema;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import ru.taximaxim.codekeeper.core.hashers.Hasher;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;

public class PgCollation extends PgStatementWithSearchPath {

    public PgCollation(String name) {
        super(name);
    }

    private String lcCollate;
    private String lcCtype;
    private String provider;
    private boolean deterministic = true;

    @Override
    public DbObjType getStatementType() {
        return DbObjType.COLLATION;
    }

    @Override
    public AbstractSchema getContainingSchema() {
        return (AbstractSchema) getParent();
    }

    public String getLcCollate() {
        return lcCollate;
    }

    public String getLcCtype() {
        return lcCtype;
    }

    public String getProvider() {
        return provider;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public void setDeterministic(boolean deterministic) {
        this.deterministic = deterministic;
        resetHash();
    }

    public void setLcCollate(final String lcCollate) {
        this.lcCollate = lcCollate;
        resetHash();
    }

    public void setLcCtype(final String lcCtype) {
        this.lcCtype = lcCtype;
        resetHash();
    }

    public void setProvider(final String provider) {
        this.provider = provider;
        resetHash();
    }

    @Override
    public String getCreationSQL() {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE COLLATION ");
        appendIfNotExists(sbSQL);
        sbSQL.append(getQualifiedName());
        sbSQL.append(" (");
        if (Objects.equals(getLcCollate(), getLcCtype())) {
            sbSQL.append("LOCALE = ").append(getLcCollate());
        } else {
            sbSQL.append("LC_COLLATE = ").append(getLcCollate());
            sbSQL.append(", LC_CTYPE = ").append(getLcCtype());
        }
        if (getProvider() != null) {
            sbSQL.append(", PROVIDER = ").append(getProvider());
        }
        if(!isDeterministic()) {
            sbSQL.append(", DETERMINISTIC = FALSE");
        }
        sbSQL.append(");");

        appendOwnerSQL(sbSQL);

        if (comment != null && !comment.isEmpty()) {
            sbSQL.append("\n\n");
            appendCommentSql(sbSQL);
        }

        return sbSQL.toString();
    }

    @Override
    public boolean appendAlterSQL(PgStatement newCondition, StringBuilder sb, AtomicBoolean isNeedDepcies) {
        final int startLength = sb.length();
        PgCollation newCollation = (PgCollation) newCondition;

        if (!compareUnalterable(newCollation)) {
            isNeedDepcies.set(true);
            return true;
        }

        if (!Objects.equals(getOwner(), newCollation.getOwner())) {
            newCollation.alterOwnerSQL(sb);
        }

        if (!Objects.equals(getComment(), newCollation.getComment())) {
            sb.append("\n\n");
            newCollation.appendCommentSql(sb);
        }

        return sb.length() > startLength;
    }

    @Override
    public boolean compare(PgStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof PgCollation && super.compare(obj)) {
            PgCollation coll = (PgCollation) obj;
            return compareUnalterable(coll);
        }
        return false;
    }

    private boolean compareUnalterable(PgCollation coll) {
        return deterministic == coll.isDeterministic()
                && Objects.equals(lcCollate, coll.getLcCollate())
                && Objects.equals(lcCtype, coll.getLcCtype())
                && Objects.equals(provider, coll.getProvider());
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(deterministic);
        hasher.put(lcCollate);
        hasher.put(lcCtype);
        hasher.put(provider);
    }

    @Override
    public PgStatement shallowCopy() {
        PgCollation collationDst = new PgCollation(getName());
        collationDst.lcCollate = getLcCollate();
        collationDst.lcCtype = getLcCtype();
        collationDst.provider = getProvider();
        collationDst.deterministic = isDeterministic();
        return collationDst;
    }
}
