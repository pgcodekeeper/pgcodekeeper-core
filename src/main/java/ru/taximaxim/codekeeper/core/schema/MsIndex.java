package ru.taximaxim.codekeeper.core.schema;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import ru.taximaxim.codekeeper.core.MsDiffUtils;
import ru.taximaxim.codekeeper.core.PgDiffArguments;

public class MsIndex extends AbstractIndex {

    public MsIndex(String name) {
        super(name);
    }

    @Override
    public String getCreationSQL() {
        return getCreationSQL(false);
    }

    private String getCreationSQL(boolean dropExisting) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE ");

        if (isUnique()) {
            sbSQL.append("UNIQUE ");
        }

        if (!isClusterIndex()) {
            sbSQL.append("NON");
        }
        sbSQL.append("CLUSTERED ");

        sbSQL.append("INDEX ");
        sbSQL.append(MsDiffUtils.quoteName(getName()));
        sbSQL.append(" ON ");
        sbSQL.append(getParent().getQualifiedName());
        sbSQL.append(' ');
        sbSQL.append(getDefinition());

        if (!includes.isEmpty()) {
            sbSQL.append(" INCLUDE (");
            for (String col : includes) {
                sbSQL.append(MsDiffUtils.quoteName(col)).append(", ");
            }
            sbSQL.setLength(sbSQL.length() - 2);
            sbSQL.append(')');
        }

        if (getWhere() != null) {
            sbSQL.append("\nWHERE ").append(getWhere());
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            sb.append(entry.getKey());
            if (!entry.getValue().isEmpty()){
                sb.append(" = ").append(entry.getValue());
            }
            sb.append(", ");
        }

        PgDiffArguments args = getDatabase().getArguments();
        if (args != null && args.isConcurrentlyMode() && !options.containsKey("ONLINE")) {
            sb.append("ONLINE = ON, ");
        }

        if (dropExisting) {
            sb.append("DROP_EXISTING = ON, ");
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 2);
            sbSQL.append("\nWITH (").append(sb).append(')');
        }

        if (getTablespace() != null) {
            sbSQL.append("\nON ").append(MsDiffUtils.quoteName(getTablespace()));
        }

        sbSQL.append(GO);

        return sbSQL.toString();
    }

    @Override
    public boolean appendAlterSQL(PgStatement newCondition, StringBuilder sb,
            AtomicBoolean isNeedDepcies) {
        if (!compare(newCondition)) {
            isNeedDepcies.set(true);

            MsIndex newIndex = (MsIndex) newCondition;
            if (!isClusterIndex() || newIndex.isClusterIndex()) {
                sb.append("\n\n")
                .append(newIndex.getCreationSQL(true));
            }

            return true;
        }

        // options can be changed by syntax :
        // ALTER SEQUENCE index_name ON schema_name.table REBUILD WITH (options (, option)*)
        // but how to reset option? all indices has all option with default value
        // and we don't know what is it and how to change current value to default value.

        return false;
    }

    @Override
    protected StringBuilder appendFullName(StringBuilder sb) {
        sb.append(MsDiffUtils.quoteName(getName()))
        .append(" ON ")
        .append(getParent().getQualifiedName());
        return sb;
    }

    @Override
    public boolean isPostgres() {
        return false;
    }

    @Override
    protected AbstractIndex getIndexCopy() {
        return new MsIndex(getName());
    }
}
