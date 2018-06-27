package cz.startnet.utils.pgdiff.schema;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import cz.startnet.utils.pgdiff.MsDiffUtils;

public class MsSchema extends PgSchema {

    public MsSchema(String name, String rawStatement) {
        super(name, rawStatement);
    }

    @Override
    public String getCreationSQL() {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE SCHEMA ");
        sbSQL.append(MsDiffUtils.getQuotedName(getName()));
        if (owner != null) {
            sbSQL.append("\nAUTHORIZATION ").append(MsDiffUtils.getQuotedName(owner));
        }
        sbSQL.append(GO);
        //appendPrivileges(sbSQL);

        return sbSQL.toString();
    }

    @Override
    public boolean appendAlterSQL(PgStatement newCondition, StringBuilder sb,
            AtomicBoolean isNeedDepcies) {
        final int startLength = sb.length();
        MsSchema newSchema;
        if (newCondition instanceof MsSchema) {
            newSchema = (MsSchema) newCondition;
        } else {
            return false;
        }
        MsSchema oldSchema = this;

        if (!Objects.equals(oldSchema.getOwner(), newSchema.getOwner())) {
            sb.append("\nALTER AUTHORIZATION ON SCHEMA :: ").append(MsDiffUtils.getQuotedName(getName()));
            sb.append("\nTO ").append(MsDiffUtils.getQuotedName(newSchema.getOwner()));
            sb.append(GO);
        }

        //alterPrivileges(newSchema, sb);

        return sb.length() > startLength;
    }

    @Override
    public String getDropSQL() {
        return "DROP SCHEMA " + MsDiffUtils.getQuotedName(getName()) + GO;
    }
}
