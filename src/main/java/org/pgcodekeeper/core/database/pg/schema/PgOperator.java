/*******************************************************************************
 * Copyright 2017-2026 TAXTELECOM, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.pgcodekeeper.core.database.pg.schema;

import java.util.Objects;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.pg.jdbc.PgSupportedVersion;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * PostgreSQL operator implementation.
 * Operators are symbols that represent specific operations (like +, -, *, etc.)
 * and can be customized for user-defined types with associated functions.
 */
public class PgOperator extends PgAbstractStatement implements IOperator {

    private String procedure;
    private String leftArg;
    private String rightArg;
    private String commutator;
    private String negator;
    private boolean isMerges;
    private boolean isHashes;
    private String restrict;
    private String join;
    private String returns;

    /**
     * Creates a new PostgreSQL operator.
     *
     * @param name operator symbol
     */
    public PgOperator(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.OPERATOR;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE OPERATOR ");
        sbSQL.append(quote(getSchemaName())).append('.');
        sbSQL.append(getBareName());
        sbSQL.append(" (\n\tPROCEDURE = ");
        sbSQL.append(procedure);

        if (leftArg != null) {
            sbSQL.append(",\n\tLEFTARG = ");
            sbSQL.append(leftArg);
        }

        if (rightArg != null) {
            sbSQL.append(",\n\tRIGHTARG = ");
            sbSQL.append(rightArg);
        }

        if (commutator != null) {
            sbSQL.append(",\n\tCOMMUTATOR = ");
            sbSQL.append(commutator);
        }

        if (negator != null) {
            sbSQL.append(",\n\tNEGATOR = ");
            sbSQL.append(negator);
        }

        if (isMerges) {
            sbSQL.append(",\n\tMERGES");
        }

        if (isHashes) {
            sbSQL.append(",\n\tHASHES");
        }

        if (restrict != null) {
            sbSQL.append(",\n\tRESTRICT = ");
            sbSQL.append(restrict);
        }

        if (join != null) {
            sbSQL.append(",\n\tJOIN = ");
            sbSQL.append(join);
        }

        sbSQL.append("\n)");
        script.addStatement(sbSQL);

        appendOwnerSQL(script);
        appendPrivileges(script);
        appendComments(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgOperator newOperator = (PgOperator) newCondition;

        if (!compareUnalterable(newOperator, script.getSettings())) {
            return ObjectState.RECREATE;
        }

        StringBuilder sql = new StringBuilder();
        if (!Objects.equals(restrict, newOperator.restrict)) {
            sql.append("RESTRICT = ").append(newOperator.restrict != null ? newOperator.restrict : "NONE")
            .append(", ");
        }
        if (!Objects.equals(join, newOperator.join)) {
            sql.append("JOIN = ").append(newOperator.join != null ? newOperator.join : "NONE")
                .append(", ");
        }
        if (!Objects.equals(commutator, newOperator.commutator)) {
            sql.append("COMMUTATOR = ").append(newOperator.commutator).append(", ");
        }
        if (!Objects.equals(negator, newOperator.negator)) {
            sql.append("NEGATOR = ").append(newOperator.commutator).append(", ");
        }
        if (isHashes != newOperator.isHashes) {
            sql.append("HASHES, ");
        }
        if (isMerges != newOperator.isMerges) {
            sql.append("MERGES, ");
        }
        if (!sql.isEmpty()) {
            sql.setLength(sql.length() - 2);
            script.addStatement("ALTER OPERATOR " + getQualifiedName() + "\n\tSET (" + sql.toString() + ')');
        }

        appendAlterOwner(newOperator, script);
        appendAlterComments(newOperator, script);

        return getObjectState(script, startSize);
    }

    @Override
    public void setReturns(String returns) {
        this.returns = returns;
    }

    @Override
    public String getReturns() {
        return returns;
    }

    /**
     * Returns the operator signature including its arguments.
     *
     * @return operator signature in format "op(leftarg, rightarg)"
     */
    public String getSignature() {
        return getBareName() +
                '(' +
                (leftArg == null ? "NONE" : leftArg) +
                ", " +
                (rightArg == null ? "NONE" : rightArg) +
                ')';
    }

    /**
     * Returns the operator arguments in parentheses format.
     *
     * @return arguments string in format "(leftarg, rightarg)" or "(rightarg)" for unary
     */
    public String getArguments() {
        StringBuilder signature = new StringBuilder();
        String left = getLeftArg();
        String right = getRightArg();

        signature.append('(');
        if (left != null) {
            signature.append(left);
            if (right != null) {
                signature.append(", ").append(right);
            }
        } else {
            signature.append(right);
        }
        signature.append(')');

        return signature.toString();
    }

    /**
     * Alias for {@link #getSignature()} which provides a unique operator ID.
     * <p>
     * Use {@link #getBareName()} to get just the operator name.
     */
    @Override
    public String getName() {
        return getSignature();
    }

    @Override
    public String getQualifiedName() {
        if (qualifiedName == null) {
            qualifiedName = parent.getQualifiedName() + '.' + getName();
        }

        return qualifiedName;
    }

    public void setProcedure(String procedure) {
        this.procedure = procedure;
        resetHash();
    }

    @Override
    public String getLeftArg() {
        return leftArg;
    }

    public void setLeftArg(String leftArg) {
        this.leftArg = leftArg;
        resetHash();
    }

    @Override
    public String getRightArg() {
        return rightArg;
    }

    public void setRightArg(String rightArg) {
        this.rightArg = rightArg;
        resetHash();
    }

    public void setCommutator(String commutator) {
        this.commutator = commutator;
        resetHash();
    }

    public void setNegator(String negator) {
        this.negator = negator;
        resetHash();
    }

    public void setMerges(boolean isMerges) {
        this.isMerges = isMerges;
        resetHash();
    }

    public void setHashes(boolean isHashes) {
        this.isHashes = isHashes;
        resetHash();
    }

    public void setRestrict(String restrict) {
        this.restrict = restrict;
        resetHash();
    }

    public void setJoin(String join) {
        this.join = join;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(procedure);
        hasher.put(leftArg);
        hasher.put(rightArg);
        hasher.put(commutator);
        hasher.put(negator);
        hasher.put(isMerges);
        hasher.put(isHashes);
        hasher.put(restrict);
        hasher.put(join);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof PgOperator oper && super.compare(obj)
                && compareUnalterable(oper, null)
                && Objects.equals(restrict, oper.restrict)
                && Objects.equals(join, oper.join);
    }

    private boolean compareUnalterable(PgOperator oper, ISettings settings) {
        if (!compareCommonUnalterable(oper)) {
            return false;
        }

        
        if (checkSyntaxVersion(settings, PgSupportedVersion.VERSION_17)) {
            return  (Objects.equals(commutator, oper.commutator) || null == commutator)
                && (Objects.equals(negator, oper.negator) || null == negator)
                && (isMerges == oper.isMerges || oper.isMerges)
                && (isHashes == oper.isHashes || oper.isHashes);
        }

        return Objects.equals(commutator, oper.commutator)
                && Objects.equals(negator, oper.negator)
                && isMerges == oper.isMerges
                && isHashes == oper.isHashes;
    }

    private boolean compareCommonUnalterable(PgOperator oper) {
        return Objects.equals(procedure, oper.procedure)
                && Objects.equals(leftArg, oper.leftArg)
                && Objects.equals(rightArg, oper.rightArg);
    }

    @Override
    protected PgOperator getCopy() {
        PgOperator operatorDst = new PgOperator(name);
        operatorDst.setProcedure(procedure);
        operatorDst.setLeftArg(leftArg);
        operatorDst.setRightArg(rightArg);
        operatorDst.setCommutator(commutator);
        operatorDst.setNegator(negator);
        operatorDst.setMerges(isMerges);
        operatorDst.setHashes(isHashes);
        operatorDst.setRestrict(restrict);
        operatorDst.setJoin(join);
        return operatorDst;
    }
}
