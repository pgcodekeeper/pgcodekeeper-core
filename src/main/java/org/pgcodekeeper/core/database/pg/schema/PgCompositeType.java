/*******************************************************************************
 * Copyright 2017-2025 TAXTELECOM, LLC
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

import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractType;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementUtils;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * PostgreSQL composite type implementation.
 * Represents a composite type consisting of multiple attributes (fields),
 * similar to a table row structure but used as a data type.
 */
public final class PgCompositeType extends AbstractType implements IPgStatement {

    private static final String COLLATE = " COLLATE ";

    private final List<AbstractColumn> attrs = new ArrayList<>();

    /**
     * Creates a new PostgreSQL composite type.
     *
     * @param name type name
     */
    public PgCompositeType(String name) {
        super(name);
    }

    @Override
    protected void appendDef(StringBuilder sb) {
        sb.append(" AS (");
        for (AbstractColumn attr : attrs) {
            sb.append("\n\t").append(PgDiffUtils.getQuotedName(attr.getName()))
                    .append(' ').append(attr.getType());

            if (attr.getCollation() != null) {
                sb.append(COLLATE).append(attr.getCollation());
            }
            sb.append(',');
        }
        if (!attrs.isEmpty()) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("\n)");

    }

    @Override
    public void appendComments(SQLScript script) {
        super.appendComments(script);
        appendChildrenComments(script);
    }

    private void appendChildrenComments(SQLScript script) {
        for (final AbstractColumn column : attrs) {
            column.appendComments(script);
        }
    }

    @Override
    protected boolean compareUnalterable(AbstractType newType) {
        return !StatementUtils.isColumnsOrderChanged(((PgCompositeType) newType).attrs, attrs);
    }

    @Override
    protected void compareType(AbstractType newType, AtomicBoolean isNeedDepcies, SQLScript script) {
        PgCompositeType newCompositeType = (PgCompositeType) newType;
        StringBuilder attrSb = new StringBuilder();
        for (AbstractColumn attr : newCompositeType.attrs) {
            AbstractColumn oldAttr = getAttr(attr.getName());
            if (oldAttr == null) {
                appendAlterAttribute(attrSb, "ADD", " ", attr);
            } else if (!oldAttr.getType().equals(attr.getType()) ||
                    !Objects.equals(attr.getCollation(), oldAttr.getCollation())) {
                appendAlterAttribute(attrSb, "ALTER", " TYPE ", attr);
            }
        }

        for (AbstractColumn attr : attrs) {
            if (newCompositeType.getAttr(attr.getName()) == null) {
                appendAlterAttribute(attrSb, "DROP", ",", attr);
            }
        }

        if (!attrSb.isEmpty()) {
            // remove last comma
            attrSb.setLength(attrSb.length() - 1);
            script.addStatement("ALTER TYPE " + getQualifiedName() + attrSb);
            isNeedDepcies.set(true);
        }
    }

    @Override
    public void appendAlterComments(AbstractStatement newObj, SQLScript script) {
        super.appendAlterComments(newObj, script);
        appendAlterChildrenComments(newObj, script);
    }

    private void appendAlterChildrenComments(AbstractStatement newObj, SQLScript script) {
        PgCompositeType newType = (PgCompositeType) newObj;
        for (AbstractColumn newAttr : newType.attrs) {
            AbstractColumn oldAttr = getAttr(newAttr.getName());
            if (oldAttr != null) {
                oldAttr.appendAlterComments(newAttr, script);
            } else {
                newAttr.appendComments(script);
            }
        }
    }

    private void appendAlterAttribute(StringBuilder attrSb, String action, String delimiter,
                                      AbstractColumn attr) {
        attrSb.append("\n\t").append(action).append(" ATTRIBUTE ")
                .append(PgDiffUtils.getQuotedName(attr.getName()))
                .append(delimiter);
        if (!"DROP".equals(action)) {
            attrSb.append(attr.getType());
            if (attr.getCollation() != null) {
                attrSb.append(COLLATE)
                        .append(attr.getCollation());
            }
            attrSb.append(",");
        }
    }

    /**
     * Returns an attribute by name.
     *
     * @param name attribute name
     * @return attribute or null if not found
     */
    public AbstractColumn getAttr(String name) {
        for (AbstractColumn att : attrs) {
            if (att.getName().equals(name)) {
                return att;
            }
        }
        return null;
    }

    /**
     * Returns an unmodifiable list of all attributes.
     *
     * @return list of attributes
     */
    public List<AbstractColumn> getAttrs() {
        return Collections.unmodifiableList(attrs);
    }

    /**
     * Adds an attribute to this composite type.
     *
     * @param attr attribute to add
     */
    public void addAttr(AbstractColumn attr) {
        attrs.add(attr);
        attr.setParent(this);
        resetHash();
    }

    @Override
    protected AbstractType getTypeCopy() {
        PgCompositeType copy = new PgCompositeType(name);
        for (AbstractColumn attr : attrs) {
            copy.addAttr((AbstractColumn) attr.deepCopy());
        }
        return copy;
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj instanceof PgCompositeType type) {
            return super.compare(type) && attrs.equals(type.attrs);
        }
        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.putOrdered(attrs);
    }
}
