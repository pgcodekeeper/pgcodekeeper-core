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
package org.pgcodekeeper.core.parsers.antlr.ch.expr;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.*;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.IRelation;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.utils.Pair;

import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.Map.Entry;

/**
 * Abstract class extending ChAbstractExpr with namespace support for SQL expression analysis.
 * Handles table aliases, CTEs (Common Table Expressions) and namespace management.
 */
public abstract class ChAbstractExprWithNmspc<T> extends ChAbstractExpr {

    /**
     * The local namespace of this Select.<br>
     * String-Reference pairs keep track of external table aliases and names.<br>
     * String-null pairs keep track of internal query names that have only the Alias.
     */
    private final Map<String, GenericColumn> namespace = new HashMap<>();
    /**
     * Unaliased namespace keeps track of tables that have no Alias.<br>
     * It has to be separate since same-named unaliased tables from different schemas can be used, requiring
     * qualification.
     */
    private final Set<GenericColumn> unaliasedNamespace = new HashSet<>();

    /**
     * CTE names that current level of FROM has access to.
     */
    private final Set<String> cte = new HashSet<>();

    /**
     * This variable is used to isolate references at current level.
     * We need it to not lose the dependencies in with_clauseContext
     */
    private final boolean isLocalScope;

    protected ChAbstractExprWithNmspc(String schema, MetaContainer meta) {
        super(schema, meta);
        this.isLocalScope = false;
    }

    protected ChAbstractExprWithNmspc(ChAbstractExpr parent, boolean isLocalScope) {
        super(parent);
        this.isLocalScope = isLocalScope;
    }

    @Override
    protected boolean hasCte(String cteName) {
        return cte.contains(cteName) || super.hasCte(cteName);
    }

    @Override
    public Entry<String, GenericColumn> findReference(String schema, String name, String column) {
        Entry<String, GenericColumn> ref = findReferenceInNmspc(schema, name);
        return ref == null ? super.findReference(schema, name, column) : ref;
    }

    private Entry<String, GenericColumn> findReferenceInNmspc(String schema, String name) {
        boolean found;
        GenericColumn dereferenced = null;
        if (schema == null && namespace.containsKey(name)) {
            found = true;
            dereferenced = namespace.get(name);
        } else if (!unaliasedNamespace.isEmpty()) {
            // simple empty check to save some allocations
            // it will almost always be empty
            for (GenericColumn unaliased : unaliasedNamespace) {
                if (unaliased.table.equalsIgnoreCase(name) &&
                        (schema == null || unaliased.schema.equalsIgnoreCase(schema))) {
                    if (dereferenced == null) {
                        dereferenced = unaliased;
                        if (schema != null) {
                            // fully qualified, no ambiguity search needed
                            break;
                        }
                    } else {
                        log(Messages.AbstractExprWithNmspc_log_ambiguos_ref, name);
                    }
                }
            }
            found = dereferenced != null;
        } else {
            found = false;
        }

        return found ? new SimpleEntry<>(name, dereferenced) : null;
    }

    @Override
    protected void addReferenceInRootParent(Qualified_nameContext name, Alias_clauseContext alias, boolean isFrom) {
        if (isLocalScope || !hasParent()) {
            addNameReference(name, alias, isFrom);
        } else {
            super.addReferenceInRootParent(name, alias, isFrom);
        }
    }

    /**
     * Clients may use this to setup pseudo-variable names before expression
     * analysis.
     */
    private void addReference(String alias, GenericColumn object) {
        if (namespace.containsKey(alias)) {
            log(Consts.DUPLICATE_ALIASES, alias);
            return;
        }
        namespace.put(alias, object);
    }

    /**
     * Adds a reference with the given alias
     *
     * @param alias the alias name to register
     */
    public void addReference(String alias) {
        addReference(alias, null);
    }

    /**
     * Adds an unaliased table reference to the namespace.
     *
     * @param qualifiedTable the table reference to add
     */
    public void addRawTableReference(GenericColumn qualifiedTable) {
        boolean exists = !unaliasedNamespace.add(qualifiedTable);
        if (exists) {
            log(Messages.ChAbstractExprWithNmspc_log_dupl_unaliased_table, qualifiedTable.schema, qualifiedTable.table);
        }
    }

    @Override
    protected Pair<IRelation, Pair<String, String>> findColumn(String name) {
        Pair<IRelation, Pair<String, String>> ret = findColumn(name, namespace.values());
        if (ret == null) {
            ret = findColumn(name, unaliasedNamespace);
        }
        return ret != null ? ret : super.findColumn(name);
    }

    private Pair<IRelation, Pair<String, String>> findColumn(String name, Collection<GenericColumn> refs) {
        for (GenericColumn ref : refs) {
            if (ref != null) {
                IRelation rel = findRelation(ref.schema, ref.table);
                if (rel == null) {
                    continue;
                }

                Pair<IRelation, Pair<String, String>> pair = rel.getRelationColumns()
                        .filter(e -> e.getFirst().equals(name))
                        .findFirst().map(e -> new Pair<>(rel, e))
                        .orElse(null);
                if (pair != null) {
                    return pair;
                }
            }
        }
        return null;
    }

    protected void analyzeCte(With_clauseContext with) {
        if (with == null) {
            return;
        }

        for (With_queryContext withQuery : with.with_query()) {
            String withName = withQuery.name.getText();
            var expr = withQuery.expr();
            if (expr != null) {
                new ChValueExpr(this).analyze(expr);
            } else {
                Dml_stmtContext data = withQuery.dml_stmt();
                var select = data.select_stmt();
                if (select != null) {
                    new ChSelect(this).analyze(select);
                }
            }

            if (!cte.add(withName)) {
                log(withQuery.name, Messages.AbstractExprWithNmspc_log_dupl_cte, withName);
            }
        }
    }

    private void addNameReference(Qualified_nameContext name, Alias_clauseContext alias, boolean isFrom) {
        String firstName = QNameParser.getFirstName(name.identifier());
        boolean isCte = name.DOT().isEmpty() && hasCte(firstName);
        GenericColumn depcy = null;
        if (!isCte && isFrom) {
            depcy = addObjectDepcy(name);
        }

        if (alias != null) {
            ParserRuleContext aliasCtx = getAliasCtx(alias);
            if (depcy != null) {
                // add alias definition
                addVariable(depcy, aliasCtx);
            }
            addReference(aliasCtx.getText(), depcy);
        } else if (isCte) {
            addReference(firstName, null);
        } else {
            addRawTableReference(depcy);
        }

    }

    protected ParserRuleContext getAliasCtx(Alias_clauseContext alias) {
        ParserRuleContext aliasCtx = alias.identifier();
        if (aliasCtx == null) {
            aliasCtx = alias.id_token();
        }
        return aliasCtx;
    }

    /**
     * Analyzes the given SQL context and returns a list of processing results.
     * Must be implemented by concrete subclasses.
     *
     * @param ruleCtx the ANTLR context to analyze
     * @return list of analysis results (implementation-dependent)
     */
    public abstract List<String> analyze(T ruleCtx);
}
