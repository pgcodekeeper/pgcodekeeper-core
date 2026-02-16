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
package org.pgcodekeeper.core.database.ms.parser.expr;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser.*;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.utils.*;

/**
 * Abstract base class for analyzing Microsoft SQL expressions with namespace support.
 * Provides functionality for managing table aliases, common table expressions (CTEs),
 * and resolving object references within a specific namespace context.
 *
 * @param <T> analyzed expression, should be extension of ParserRuleContext or a rulectx wrapper class
 * @author levsha_aa
 */
public abstract class MsAbstractExprWithNmspc<T> extends MsAbstractExpr {

    /**
     * The local namespace of this Select.<br>
     * String-Reference pairs keep track of external table aliases and
     * names.<br>
     * String-null pairs keep track of internal query names that have only the
     * Alias.
     */
    private final Map<String, ObjectReference> namespace = new HashMap<>();
    /**
     * Unaliased namespace keeps track of tables that have no Alias.<br>
     * It has to be separate since same-named unaliased tables from different
     * schemas can be used, requiring qualification.
     */
    private final Set<ObjectReference> unaliasedNamespace = new HashSet<>();
    /**
     * CTE names that current level of FROM has access to.
     */
    private final Set<String> cte = new HashSet<>();

    protected MsAbstractExprWithNmspc(String schema, IMetaContainer meta) {
        super(schema, meta);
    }

    protected MsAbstractExprWithNmspc(MsAbstractExpr parent) {
        super(parent);
    }

    @Override
    protected boolean hasCte(String cteName) {
        return cte.contains(cteName) || super.hasCte(cteName);
    }

    @Override
    public Entry<String, ObjectReference> findReference(String schema, String name, String column) {
        Entry<String, ObjectReference> ref = findReferenceInNmspc(schema, name);
        return ref == null ? super.findReference(schema, name, column) : ref;
    }

    protected Entry<String, ObjectReference> findReferenceInNmspc(String schema, String name) {
        String loweredName = name.toLowerCase(Locale.ROOT);
        boolean found;
        ObjectReference dereferenced = null;
        if (schema == null && namespace.containsKey(loweredName)) {
            found = true;
            dereferenced = namespace.get(loweredName);
        } else if (!unaliasedNamespace.isEmpty()) {
            // simple empty check to save some allocations
            // it will almost always be empty
            for (ObjectReference unaliased : unaliasedNamespace) {
                if (unaliased.table().equalsIgnoreCase(loweredName) &&
                        (schema == null || unaliased.schema().equalsIgnoreCase(schema))) {
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
    protected Pair<IRelation, Pair<String, String>> findColumn(String name) {
        Pair<IRelation, Pair<String, String>> ret = findColumn(name, namespace.values());
        if (ret == null) {
            ret = findColumn(name, unaliasedNamespace);
        }
        return ret != null ? ret : super.findColumn(name);
    }

    private Pair<IRelation, Pair<String, String>> findColumn(String name, Collection<ObjectReference> refs) {
        for (ObjectReference ref : refs) {
            if (ref == null) {
                continue;
            }
            IRelation rel = findRelation(ref.schema(), ref.table());
            if (rel == null) {
                continue;
            }

            Stream<Pair<String, String>> columns = rel.getRelationColumns();
            for (Pair<String, String> col : Utils.streamIterator(columns)) {
                if (col.getFirst().equals(name)) {
                    return new Pair<>(rel, col);
                }
            }
        }
        return null;
    }

    /**
     * Adds a reference to the namespace with the specified alias.
     * Clients may use this to setup pseudo-variable names before expression analysis.
     *
     * @param alias  the alias name for the reference
     * @param object the database object being referenced, may be null for internal query names
     * @return true if the reference was added successfully, false if alias already exists
     */
    public boolean addReference(String alias, ObjectReference object) {
        String aliasCi = alias.toLowerCase(Locale.ROOT);
        boolean exists = namespace.containsKey(aliasCi);
        if (exists) {
            log(DUPLICATE_ALIASES, aliasCi);
        } else {
            namespace.put(aliasCi, object);
        }
        return !exists;
    }

    /**
     * Adds an unaliased table reference to the namespace.
     * Used for tables that don't have explicit aliases in the FROM clause.
     *
     * @param qualifiedTable the fully qualified table reference to add
     */
    public void addRawTableReference(ObjectReference qualifiedTable) {
        boolean exists = !unaliasedNamespace.add(qualifiedTable);
        if (exists) {
            log(Messages.MsAbstractExprWithNmspc_log_dupl_unaliased_table, qualifiedTable.schema(), qualifiedTable.table());
        }
    }

    protected ObjectReference addNameReference(Qualified_nameContext name, As_table_aliasContext alias) {
        String firstName = name.name.getText();

        boolean isCte = name.DOT().isEmpty() && hasCte(firstName);
        ObjectReference depcy = null;
        if (!isCte) {
            depcy = addObjectDepcy(name, DbObjType.TABLE);
        }

        if (alias != null) {
            if (depcy != null) {
                // add alias definition
                addVariable(depcy, alias.id());
            }
            String aliasName = alias.id().getText();
            addReference(aliasName, depcy);
        } else if (isCte) {
            addReference(firstName, null);
        } else {
            addRawTableReference(depcy);
        }

        return depcy;
    }

    protected void analyzeCte(With_expressionContext with) {
        for (Common_table_expressionContext withQuery : with.common_table_expression()) {
            new MsSelect(this).analyze(withQuery.select_statement());

            String withName = withQuery.id().getText();
            if (!cte.add(withName)) {
                log(withQuery.id(), Messages.AbstractExprWithNmspc_log_dupl_cte, withName);
            }
        }
    }

    /**
     * Analyzes the given rule context and returns a list of column names.
     * Implementations should process the specific Microsoft SQL expression type
     * and extract relevant database dependencies and column information.
     *
     * @param ruleCtx the parser rule context to analyze
     * @return list of column names found during analysis
     */
    public abstract List<String> analyze(T ruleCtx);
}
