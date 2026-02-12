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

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;

import java.util.*;

/**
 * Abstract base class for database objects that can contain other statements.
 * Provides common functionality for containers like tables and views that can have
 * indexes, triggers, rules, policies, and constraints as child objects.
 */
public abstract class PgAbstractStatementContainer extends PgAbstractStatement
        implements IRelation, IStatementContainer, ISearchPath {

    private final Map<String, PgIndex> indexes = new LinkedHashMap<>();
    private final Map<String, PgTrigger> triggers = new LinkedHashMap<>();
    private final Map<String, PgRule> rules = new LinkedHashMap<>();
    private final Map<String, PgPolicy> policies = new LinkedHashMap<>();

    protected PgAbstractStatementContainer(String name) {
        super(name);
    }

    @Override
    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        l.add(indexes.values());
        l.add(triggers.values());
        l.add(rules.values());
        l.add(policies.values());
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case INDEX -> getIndex(name);
            case TRIGGER -> getTrigger(name);
            case RULE -> getRule(name);
            case POLICY -> getPolicy(name);
            default -> null;
        };
    }

    @Override
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        return switch (type) {
            case INDEX -> Collections.unmodifiableCollection(indexes.values());
            case TRIGGER -> Collections.unmodifiableCollection(triggers.values());
            case RULE -> Collections.unmodifiableCollection(rules.values());
            case POLICY -> Collections.unmodifiableCollection(policies.values());
            default -> List.of();
        };
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case INDEX:
                addIndex((PgIndex) st);
                break;
            case TRIGGER:
                addTrigger((PgTrigger) st);
                break;
            case RULE:
                addRule((PgRule) st);
                break;
            case POLICY:
                addPolicy((PgPolicy) st);
                break;
            default:
                throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    /**
     * Checks if this container has any clustered indexes or constraints.
     */
    public boolean isClustered() {
        for (PgIndex ind : getIndexes()) {
            if (ind.isClustered()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Finds index according to specified index {@code name}.
     *
     * @param name name of the index to be searched
     * @return found index or null if no such index has been found
     */
    public PgIndex getIndex(final String name) {
        return getChildByName(indexes, name);
    }

    /**
     * Finds trigger according to specified trigger {@code name}.
     *
     * @param name name of the trigger to be searched
     * @return found trigger or null if no such trigger has been found
     */
    public PgTrigger getTrigger(final String name) {
        return getChildByName(triggers, name);
    }

    /**
     * Finds rule according to specified rule {@code name}.
     *
     * @param name name of the rule to be searched
     * @return found rule or null if no such rule has been found
     */
    public PgRule getRule(final String name) {
        return getChildByName(rules, name);
    }

    /**
     * Finds policy according to specified policy {@code name}.
     *
     * @param name name of the policy to be searched
     * @return found policy or null if no such policy has been found
     */
    public PgPolicy getPolicy(String name) {
        return getChildByName(policies, name);
    }

    public abstract Collection<PgConstraint> getConstraints();

    /**
     * Getter for {@link #indexes}. The list cannot be modified.
     *
     * @return {@link #indexes}
     */
    public Collection<PgIndex> getIndexes() {
        return Collections.unmodifiableCollection(indexes.values());
    }

    /**
     * Getter for {@link #triggers}. The list cannot be modified.
     *
     * @return {@link #triggers}
     */
    public Collection<PgTrigger> getTriggers() {
        return Collections.unmodifiableCollection(triggers.values());
    }

    /**
     * Getter for {@link #policies}. The list cannot be modified.
     *
     * @return {@link #policies}
     */
    public Collection<PgPolicy> getPolicies() {
        return Collections.unmodifiableCollection(policies.values());
    }

    /**
     * Adds an index to this container.
     *
     * @param index the index to add
     */
    private void addIndex(final PgIndex index) {
        addUnique(indexes, index);
    }

    /**
     * Adds a trigger to this container.
     *
     * @param trigger the trigger to add
     */
    private void addTrigger(final PgTrigger trigger) {
        addUnique(triggers, trigger);
    }

    /**
     * Adds a rule to this container.
     *
     * @param rule the rule to add
     */
    private void addRule(final PgRule rule) {
        addUnique(rules, rule);
    }

    /**
     * Adds a policy to this container.
     *
     * @param policy the policy to add
     */
    private void addPolicy(PgPolicy policy) {
        addUnique(policies, policy);
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        hasher.putUnordered(indexes);
        hasher.putUnordered(triggers);
        hasher.putUnordered(rules);
        hasher.putUnordered(policies);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof PgAbstractStatementContainer cont) {
            return indexes.equals(cont.indexes)
                    && triggers.equals(cont.triggers)
                    && rules.equals(cont.rules)
                    && policies.equals(cont.policies);
        }

        return false;
    }
}
