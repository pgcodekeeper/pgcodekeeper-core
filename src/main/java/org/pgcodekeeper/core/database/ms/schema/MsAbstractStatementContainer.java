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
package org.pgcodekeeper.core.database.ms.schema;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;

import java.util.*;

/**
 * Abstract base class for database objects that can contain other statements.
 * Provides common functionality for containers like tables and views that can have
 * indexes, triggers, rules, policies, and constraints as child objects.
 */
public abstract class MsAbstractStatementContainer extends MsAbstractStatement
        implements IRelation, IStatementContainer, ISearchPath {

    private final Map<String, MsTrigger> triggers = new LinkedHashMap<>();
    private final Map<String, MsIndex> indexes = new LinkedHashMap<>();
    private final Map<String, MsStatistics> statistics = new HashMap<>();

    protected MsAbstractStatementContainer(String name) {
        super(name);
    }

    @Override
    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        l.add(indexes.values());
        l.add(triggers.values());
        l.add(statistics.values());
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case INDEX -> getChildByName(indexes, name);
            case TRIGGER -> getChildByName(triggers, name);
            case STATISTICS -> getChildByName(statistics, name);
            default -> null;
        };
    }

    @Override
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        return switch (type) {
            case INDEX -> Collections.unmodifiableCollection(indexes.values());
            case TRIGGER -> Collections.unmodifiableCollection(triggers.values());
            case STATISTICS -> Collections.unmodifiableCollection(statistics.values());
            default -> List.of();
        };
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case INDEX -> addUnique(indexes, (MsIndex) st);
            case TRIGGER -> addUnique(triggers, (MsTrigger) st);
            case STATISTICS -> addUnique(statistics, (MsStatistics) st);
            default -> throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    /**
     * Checks if this container has any clustered indexes or constraints.
     */
    public boolean isClustered() {
        for (MsIndex ind : indexes.values()) {
            if (ind.isClustered()) {
                return true;
            }
        }

        return false;
    }

    public MsTrigger getTrigger(final String name) {
        return triggers.get(name);
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        hasher.putUnordered(indexes);
        hasher.putUnordered(triggers);
        hasher.putUnordered(statistics);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        return obj instanceof MsAbstractStatementContainer table
                && indexes.equals(table.indexes)
                && triggers.equals(table.triggers)
                && statistics.equals(table.statistics);
    }
}
