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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.database.pg.schema;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.schema.IOperator;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.api.schema.DbObjType;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL database implementation.
 * Manages database-level objects such as schemas, extensions, event triggers,
 * foreign data wrappers, servers, user mappings, and casts.
 *
 * @author fordfrog
 */
public final class PgDatabase extends AbstractDatabase implements IPgStatement {

    private final Map<String, PgExtension> extensions = new LinkedHashMap<>();
    private final Map<String, PgEventTrigger> eventTriggers = new LinkedHashMap<>();
    private final Map<String, PgForeignDataWrapper> fdws = new LinkedHashMap<>();
    private final Map<String, PgServer> servers = new LinkedHashMap<>();
    private final Map<String, PgUserMapping> userMappings = new LinkedHashMap<>();
    private final Map<String, PgCast> casts = new LinkedHashMap<>();

    /**
     * Creates a new PostgreSQL database.
     */
    public PgDatabase() {
        super();
    }

    @Override
    protected void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        super.fillChildrenList(l);
        l.add(extensions.values());
        l.add(eventTriggers.values());
        l.add(fdws.values());
        l.add(servers.values());
        l.add(userMappings.values());
        l.add(casts.values());
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case SCHEMA -> getSchema(name);
            case EXTENSION -> getExtension(name);
            case EVENT_TRIGGER -> getEventTrigger(name);
            case FOREIGN_DATA_WRAPPER -> getForeignDW(name);
            case SERVER -> getServer(name);
            case USER_MAPPING -> getChildByName(userMappings, name);
            case CAST -> getCast(name);
            default -> null;
        };
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case SCHEMA:
                addSchema((AbstractSchema) st);
                break;
            case EXTENSION:
                addExtension((PgExtension) st);
                break;
            case EVENT_TRIGGER:
                addEventTrigger((PgEventTrigger) st);
                break;
            case FOREIGN_DATA_WRAPPER:
                addForeignDW((PgForeignDataWrapper) st);
                break;
            case SERVER:
                addServer((PgServer) st);
                break;
            case CAST:
                addCast((PgCast) st);
                break;
            case USER_MAPPING:
                addUserMapping((PgUserMapping) st);
                break;
            default:
                throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    /**
     * Returns extension of given name or null if the extension has not been found.
     *
     * @param name extension name
     * @return found extension or null
     */
    public PgExtension getExtension(final String name) {
        return getChildByName(extensions, name);
    }

    private void addExtension(final PgExtension extension) {
        addUnique(extensions, extension);
    }

    public PgEventTrigger getEventTrigger(final String name) {
        return getChildByName(eventTriggers, name);
    }

    private void addEventTrigger(final PgEventTrigger et) {
        addUnique(eventTriggers, et);
    }

    /**
     * Returns foreign data wrapper of given name or null if the foreign data wrapper has not been found.
     *
     * @param name foreign data wrapper name
     * @return found foreign data wrapper or null
     */
    public PgForeignDataWrapper getForeignDW(final String name) {
        return getChildByName(fdws, name);
    }

    private void addForeignDW(final PgForeignDataWrapper fDW) {
        addUnique(fdws, fDW);
    }

    public PgServer getServer(final String name) {
        return getChildByName(servers, name);
    }

    private void addServer(final PgServer server) {
        addUnique(servers, server);
    }

    private void addUserMapping(final PgUserMapping userMapping) {
        addUnique(userMappings, userMapping);
    }

    /**
     * Returns cast of given name or null if the cast has not been found.
     *
     * @param name cast name
     * @return found cast or null
     */
    public PgCast getCast(final String name) {
        return casts.get(name);
    }

    private void addCast(final PgCast cast) {
        addUnique(casts, cast);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof PgDatabase db && super.compareChildren(obj)) {
            return extensions.equals(db.extensions)
                    && eventTriggers.equals(db.eventTriggers)
                    && fdws.equals(db.fdws)
                    && servers.equals(db.servers)
                    && casts.equals(db.casts);
        }
        return false;
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        super.computeChildrenHash(hasher);
        hasher.putUnordered(extensions);
        hasher.putUnordered(eventTriggers);
        hasher.putUnordered(fdws);
        hasher.putUnordered(servers);
        hasher.putUnordered(casts);
    }

    @Override
    protected void concat(AbstractStatement st) {
        DbObjType type = st.getStatementType();
        String name = st.getName();
        if (type == DbObjType.SCHEMA && Consts.PUBLIC.equals(name) && !st.hasChildren()) {
            // skip empty public schema
            return;
        }

        super.concat(st);
    }

    /**
     * Sorts columns in all tables within all schemas of this database.
     * This is used to ensure consistent column ordering in inherited tables.
     */
    public void sortColumns() {
        for (AbstractSchema schema : getSchemas()) {
            schema.getTables().forEach(t -> ((PgAbstractTable) t).sortColumns());
        }
    }

    @Override
    protected boolean isFirstLevelType(DbObjType type) {
        return type.in(DbObjType.SCHEMA, DbObjType.EXTENSION, DbObjType.FOREIGN_DATA_WRAPPER, DbObjType.EVENT_TRIGGER,
                DbObjType.SERVER, DbObjType.USER_MAPPING, DbObjType.CAST);
    }

    @Override
    protected AbstractDatabase getDatabaseCopy() {
        return new PgDatabase();
    }

    @Override
    protected IOperator resolveOperatorCall(AbstractSchema abstractSchema, String table) {
        PgSchema schema = (PgSchema) abstractSchema;
        IOperator oper = null;
        if (table.indexOf('(') != -1) {
            oper = schema.getOperator(table);
        }
        if (oper != null) {
            return oper;
        }

        int found = 0;
        for (IOperator o : schema.getOperators()) {
            if (o.getBareName().equals(table)) {
                ++found;
                oper = o;
            }
        }
        return found == 1 ? oper : null;
    }
}