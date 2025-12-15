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
package org.pgcodekeeper.core.database.base.parser.antlr;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.parsers.antlr.base.CodeUnitToken;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.api.schema.IRelation;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;
import org.pgcodekeeper.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.pgcodekeeper.core.database.api.schema.ObjectLocation.Builder;
import static org.pgcodekeeper.core.database.api.schema.ObjectLocation.LocationType;

/**
 * Abstract base class for SQL expression analysis.
 */
public abstract class AbstractExpr {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractExpr.class);

    private final Set<ObjectLocation> dependencies;

    // TODO get current version.
    // Need to get version. I already got it from JdbcLoader(READER)
    // and put it to the 'PgDatabase' as currentPostgreSqlVersion,
    // but I couldn't get it from PgDumpLoader(WRITER), that's why for
    // cases with 'PgDumpLoader(WRITER)' the version was hard-coded in 'AbstractDatabase'.
    protected final MetaContainer meta;

    protected final AbstractExpr parent;

    /**
     * Base constructor
     *
     * @param parent parent scope
     * @param dependencies set for dependencies
     * @param meta meta storage
     */
    protected AbstractExpr(AbstractExpr parent, Set<ObjectLocation> dependencies, MetaContainer meta) {
        this.parent = parent;
        this.dependencies = dependencies;
        this.meta = meta;
    }

    /**
     * Constructor for child scope
     *
     * @param parent parent scope
     */
    protected AbstractExpr(AbstractExpr parent) {
        this(parent, parent.dependencies, parent.meta);
    }

    /**
     * Constructor for root scope
     *
     * @param meta meta storage
     */
    protected AbstractExpr(MetaContainer meta) {
        this(null, new LinkedHashSet<>(), meta);
    }

    /**
     * Gets an unmodifiable set of database object dependencies found in this expression.
     *
     * @return set of {@link ObjectLocation} representing dependencies
     */
    public Set<ObjectLocation> getDependencies() {
        return Collections.unmodifiableSet(dependencies);
    }

    /**
     * @return true if this is a child element
     */
    protected final boolean hasParent() {
        return parent != null;
    }

    /**
     * @param cteName cte name
     * @return true if a cte with the given name exists in the current scope
     */
    protected boolean hasCte(String cteName) {
        return hasParent() && parent.hasCte(cteName);
    }

    /**
     * @param cteName cte name
     * @return cte for the given name if it exists in the current scope
     */
    protected List<Pair<String, String>> findCte(String cteName) {
        return hasParent() ? parent.findCte(cteName) : null;
    }

    /**
     * @param schema optional schema qualification of name, may be null
     * @param name   alias of the referenced object
     * @param column optional referenced column alias, may be null
     * @return a pair of (Alias, Dealiased name) where Alias is the given name.
     * Dealiased name can be null if the name is internal to the query
     * and is not a reference to external table.<br>
     * null if the name is not found
     */
    public Map.Entry<String, GenericColumn> findReference(String schema, String name, String column) {
        return hasParent() ? parent.findReference(schema, name, column) : null;
    }

    /**
     * @param name column name
     * @return columns and their types for the given name, if it exists in the current scope
     */
    protected Pair<IRelation, Pair<String, String>> findColumn(String name) {
        return hasParent() ? parent.findColumn(name) : null;
    }

    /**
     * Adds a dependency
     *
     * @param dependency dependency
     */
    public void addDependency(ObjectLocation dependency) {
        dependencies.add(dependency);
    }

    /**
     * Adds a dependency with parser rule context as a reference
     *
     * @param genericColumn object location
     * @param ctx parser rule context
     */
    public void addReference(GenericColumn genericColumn, ParserRuleContext ctx) {
        addDependency(genericColumn, ctx, LocationType.LOCAL_REF);
    }

    /**
     * Adds a dependency with parser rule context as a variable
     *
     * @param genericColumn object location
     * @param ctx rule context
     */
    public void addVariable(GenericColumn genericColumn, ParserRuleContext ctx) {
        addDependency(genericColumn, ctx, LocationType.VARIABLE);
    }

    /**
     * Adds a dependency with parser rule context and dependency type
     *
     * @param genericColumn object location
     * @param ctx rule context
     * @param type dependency type
     */
    public void addDependency(GenericColumn genericColumn, ParserRuleContext ctx, LocationType type) {
        addDependency(new Builder()
                .setObject(genericColumn)
                .setCtx(ctx)
                .setLocationType(type)
                .setAlias(ctx.getText())
                .build());
    }

    /**
     * Adds a non-system dependency
     *
     * @param genericColumn object location
     * @param ctx rule context
     */
    public void addDependency(GenericColumn genericColumn, ParserRuleContext ctx) {
        if (isSystemSchema(genericColumn.schema())) {
            return;
        }

        var dep = new Builder()
                .setObject(genericColumn)
                .setCtx(ctx)
                .build();
        addDependency(dep);
    }

    /**
     * Checks whether the schema is systemic.
     *
     * @param schema schema name
     * @return true if the schema is a system schema.
     */
    protected abstract boolean isSystemSchema(String schema);

    /**
     * Looks up a relation in the meta store.
     *
     * @param schemaName schema name
     * @param relationName relation name
     * @return the found relation or null
     */
    protected IRelation findRelation(String schemaName, String relationName) {
        return meta.findRelation(getSchemaName(schemaName), relationName);
    }

    /**
     * Returns schema name to use to search for objects in the metastore. By default, returns given schema name.
     * Subclasses can override this behavior.
     *
     * @param schemaName schema name
     * @return schema name
     */
    protected String getSchemaName(String schemaName) {
        return schemaName;
    }

    /**
     * Logs a message without context
     *
     * @param msg message
     * @param args optional arguments for message
     */
    protected void log(String msg, Object... args) {
        log(null, msg, args);
    }

    /**
     * Logs a message with context
     *
     * @param ctx parser rule context
     * @param msg message
     * @param args optional arguments for message
     */
    protected void log(ParserRuleContext ctx, String msg, Object... args) {
        if (LOG.isDebugEnabled()) {
            String positionInfo = "";
            if (ctx != null) {
                CodeUnitToken token = (CodeUnitToken) ctx.getStart();
                positionInfo = "line %d:%d ".formatted(token.getLine(), token.getCodeUnitPositionInLine());
            }

            var logMsg = positionInfo + msg.formatted(args);
            LOG.debug(logMsg);
        }
    }
}
