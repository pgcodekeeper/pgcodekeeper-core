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
package org.pgcodekeeper.core.database.base.loader;

import org.pgcodekeeper.core.database.api.schema.ISchema;
import org.pgcodekeeper.core.database.base.jdbc.JdbcRunner;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.antlr.v4.runtime.Parser;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.loader.IJdbcLoader;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.exception.MonitorCancelledRuntimeException;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrTaskManager;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base JDBC database loader
 */
public abstract class AbstractJdbcLoader extends AbstractLoader implements IJdbcLoader {

    protected final IMonitor monitor;
    protected final JdbcRunner runner;
    protected final IJdbcConnector connector;
    protected final IgnoreSchemaList ignoreSchemaList;
    protected final Map<Object, AbstractSchema> schemaIds = new HashMap<>();

    private GenericColumn currentObject;

    protected Map<Long, String> cachedRolesNamesByOid;
    protected Connection connection;
    protected Statement statement;

    protected AbstractJdbcLoader(IJdbcConnector connector, IMonitor monitor, ISettings settings, IgnoreSchemaList ignoreSchemaList) {
        super(settings);
        this.monitor = monitor;
        this.connector = connector;
        this.runner = new JdbcRunner(monitor);
        this.ignoreSchemaList = ignoreSchemaList;
    }

    protected <T, P extends Parser> void submitAntlrTask(BiFunction<List<Object>, String, P> parserCreateFunction,
                                                         Function<P, T> parserCtxReader, Consumer<T> finalizer) {
        String location = getCurrentLocation();
        GenericColumn object = this.currentObject;
        List<Object> list = new ArrayList<>();
        AntlrTaskManager.submit(antlrTasks, () -> {
            IMonitor.checkCancelled(monitor);
            P p = parserCreateFunction.apply(list, location);
            return parserCtxReader.apply(p);
        }, t -> {
            errors.addAll(list);
            if (monitor.isCanceled()) {
                throw new MonitorCancelledRuntimeException();
            }
            setCurrentObject(object);
            finalizer.accept(t);
        });
    }

    public void setOwner(AbstractStatement st, String owner) {
        if (!settings.isIgnorePrivileges()) {
            st.setOwner(owner);
        }
    }

    public void setCurrentOperation(String operation) {
        currentObject = null;
        currentOperation = operation;
        debug("%s", currentOperation);
    }

    public void setCurrentObject(GenericColumn currentObject) {
        this.currentObject = currentObject;
        debug(Messages.JdbcLoaderBase_log_current_obj, currentObject);
    }

    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * Associates a schema ID with a schema object.
     *
     * @param schemaId the schema identifier
     * @param schema   the schema object to associate
     */
    public void putSchema(Object schemaId, AbstractSchema schema) {
        schemaIds.put(schemaId, schema);
    }

    public final void setComment(AbstractStatement f, ResultSet res) throws SQLException {
        String comment = res.getString("description");
        if (comment != null && !comment.isEmpty()) {
            f.setComment(Utils.checkNewLines(Utils.quoteString(comment), settings.isKeepNewlines()));
        }
    }

    public int getVersion() {
        return version;
    }

    public JdbcRunner getRunner() {
        return runner;
    }

    public Connection getConnection() {
        return connection;
    }

    public boolean isIgnoredSchema(String schemaName) {
        return ignoreSchemaList != null && !ignoreSchemaList.getNameStatus(schemaName);
    }

    public IMonitor getMonitor() {
        return monitor;
    }

    public String getCurrentLocation() {
        StringBuilder sb = new StringBuilder("jdbc:");
        if (currentObject == null) {
            return sb.append(currentOperation).toString();
        }
        if (currentObject.schema() != null) {
            sb.append('/').append(currentObject.schema());
        }
        if (currentObject.table() != null) {
            sb.append('/').append(currentObject.table());
        }
        if (currentObject.column() != null) {
            sb.append('/').append(currentObject.column());
        }
        return sb.toString();
    }

    public Statement getStatement() {
        return statement;
    }

    /**
     * Returns a string representation of loaded schemas.
     *
     * @return string containing schema information
     */
    public String getSchemas() {
        return schemaIds.keySet().stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    /**
     * Returning {@link ISchema} of some id
     *
     * @param schemaId the schema identifier
     * @return {@link ISchema} - the schema object to associate
     */
    public AbstractSchema getSchema(Object schemaId) {
        return schemaIds.get(schemaId);
    }

    @Override
    protected void prepare() {
        // no imp
    }
}
