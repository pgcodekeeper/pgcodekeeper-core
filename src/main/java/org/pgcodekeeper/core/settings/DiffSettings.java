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
package org.pgcodekeeper.core.settings;

import org.pgcodekeeper.core.database.api.jdbc.ISupportedVersion;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.ignorelist.IIgnoreList;
import org.pgcodekeeper.core.ignorelist.IgnoreList;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.monitor.NullMonitor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DiffSettings {

    private final ISettings settings;
    private final List<Object> errors = new ArrayList<>();
    private final IgnoreList ignoreList = new IgnoreList();
    private final IgnoreSchemaList ignoreSchemaList = new IgnoreSchemaList();
    private final List<Map.Entry<IStatement, IStatement>> additionalDependencies = new ArrayList<>();
    private IMonitor monitor;
    private ISupportedVersion version;

    public DiffSettings(ISettings settings, IMonitor monitor) {
        this.settings = settings;
        this.monitor = monitor;
    }

    public DiffSettings(ISettings settings) {
        this(settings, new NullMonitor());
    }

    public DiffSettings() {
        this(new CoreSettings(), new NullMonitor());
    }

    public ISettings getSettings() {
        return settings;
    }

    public IMonitor getMonitor() {
        return monitor;
    }

    public IgnoreList getIgnoreList() {
        return ignoreList;
    }

    public void addIgnoreList(Path ignoreListPath) throws IOException {
        IIgnoreList.parseIgnoreList(ignoreListPath, ignoreList);
    }

    public void addIgnoreSchemaList(Path ignoreSchemaListPath) throws IOException {
        IIgnoreList.parseIgnoreList(ignoreSchemaListPath, ignoreSchemaList);
    }

    public List<Map.Entry<IStatement, IStatement>> getAdditionalDependencies() {
        return additionalDependencies;
    }

    public void addAdditionalDependencies(Collection<Map.Entry<IStatement, IStatement>> deps) {
        additionalDependencies.addAll(deps);
    }

    public List<Object> getErrors() {
        return errors;
    }

    public void addError(Object error) {
        errors.add(error);
    }

    public void addErrors(Collection<Object> errors) {
        this.errors.addAll(errors);
    }

    public void setMonitor(IMonitor monitor) {
        this.monitor = monitor;
    }

    public boolean isAllowedSchema(String schemaName) {
        return ignoreSchemaList.getNameStatus(schemaName);
    }

    public void setVersion(ISupportedVersion version) {
        if (null == this.version || !this.version.isLE(version.getVersion())) {
            this.version = version;
        }
    }

    public ISupportedVersion getVersion() {
        return version;
    }
}
