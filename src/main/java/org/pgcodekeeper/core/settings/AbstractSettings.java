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
import org.pgcodekeeper.core.dependencieslist.Dependency;
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

/**
 * Base implementation of {@link ISettings} that owns the per-operation state:
 * progress monitor, ignore lists, additional dependencies, error accumulator and
 * detected database version.
 */
public abstract class AbstractSettings implements ISettings {

    private final List<Object> errors = new ArrayList<>();
    private final IgnoreList ignoreList = new IgnoreList();
    private final IgnoreSchemaList ignoreSchemaList = new IgnoreSchemaList();
    private final List<Dependency> additionalDependencies = new ArrayList<>();

    private IMonitor monitor = new NullMonitor();
    private ISupportedVersion version;

    @Override
    public IMonitor getMonitor() {
        return monitor;
    }

    @Override
    public void setMonitor(IMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public IgnoreList getIgnoreList() {
        return ignoreList;
    }

    @Override
    public void addIgnoreList(Path ignoreListPath) throws IOException {
        IIgnoreList.parseIgnoreList(ignoreListPath, ignoreList);
    }

    @Override
    public void addIgnoreSchemaList(Path ignoreSchemaListPath) throws IOException {
        IIgnoreList.parseIgnoreList(ignoreSchemaListPath, ignoreSchemaList);
    }

    @Override
    public boolean isAllowedSchema(String schemaName) {
        return ignoreSchemaList.getNameStatus(schemaName);
    }

    @Override
    public List<Dependency> getAdditionalDependencies() {
        return additionalDependencies;
    }

    @Override
    public void addAdditionalDependencies(Collection<Dependency> deps) {
        additionalDependencies.addAll(deps);
    }

    @Override
    public List<Object> getErrors() {
        return errors;
    }

    @Override
    public void addError(Object error) {
        errors.add(error);
    }

    @Override
    public void addErrors(Collection<Object> errors) {
        this.errors.addAll(errors);
    }

    @Override
    public void clearErrors() {
        errors.clear();
    }

    @Override
    public ISupportedVersion getVersion() {
        return version;
    }

    @Override
    public void setVersion(ISupportedVersion version) {
        if (null == this.version || !this.version.isLE(version.getVersion())) {
            this.version = version;
        }
    }

    @Override
    public void resetVersion() {
        this.version = null;
    }

    @Override
    public ISettings copy() {
        var copy = shallowCopy();
        copy.version = version;
        copy.errors.addAll(errors);
        copy.additionalDependencies.addAll(additionalDependencies);
        copy.ignoreList.addAll(ignoreList.getList());
        copy.ignoreSchemaList.addAll(ignoreSchemaList.getList());
        return null;
    }

    abstract AbstractSettings shallowCopy();
}
