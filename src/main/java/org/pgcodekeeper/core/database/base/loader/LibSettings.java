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
package org.pgcodekeeper.core.database.base.loader;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.api.jdbc.ISupportedVersion;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.dependencieslist.Dependency;
import org.pgcodekeeper.core.ignorelist.IgnoreList;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * View of the parent settings for library loading that prevents libraries
 * from adding their own ignore lists and additional dependencies.
 * <p>
 * All state (monitor, ignore lists, errors, version) is shared with the parent
 * settings; only the isIgnorePrivileges flag may differ per library. When loading
 * library schemas, any {@code .pgcodekeeperignore}, {@code .pgcodekeeperignoreschema}
 * or {@code .pgcodekeeperdependencies} files found in library sources are ignored.
 * Libraries inherit these settings from the main database configuration instead.
 */
class LibSettings implements ISettings {

    private final ISettings parent;
    private final boolean isIgnorePrivileges;

    public LibSettings(ISettings parent, boolean isIgnorePrivileges) {
        this.parent = parent;
        this.isIgnorePrivileges = isIgnorePrivileges;
    }

    @Override
    public boolean isIgnorePrivileges() {
        return isIgnorePrivileges;
    }

    @Override
    public void setIgnorePrivileges(boolean ignorePrivileges) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addIgnoreList(Path ignoreListPath) {
        // no impl, libraries must not add their own ignore lists
    }

    @Override
    public void addIgnoreSchemaList(Path ignoreSchemaListPath) {
        // no impl, libraries must not add their own ignore schema lists
    }

    @Override
    public void addAdditionalDependencies(Collection<Dependency> deps) {
        // no impl, libraries must not add their own dependencies
    }

    @Override
    public ISettings copy() {
        ISettings copy = parent.copy();
        copy.setIgnorePrivileges(isIgnorePrivileges);
        return copy;
    }

    @Override
    public IMonitor getMonitor() {
        return parent.getMonitor();
    }

    @Override
    public void setMonitor(IMonitor monitor) {
        // no impl, monitor is inherited from the parent settings
    }

    @Override
    public void clearErrors() {
        // no impl
    }

    @Override
    public void resetVersion() {
        // no impl
    }

    @Override
    public IgnoreList getIgnoreList() {
        return parent.getIgnoreList();
    }

    @Override
    public boolean isAllowedSchema(String schemaName) {
        return parent.isAllowedSchema(schemaName);
    }

    @Override
    public List<Dependency> getAdditionalDependencies() {
        return parent.getAdditionalDependencies();
    }

    @Override
    public List<Object> getErrors() {
        return parent.getErrors();
    }

    @Override
    public void addError(Object error) {
        parent.addError(error);
    }

    @Override
    public void addErrors(Collection<Object> errors) {
        parent.addErrors(errors);
    }

    @Override
    public ISupportedVersion getVersion() {
        return parent.getVersion();
    }

    @Override
    public void setVersion(ISupportedVersion version) {
        parent.setVersion(version);
    }

    @Override
    public boolean isConcurrentlyMode() {
        return parent.isConcurrentlyMode();
    }

    @Override
    public boolean isAddTransaction() {
        return parent.isAddTransaction();
    }

    @Override
    public boolean isGenerateExists() {
        return parent.isGenerateExists();
    }

    @Override
    public boolean isGenerateConstraintNotValid() {
        return parent.isGenerateConstraintNotValid();
    }

    @Override
    public boolean isGenerateExistDoBlock() {
        return parent.isGenerateExistDoBlock();
    }

    @Override
    public boolean isPrintUsing() {
        return parent.isPrintUsing();
    }

    @Override
    public boolean isKeepNewlines() {
        return parent.isKeepNewlines();
    }

    @Override
    public boolean isCommentsToEnd() {
        return parent.isCommentsToEnd();
    }

    @Override
    public boolean isAutoFormatObjectCode() {
        return parent.isAutoFormatObjectCode();
    }

    @Override
    public boolean isIgnoreColumnOrder() {
        return parent.isIgnoreColumnOrder();
    }

    @Override
    public boolean isEnableFunctionBodiesDependencies() {
        return parent.isEnableFunctionBodiesDependencies();
    }

    @Override
    public boolean isDataMovementMode() {
        return parent.isDataMovementMode();
    }

    @Override
    public boolean isDropBeforeCreate() {
        return parent.isDropBeforeCreate();
    }

    @Override
    public boolean isStopNotAllowed() {
        return parent.isStopNotAllowed();
    }

    @Override
    public boolean isSelectedOnly() {
        return parent.isSelectedOnly();
    }

    @Override
    public boolean isIgnoreConcurrentModification() {
        return parent.isIgnoreConcurrentModification();
    }

    @Override
    public boolean isSimplifyView() {
        return parent.isSimplifyView();
    }

    @Override
    public boolean isSimplifyNotNull() {
        return parent.isSimplifyNotNull();
    }

    @Override
    public boolean isDisableCheckFunctionBodies() {
        return parent.isDisableCheckFunctionBodies();
    }

    @Override
    public boolean isParallelLoad() {
        return parent.isParallelLoad();
    }

    @Override
    public boolean isDisableAutoLoad() {
        return parent.isDisableAutoLoad();
    }

    @Override
    public String getInCharsetName() {
        return parent.getInCharsetName();
    }

    @Override
    public String getTimeZone() {
        return parent.getTimeZone();
    }

    @Override
    public IFormatConfiguration getFormatConfiguration() {
        return parent.getFormatConfiguration();
    }

    @Override
    public Collection<DbObjType> getAllowedTypes() {
        return parent.getAllowedTypes();
    }

    @Override
    public Collection<String> getPreFilePath() {
        return parent.getPreFilePath();
    }

    @Override
    public Collection<String> getPostFilePath() {
        return parent.getPostFilePath();
    }

    @Override
    public String getClusterName() {
        return parent.getClusterName();
    }

    @Override
    public boolean isUseActualVersionSyntax() {
        return parent.isUseActualVersionSyntax();
    }
}
