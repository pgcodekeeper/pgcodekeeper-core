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
package org.pgcodekeeper.core.settings;

import java.util.Collection;

import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.formatter.FormatConfiguration;
import org.pgcodekeeper.core.model.difftree.DbObjType;

public class CoreSettings implements ISettings {

    @Override
    public DatabaseType getDbType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isConcurrentlyMode() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isAddTransaction() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isGenerateExists() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isConstraintNotValid() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isGenerateExistDoBlock() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isPrintUsing() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isKeepNewlines() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isCommentsToEnd() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isAutoFormatObjectCode() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isIgnorePrivileges() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isIgnoreColumnOrder() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isEnableFunctionBodiesDependencies() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isDataMovementMode() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isDropBeforeCreate() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isStopNotAllowed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isSelectedOnly() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isIgnoreConcurrentModification() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isSimplifyView() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isDisableCheckFunctionBodies() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getInCharsetName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTimeZone() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FormatConfiguration getFormatConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<DbObjType> getAllowedTypes() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<String> getPreFilePath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<String> getPostFilePath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ISettings createTempSettings(boolean isIgnorePriv) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ISettings createTempSettings(String inCharsetName) {
        // TODO Auto-generated method stub
        return null;
    }

}
