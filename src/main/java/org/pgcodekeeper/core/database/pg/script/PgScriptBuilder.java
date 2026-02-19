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
package org.pgcodekeeper.core.database.pg.script;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.script.AbstractScriptBuilder;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.graph.ActionContainer;
import org.pgcodekeeper.core.model.graph.ActionsToScriptConverter;
import org.pgcodekeeper.core.script.SQLActionType;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.Utils;

public class PgScriptBuilder extends AbstractScriptBuilder {

    public PgScriptBuilder(DiffSettings diffSettings) {
        super(diffSettings);
    }

    @Override
    protected String getScript(Set<ActionContainer> actions, Set<IStatement> toRefresh, List<TreeElement> selected,
                               IDatabase oldDb, IDatabase newDb)
            throws IOException {
        var settings = getSettings();
        SQLScript script = new SQLScript(settings, oldDb.getSeparator());
        for (String preFilePath : settings.getPreFilePath()) {
            addPrePostPath(script, preFilePath, SQLActionType.PRE);
        }

        if (settings.getTimeZone() != null) {
            script.addStatement("SET TIMEZONE TO " + Utils.quoteString(settings.getTimeZone()), //$NON-NLS-1$
                    SQLActionType.BEGIN);
        }

        if (settings.isDisableCheckFunctionBodies()) {
            script.addStatement("SET check_function_bodies = false", SQLActionType.BEGIN); //$NON-NLS-1$
        }

        if (settings.isAddTransaction()) {
            script.addStatement("START TRANSACTION", SQLActionType.BEGIN); //$NON-NLS-1$
        }

        script.addStatement("SET search_path = pg_catalog", SQLActionType.BEGIN); //$NON-NLS-1$
        ActionsToScriptConverter.fillScript(script, actions, toRefresh, oldDb, newDb, selected);

        if (settings.isAddTransaction()) {
            script.addStatement("COMMIT TRANSACTION", SQLActionType.END); //$NON-NLS-1$
        }

        for (String postFilePath : settings.getPostFilePath()) {
            addPrePostPath(script, postFilePath, SQLActionType.POST);
        }
        return script.getFullScript();
    }

    private void addPrePostPath(SQLScript script, String scriptPath, SQLActionType actionType) throws IOException {
        Path path = Paths.get(scriptPath);
        addPrePostPath(script, path, actionType);
    }

    private void addPrePostPath(SQLScript script, Path path, SQLActionType actionType) throws IOException {
        if (Files.isRegularFile(path)) {
            addPrePostScript(script, path, actionType);
            return;
        }
        Stream<Path> stream = Files.list(path).sorted();
        for (Path child : Utils.streamIterator(stream)) {
            addPrePostPath(script, child, actionType);
        }
    }

    private void addPrePostScript(SQLScript script, Path fileName, SQLActionType actionType) throws IOException {
        try {
            String prePostScript = Files.readString(fileName);
            prePostScript = "-- " + fileName + "\n\n" + prePostScript; //$NON-NLS-1$ //$NON-NLS-2$
            script.addStatementWithoutSeparator(prePostScript, actionType);
        } catch (IOException e) {
            throw new IOException(Messages.PgDiff_read_error + e.getLocalizedMessage(), e);
        }
    }
}
