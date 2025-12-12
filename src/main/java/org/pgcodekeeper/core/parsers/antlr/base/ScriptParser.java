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
package org.pgcodekeeper.core.parsers.antlr.base;

import org.pgcodekeeper.core.DangerStatement;
import org.pgcodekeeper.core.loader.ParserListenerMode;
import org.pgcodekeeper.core.loader.PgDumpLoader;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Parses and analyzes SQL scripts, detecting dangerous statements and syntax errors.
 * Provides access to parsed script batches and validation results.
 */
public final class ScriptParser {

    private static final Logger LOG = LoggerFactory.getLogger(ScriptParser.class);
    private final String script;

    private final List<ObjectLocation> batches;
    private final List<Object> errors;
    private final Set<DangerStatement> dangerStatements;

    /**
     * Creates a new script parser and immediately processes the script.
     *
     * @param name     name of the script (for error reporting)
     * @param script   the SQL script content to parse
     * @param settings application settings to use for parsing
     * @throws IOException          if there's an error reading the script
     * @throws InterruptedException if parsing is interrupted
     */
    public ScriptParser(String name, String script, ISettings settings)
            throws IOException, InterruptedException {
        this.script = script;
        LOG.info(Messages.ScriptParser_log_load_dump);
        PgDumpLoader loader = new PgDumpLoader(
                () -> new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8)),
                name, settings, new NullMonitor(), 0);
        loader.setMode(ParserListenerMode.SCRIPT);
        // script mode collects only references
        batches = new ArrayList<>(loader.load().getObjReferences(name));

        dangerStatements = batches.stream()
                .filter(ObjectLocation::isDanger)
                .map(ObjectLocation::getDanger)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(DangerStatement.class)));
        errors = loader.getErrors();
    }

    /**
     * Gets the parsed script batches (statements).
     *
     * @return list of parsed statement locations and metadata
     */
    public List<ObjectLocation> batch() {
        return batches;
    }

    /**
     * Checks if the script contains dangerous DDL statements not in the allowed set.
     *
     * @param allowedDangers collection of dangerous statements that are permitted
     * @return true if script contains unapproved dangerous statements
     */
    public boolean isDangerDdl(Collection<DangerStatement> allowedDangers) {
        return !allowedDangers.containsAll(dangerStatements);
    }

    /**
     * Gets the set of dangerous DDL statements not in the allowed set.
     *
     * @param allowedDangers collection of dangerous statements that are permitted
     * @return set of unapproved dangerous statements found in script
     */
    public Set<DangerStatement> getDangerDdl(Collection<DangerStatement> allowedDangers) {
        Set<DangerStatement> danger = EnumSet.copyOf(dangerStatements);
        danger.removeAll(allowedDangers);
        return danger;
    }

    /**
     * Gets a formatted error message if parsing encountered errors.
     *
     * @return formatted error message string, or null if no errors
     */
    public String getErrorMessage() {
        if (!errors.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Errors while parse script:\n"); //$NON-NLS-1$
            for (Object er : errors) {
                sb.append(er).append('\n');
            }
            return sb.toString();
        }

        return null;
    }

    public String getScript() {
        return script;
    }
}
