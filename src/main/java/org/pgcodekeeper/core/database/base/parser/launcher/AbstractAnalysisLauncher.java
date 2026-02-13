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
package org.pgcodekeeper.core.database.base.parser.launcher;

import java.util.*;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.pgcodekeeper.core.database.api.launcher.IAnalysisLauncher;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.base.parser.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.exception.*;
import org.pgcodekeeper.core.localizations.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class and all child classes contains statement, its contexts and
 * implementation of logic for launch the analysis of statement's contexts.
 */
public abstract class AbstractAnalysisLauncher implements IAnalysisLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractAnalysisLauncher.class);

    private final List<ObjectLocation> references = new ArrayList<>();

    protected IStatement stmt;
    private final ParserRuleContext ctx;
    private final String location;

    private int offset;
    private int lineOffset;
    private int inLineOffset;

    protected AbstractAnalysisLauncher(AbstractStatement stmt,
                                       ParserRuleContext ctx, String location) {
        this.stmt = stmt;
        this.ctx = ctx;
        this.location = location;
    }

    @Override
    public IStatement getStmt() {
        return stmt;
    }

    /**
     * Gets the schema name for the statement if available.
     *
     * @return schema name or null if not applicable
     */
    @Override
    public String getSchemaName() {
        if (stmt instanceof ISearchPath path) {
            return path.getSchemaName();
        }

        return null;
    }

    /**
     * Gets the list of object references founded during analysis.
     *
     * @return unmodifiable list of references
     */
    @Override
    public List<ObjectLocation> getReferences() {
        return Collections.unmodifiableList(references);
    }

    public void setOffset(Token codeStart) {
        CodeUnitToken cuToken = (CodeUnitToken) codeStart;
        offset = cuToken.getCodeUnitStart();
        lineOffset = cuToken.getLine() - 1;
        inLineOffset = cuToken.getCodeUnitPositionInLine();
    }

    /**
     * Updates the saved statement to the twin found in the given db
     *
     * @param db
     *            database
     */
    @Override
    public void updateStmt(IDatabase db) {
        if (stmt.getDatabase() != db) {
            // statement came from another DB object, probably a library
            // for proper depcy processing, find its twin in the final DB object

            // twin implies the exact same object type, hence this is safe
            stmt = stmt.getTwin(db);
        }
    }

    /**
     * Launches the analysis of the statement.
     *
     * @param errors list to collect analysis errors
     * @param meta   metadata container for dependency resolution
     * @return set of dependencies found
     */
    @Override
    public Set<ObjectReference> launchAnalyze(List<Object> errors, IMetaContainer meta) {
        // Duplicated objects don't have parent, skip them
        if (stmt.getParent() == null) {
            return Collections.emptySet();
        }

        try {
            Set<ObjectLocation> locs = analyze(ctx, meta);
            Set<ObjectReference> depcies = new LinkedHashSet<>();
            EnumSet<DbObjType> disabledDepcies = getDisabledDepcies();
            for (ObjectLocation loc : locs) {
                if (!disabledDepcies.contains(loc.getType())) {
                    depcies.add(loc.getObjectReference());
                }

                if (loc.getLineNumber() != 0) {
                    references.add(loc.copyWithOffset(offset, lineOffset, inLineOffset, location));
                }
            }
            return depcies;
        } catch (UnresolvedReferenceException ex) {
            Token t = ex.getErrorToken();
            if (t != null) {
                ErrorTypes errorType = ex instanceof MisplacedObjectException ? ErrorTypes.MISPLACEERROR : ErrorTypes.OTHER;
                AntlrError err = new AntlrError(t, location, t.getLine(),
                        ((CodeUnitToken) t).getCodeUnitPositionInLine(), ex.getMessage(), errorType)
                        .copyWithOffset(offset, lineOffset, inLineOffset);
                LOG.warn(err.toString(), ex);
                errors.add(err);
            } else {
                var errorMsg = Messages.AbstractAnalysisLauncher_error_prefix.formatted(location, ex);
                LOG.warn(errorMsg, ex);
                errors.add(errorMsg);
            }
        } catch (Exception ex) {
            var errorMsg = Messages.AbstractAnalysisLauncher_error_prefix.formatted(location, ex);
            LOG.error(errorMsg, ex);
            errors.add(errorMsg);
        }

        return Collections.emptySet();
    }

    /**
     * Gets the set of database object types that should be excluded from dependency analysis.
     * Can be overridden by subclasses to customize which dependency types are ignored.
     * By default, returns an empty set (no types are disabled).
     *
     * @return EnumSet of {@link DbObjType} that should be excluded from dependency collection
     */
    protected EnumSet<DbObjType> getDisabledDepcies() {
        return EnumSet.noneOf(DbObjType.class);
    }

    /**
     * Performs analysis of the given parser context to extract object dependencies.
     * Must be implemented by concrete subclasses to provide specific analysis logic.
     *
     * @param ctx  the parser rule context to analyze
     * @param meta the metadata container providing schema information
     * @return set of object locations representing dependencies found in the context
     * @throws UnresolvedReferenceException if references cannot be resolved
     */
    protected abstract Set<ObjectLocation> analyze(ParserRuleContext ctx, IMetaContainer meta);
}
