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
package org.pgcodekeeper.core.database.base.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.pgcodekeeper.core.exception.MonitorCancelledRuntimeException;
import org.pgcodekeeper.core.monitor.IMonitor;

/**
 * Custom ANTLR parse tree listener that monitors parsing progress and checks for cancellation.
 * Provides basic progress tracking during parsing operations.
 */
public final class CustomParseTreeListener implements ParseTreeListener {
    private final int monitoringLevel;
    private final IMonitor monitor;

    /**
     * Creates a new parse tree listener with progress monitoring.
     *
     * @param monitoringLevel the depth level at which to report progress
     * @param monitor         the progress monitor to report to (cannot be null)
     */
    public CustomParseTreeListener(int monitoringLevel, IMonitor monitor) {
        this.monitoringLevel = monitoringLevel;
        this.monitor = monitor;
    }

    /**
     * Called when entering a terminal node in the parse tree.
     * This implementation does nothing.
     *
     * @param node the terminal node being entered
     */
    @Override
    public void visitTerminal(TerminalNode node) {
        // no imp
    }

    /**
     * Called when visiting an error node in the parse tree.
     * This implementation does nothing.
     *
     * @param node the error node being visited
     */
    @Override
    public void visitErrorNode(ErrorNode node) {
        // no imp
    }

    /**
     * Called when exiting any parser rule in the parse tree.
     * Checks for cancellation and reports progress when exiting rules at or above monitoring level.
     *
     * @param ctx the parser rule context being exited
     * @throws MonitorCancelledRuntimeException if parsing was cancelled
     */
    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        if (ctx.depth() <= monitoringLevel) {
            monitor.worked(1);
            try {
                IMonitor.checkCancelled(monitor);
            } catch (InterruptedException e) {
                throw new MonitorCancelledRuntimeException();
            }
        }
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        // no imp
    }
}
