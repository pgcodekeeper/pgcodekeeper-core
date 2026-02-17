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
package org.pgcodekeeper.core.database.ms.schema;

import java.util.*;
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Represents a Microsoft SQL view with support for schema binding,
 * ANSI_NULLS and QUOTED_IDENTIFIER settings, and statistics.
 */
public class MsView extends MsAbstractStatementContainer implements MsSourceStatement, IView {

    private boolean ansiNulls;
    private boolean quotedIdentified;
    /**
     * Option that blocks changes to objects on which the view depends without recreating it. <br>
     * <br>
     * Does not participate in comparison, since it is part of {@link #secondPart}
     */
    private boolean schemaBinding;
    private String firstPart;
    private String secondPart;

    /**
     * Creates a new Microsoft SQL view.
     *
     * @param name the view name
     */
    public MsView(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        addViewFullSQL(script, true);
        appendOwnerSQL(script);
        appendPrivileges(script);
    }

    private void addViewFullSQL(SQLScript script, boolean isCreate) {
        final StringBuilder sb = new StringBuilder();
        appendSourceStatement(sb, quotedIdentified, ansiNulls, isCreate);
        script.addStatement(sb);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        MsView newView = (MsView) newCondition;
        boolean isNeedDepcies = false;
        if (ansiNulls != newView.ansiNulls
                || quotedIdentified != newView.quotedIdentified
                || !Objects.equals(firstPart, newView.firstPart)
                || !Objects.equals(secondPart, newView.secondPart)) {
            newView.addViewFullSQL(script, false);
            isNeedDepcies = true;
        }

        appendAlterOwner(newView, script);
        alterPrivileges(newView, script);

        return getObjectState(isNeedDepcies, script, startSize);
    }

    @Override
    public boolean canDropBeforeCreate() {
        return true;
    }

    public void setAnsiNulls(boolean ansiNulls) {
        this.ansiNulls = ansiNulls;
        resetHash();
    }

    public void setQuotedIdentified(boolean quotedIdentified) {
        this.quotedIdentified = quotedIdentified;
        resetHash();
    }

    @Override
    public String getFirstPart() {
        return firstPart;
    }

    @Override
    public void setFirstPart(String firstPart) {
        this.firstPart = firstPart;
        resetHash();
    }

    @Override
    public String getSecondPart() {
        return secondPart;
    }

    @Override
    public void setSecondPart(String secondPart) {
        this.secondPart = secondPart;
        resetHash();
    }

    public void setSchemaBinding(boolean schemaBinding) {
        this.schemaBinding = schemaBinding;
        resetHash();
    }

    public boolean isSchemaBinding() {
        return schemaBinding;
    }

    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        return Stream.empty();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(firstPart);
        hasher.put(secondPart);
        hasher.put(quotedIdentified);
        hasher.put(ansiNulls);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof MsView view
                && super.compare(view)
                && Objects.equals(firstPart, view.firstPart)
                && Objects.equals(secondPart, view.secondPart)
                && quotedIdentified == view.quotedIdentified
                && ansiNulls == view.ansiNulls;
    }

    @Override
    protected MsView getCopy() {
        MsView view = new MsView(name);
        view.setFirstPart(firstPart);
        view.setSecondPart(secondPart);
        view.setAnsiNulls(ansiNulls);
        view.setQuotedIdentified(quotedIdentified);
        view.setSchemaBinding(schemaBinding);
        return view;
    }
}
