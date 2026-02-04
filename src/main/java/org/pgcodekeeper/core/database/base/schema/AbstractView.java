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
package org.pgcodekeeper.core.database.base.schema;

import java.util.*;
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Abstract base class for database views.
 * Provides common functionality for views across different database types including.
 */
@Deprecated
public abstract class AbstractView extends AbstractStatementContainer {

    @Override
    public DbObjType getStatementType() {
        return DbObjType.VIEW;
    }

    protected AbstractView(String name) {
        super(name);
    }

    @Override
    public void addConstraint(AbstractConstraint constraint) {
        // no op
        // throw error later?
    }

    @Override
    public AbstractConstraint getConstraint(String name) {
        return null;
    }

    @Override
    public List<AbstractConstraint> getConstraints() {
        return Collections.emptyList();
    }

    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        return Stream.empty();
    }

    @Override
    public boolean compare(IStatement obj) {
        return this == obj || obj instanceof AbstractView && super.compare(obj);
    }

    @Override
    protected AbstractStatementContainer getCopy() {
        return getViewCopy();
    }

    protected abstract AbstractView getViewCopy();
}