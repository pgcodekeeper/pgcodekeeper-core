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
package org.pgcodekeeper.core.database.base.project;

import org.pgcodekeeper.core.database.api.project.IDirRule;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IStatement;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Describes how database objects are placed in the project's directory layout.
 * One rule covers either all objects of a given {@link DbObjType} (generic rule)
 * or a narrower subset matched by a predicate (specific rule, e.g. materialized
 * views inside {@code VIEW}). Used both for default layouts and for overrides
 * loaded from {@code structure.properties}.
 */
public class DirRule implements IDirRule {

    private final DbObjType type;
    private final boolean isSubElement;
    private final boolean isSpecific;
    private final Predicate<IStatement> predicate;
    private String dirName;

    /**
     * Creates a rule with an explicit matching predicate.
     *
     * @param dirName      directory name where matching objects are stored
     * @param type         the object type this rule applies to
     * @param isSubElement {@code true} if objects of this type are nested under a schema
     * @param isSpecific   {@code true} if this rule targets a narrow subset of objects
     *                     and should take priority over generic rules for the same type
     * @param predicate    test that decides whether a given statement matches this rule
     */
    public DirRule(String dirName, DbObjType type, boolean isSubElement, boolean isSpecific,
                   Predicate<IStatement> predicate) {
        this.dirName = dirName;
        this.type = type;
        this.isSubElement = isSubElement;
        this.isSpecific = isSpecific;
        this.predicate = predicate;
    }

    /**
     * Creates a generic rule that matches all objects of the given type.
     *
     * @param dirName      directory name where matching objects are stored
     * @param type         the object type this rule applies to
     * @param isSubElement {@code true} if objects of this type are nested under a schema
     */
    public DirRule(String dirName, DbObjType type, boolean isSubElement) {
        this(dirName, type, isSubElement, false, st -> st.getStatementType() == type);
    }

    @Override
    public String getDirName() {
        return dirName;
    }

    public void setDirName(String dirName) {
        this.dirName = dirName;
    }

    public DbObjType getType() {
        return type;
    }

    public boolean isSpecific() {
        return isSpecific;
    }

    public Predicate<IStatement> getPredicate() {
        return predicate;
    }

    @Override
    public boolean isSubElement() {
        return isSubElement;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dirName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return obj instanceof DirRule other
            && Objects.equals(dirName, other.dirName);
    }
}
