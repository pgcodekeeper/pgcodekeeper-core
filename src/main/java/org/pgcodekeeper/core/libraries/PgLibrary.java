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
package org.pgcodekeeper.core.libraries;

import java.util.Objects;

/**
 * Represents a database library with its metadata and properties.
 * This class encapsulates information about a library including its name,
 * file system path, privilege ignore flag, and owner information.
 */
public class PgLibrary {

    private final String name;
    private final String path;
    private final boolean isIgnorePriv;
    private final String owner;

    /**
     * Constructs a new PgLibrary instance.
     *
     * @param name         the name of the library
     * @param path         the file system path to the library
     * @param isIgnorePriv whether to ignore privileges for this library
     * @param owner        the owner of the library
     */
    public PgLibrary(String name, String path, boolean isIgnorePriv, String owner) {
        this.name = name;
        this.path = path;
        this.isIgnorePriv = isIgnorePriv;
        this.owner = owner;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public boolean isIgnorePriv() {
        return isIgnorePriv;
    }

    public String getOwner() {
        return owner;
    }

    /**
     * Gets the display title for the library.
     * Returns the library name if not blank, otherwise returns the path.
     *
     * @return the display title
     */
    public String getTitle() {
        if (!name.isBlank()) {
            return name;
        }

        return path;
    }

    /**
     * Computes the hash code based on the library title.
     *
     * @return the computed hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(getTitle());
    }

    /**
     * Compares this library with another object for equality.
     * Two libraries are considered equal if their titles match.
     *
     * @param obj the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PgLibrary other)) {
            return false;
        }
        return Objects.equals(getTitle(), other.getTitle());
    }
}