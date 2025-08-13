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
package org.pgcodekeeper.core.library;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Enumerates possible source types for PostgreSQL libraries.
 * Determines how a library is accessed and where it's located.
 */
public enum PgLibrarySource {
    LOCAL,
    JDBC,
    URL;

    /**
     * Determines the source type from a library path string.
     *
     * @param libPath the path/URL to analyze
     * @return the determined source type (LOCAL if path doesn't match JDBC or URL patterns)
     */
    public static PgLibrarySource getSource(String libPath) {
        if (libPath.startsWith("jdbc:")) {
            return PgLibrarySource.JDBC;
        }
        try {
            URI uri = new URI(libPath);
            if (uri.getScheme() != null) {
                return PgLibrarySource.URL;
            }
        } catch (URISyntaxException e) {
            // not URI, try to folder or file
        }
        return PgLibrarySource.LOCAL;
    }
}
