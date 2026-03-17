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
package org.pgcodekeeper.core;

/**
 * Stores string constants
 *
 * @author Anton Ryabinin
 */
public final class Consts {

    /**
     * Prefer using StandardCharsets instead of this String representation.
     */
    public static final String UTF_8 = "UTF-8";
    public static final String UTC = "UTC";

    public static final String POOL_SIZE = "ru.taximaxim.codekeeper.parser.poolsize";

    public static final String FILENAME_WORKING_DIR_MARKER = ".pgcodekeeper";
    public static final String VERSION_PROP_NAME = "version";
    public static final String EXPORT_CURRENT_VERSION = "0.6.0";

    public static final String JDBC_SUCCESS = "success";
    public static final String SQL_POSTFIX = ".sql";

    private Consts() {
    }
}
