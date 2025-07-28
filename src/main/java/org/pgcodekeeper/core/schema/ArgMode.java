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
package org.pgcodekeeper.core.schema;

import java.util.Locale;

/**
 * Enumeration of function argument modes.
 * Defines how function parameters are passed and returned across different database types.
 */
public enum ArgMode {
    IN,
    INOUT,
    OUT,
    VARIADIC,
    // MS SQL
    OUTPUT;

    /**
     * Checks if this argument mode represents an input parameter.
     *
     * @return true if the mode is IN, INOUT, or VARIADIC
     */
    public boolean isIn() {
        return this == IN || this == INOUT || this == VARIADIC;
    }

    /**
     * Converts a string representation to an ArgMode enum value.
     *
     * @param string the string to convert
     * @return the corresponding ArgMode enum value
     */
    public static ArgMode of(String string) {
        String s = string.toLowerCase(Locale.ROOT);
        return switch (s) {
            case "in", "i" -> IN;
            case "out", "o" -> OUT;
            case "inout", "b" -> INOUT;
            case "variadic", "v" -> VARIADIC;
            case "output" -> OUTPUT;
            default -> valueOf(string);
        };
    }
}
