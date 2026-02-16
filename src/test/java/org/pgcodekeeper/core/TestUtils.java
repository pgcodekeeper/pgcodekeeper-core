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

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import org.junit.jupiter.api.Assertions;

public final class TestUtils {

    static {
        // explicit locale for tests with localization
        Locale.setDefault(Locale.ENGLISH);
    }

    public static String readResource(String resourceName, Class<?> clazz) throws IOException {
        try (InputStream is = clazz.getResourceAsStream(resourceName)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    public static Path getFilePath(String resourceName, Class<?> clazz) {
        var resource = clazz.getResource(resourceName);
        if (resource == null) {
            throw new IllegalArgumentException("Test resource not found: " + resourceName);
        }

        try {
            return Paths.get(resource.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid resource URI: " + resourceName, e);
        }
    }

    public static void assertIgnoreNewLines(String expected, String actual) {
        Assertions.assertLinesMatch(
                expected.lines(),
                actual.lines());
    }

    private TestUtils() {
    }
}
