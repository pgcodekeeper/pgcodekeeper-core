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

import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.ch.loader.ChDumpLoader;
import org.pgcodekeeper.core.database.ms.loader.MsDumpLoader;
import org.pgcodekeeper.core.database.pg.loader.PgDumpLoader;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

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

    public static Path getPathToResource(String resourceName, Class<?> clazz) throws URISyntaxException {
        URL url = clazz.getResource(resourceName);
        return Paths.get(url.toURI());
    }

    public static String getFilePath(String resourceName, Class<?> clazz) {
        var resource = clazz.getResource(resourceName);
        if (resource == null) {
            throw new IllegalArgumentException("Test resource not found: " + resourceName);
        }

        try {
            return Paths.get(resource.toURI()).toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid resource URI: " + resourceName, e);
        }
    }

    public static IDatabase loadDatabaseFromDump(String pathToFile, ISettings settings)
            throws IOException, InterruptedException {
        var path = Path.of(pathToFile);
        return (switch (settings.getDbType()) {
            case PG -> new PgDumpLoader(path, settings);
            case MS -> new MsDumpLoader(path, settings);
            case CH -> new ChDumpLoader(path, settings);
        }).load();
    }

    private TestUtils() {
    }
}
