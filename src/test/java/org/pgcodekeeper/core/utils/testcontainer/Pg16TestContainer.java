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
package org.pgcodekeeper.core.utils.testcontainer;

import org.pgcodekeeper.core.database.pg.jdbc.SupportedPgVersion;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

final class Pg16TestContainer implements ITestContainer {

    private static final String TEST_USER = "test";
    private static final String TEST_PASSWORD = "1245789630";

    @Override
    public SupportedPgVersion getVersion() {
        return SupportedPgVersion.VERSION_16;
    }

    @Override
    public String getDraftUrl() {
        return "jdbc:postgresql://localhost:%s/test" + "?user=%s&password=%s".formatted(TEST_USER, TEST_PASSWORD);
    }

    @Override
    public GenericContainer<?> getTestContainer() {
        return new PostgreSQLContainer("postgres:16.4-alpine3.20")
                .withUsername(TEST_USER)
                .withPassword(TEST_PASSWORD);
    }
}