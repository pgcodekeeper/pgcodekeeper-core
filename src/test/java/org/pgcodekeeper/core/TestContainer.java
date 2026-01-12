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

import java.time.Duration;

import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

public final class TestContainer {

    private static final String PG_IMAGE = "postgres:16.4-alpine3.20";
    private static final String MS_IMAGE = "mcr.microsoft.com/mssql/server:2022-CU20-GDR1-ubuntu-22.04";
    private static final String CH_IMAGE = "clickhouse/clickhouse-server:24.12.3";

    private static final String TEST_USER = "test";
    private static final String TEST_PASSWORD = "1245789630";
    private static final String PG_CH_URL_POSTFIX = "?user=%s&password=%s".formatted(TEST_USER, TEST_PASSWORD);

    public static final String PG_URL;
    public static final String MS_URL;
    public static final String MS_URL_MEMORY_OPTIMIZED;
    public static final String CH_URL;

    public static final Integer PG_PORT;
    public static final Integer MS_PORT;
    public static final Integer CH_PORT;

    private static final PostgreSQLContainer PG_DB = new PostgreSQLContainer(PG_IMAGE);
    private static final MSSQLServerContainer MS_DB = new MSSQLServerContainer(MS_IMAGE);
    private static final ClickHouseContainer CH_DB = new ClickHouseContainer(CH_IMAGE);

    static {
        PG_DB.withUsername(TEST_USER).withPassword(TEST_PASSWORD);
        MS_DB.withEnv("ACCEPT_EULA", "Y")
        .withInitScript(getInitScriptPath("init_ms_db.sql"))
        .withStartupCheckStrategy(
                new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(10)));
        CH_DB.withUsername(TEST_USER).withPassword(TEST_PASSWORD);

        PG_DB.start();
        MS_DB.start();
        CH_DB.start();

        PG_PORT = PG_DB.getFirstMappedPort();
        MS_PORT = MS_DB.getFirstMappedPort();
        CH_PORT = CH_DB.getFirstMappedPort();

        PG_URL = "jdbc:postgresql://localhost:" + PG_PORT + "/test" + PG_CH_URL_POSTFIX;
        MS_URL = buildMsConnectionUrl("test_db");
        MS_URL_MEMORY_OPTIMIZED = buildMsConnectionUrl("test_db_memory_optimized");
        CH_URL = "jdbc:clickhouse://localhost:" + CH_PORT + "/default" + PG_CH_URL_POSTFIX;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Thread.currentThread().setName("-> shutdown_hook : shutdown containers");
            PG_DB.close();
            MS_DB.close();
            CH_DB.close();
        }));
    }

    private static String getInitScriptPath(String fileName) {
        return TestContainer.class.getPackage().getName().replace('.', '/') + '/' + fileName;
    }

    private static String buildMsConnectionUrl(String databaseName) {
        return "jdbc:sqlserver://localhost:" + MS_PORT +
                ";databaseName=" + databaseName +
                ";user=sa;password=A_Str0ng_Required_Password";
    }

    private TestContainer() {
    }
}
