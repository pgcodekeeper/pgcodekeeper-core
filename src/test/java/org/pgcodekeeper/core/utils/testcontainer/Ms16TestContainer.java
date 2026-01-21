package org.pgcodekeeper.core.utils.testcontainer;

import org.pgcodekeeper.core.database.pg.jdbc.SupportedPgVersion;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;
import org.testcontainers.mssqlserver.MSSQLServerContainer;

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
import java.time.Duration;

final class Ms16TestContainer implements ITestContainer {

    @Override
    public SupportedPgVersion getVersion() {
        return null;
    }

    @Override
    public String getDraftUrl() {
        return "jdbc:sqlserver://localhost:%s;databaseName=%s;user=sa;password=A_Str0ng_Required_Password";
    }

    @Override
    public GenericContainer<?> getTestContainer() {
        return new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-CU20-GDR1-ubuntu-22.04")
                .withEnv("ACCEPT_EULA", "Y")
                .withInitScript(getInitScriptPath("init_ms_db.sql"))
                .withStartupCheckStrategy(
                        new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(10)));
    }

    private static String getInitScriptPath(String fileName) {
        return TestContainerType.class.getPackage().getName().replace('.', '/') + '/' + fileName;
    }
}