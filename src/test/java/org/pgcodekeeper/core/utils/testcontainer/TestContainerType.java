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

public enum TestContainerType {

    PG_16(new Pg16TestContainer()),
    MS_16(new Ms16TestContainer()),
    CH_24(new Ch24TestContainer()),
    GP_6(new Gp6TestContainer()),
    GP_7(new Gp7TestContainer());

    private final SupportedPgVersion version;
    private final String draftUrl;
    private final GenericContainer<?> container;

    private String finalUrl;

    TestContainerType(ITestContainer container) {
        this.version = container.getVersion();
        this.draftUrl = container.getDraftUrl();
        this.container = container.getTestContainer();
    }

    public String getMsUrl(Boolean isMemmoryOpt) {
        initContainer();
        return draftUrl.formatted(container.getFirstMappedPort(), isMemmoryOpt ? "test_db_memory_optimized" : "test_db");
    }

    public SupportedPgVersion getVersion() {
        return version;
    }

    public String getUrl() {
        initContainer();
        if (null == finalUrl) {
            finalUrl = draftUrl.formatted(container.getFirstMappedPort());
        }
        return finalUrl;
    }

    private void initContainer() {
        if (container.isCreated()) {
            return;
        }
        container.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Thread.currentThread().setName("-> shutdown_hook : shutdown containers");
            container.close();
        }));
    }
}
