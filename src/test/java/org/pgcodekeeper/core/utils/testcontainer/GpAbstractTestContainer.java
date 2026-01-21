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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

abstract class GpAbstractTestContainer implements ITestContainer {

    abstract String getImageName();

    @Override
    public String getDraftUrl() {
        return "jdbc:postgresql://localhost:%s/postgres?user=gpadmin";
    }

    @Override
    public GenericContainer<?> getTestContainer() {
        return new GenericContainer<>(getImageName())
                .withExposedPorts(5432)
                .waitingFor(Wait.forLogMessage(".*Database successfully started.*\\s", 1));
    }
}