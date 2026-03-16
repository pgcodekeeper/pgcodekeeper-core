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
package org.pgcodekeeper.core.api;
import static org.mockito.Mockito.verify;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.pgcodekeeper.core.database.api.loader.ILoader;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.utils.Utils;

/**
 * Tests for parallel and sequential database loading behavior in {@link Utils#loadDatabases}.
 *
 * <p>Verifies the following scenarios:
 * <ul>
 *   <li>IOException propagation when one of the loaders fails during parallel load</li>
 *   <li>Monitor cancellation on loading error</li>
 * </ul>
 *
 * <p>Uses Mockito to simulate {@link ILoader} and {@link IMonitor} behavior.
 */
class ParallelLoadTest {

    private ILoader oldDbLoader;
    private ILoader newDbLoader;
    private IMonitor subMonitor;
    private IDatabase oldDb;
    private IDatabase newDb;

    @BeforeEach
    void initSettings() {
        oldDbLoader = mock(ILoader.class);
        newDbLoader = mock(ILoader.class);
        subMonitor = mock(IMonitor.class);
        oldDb = mock(IDatabase.class);
        newDb = mock(IDatabase.class);
    }

    @Test
    void testOneThreadWithException() throws IOException, InterruptedException {
        IOException ioException = new IOException("old DB read error");
        when(oldDbLoader.loadAndAnalyze()).thenThrow(ioException);
        when(newDbLoader.loadAndAnalyze()).thenReturn(newDb);

        IOException thrown = assertThrows(IOException.class, () ->
                Utils.loadDatabases(oldDbLoader, newDbLoader, subMonitor, true));

        assertEquals(ioException, thrown);
        verify(subMonitor).setCancelled(true);
    }

    @Test
    void testAnotherThreadWithException() throws IOException, InterruptedException {
        when(oldDbLoader.loadAndAnalyze()).thenReturn(oldDb);
        IOException ioException = new IOException("new DB read error");
        when(newDbLoader.loadAndAnalyze()).thenThrow(ioException);

        IOException thrown = assertThrows(IOException.class,
                () -> Utils.loadDatabases(oldDbLoader, newDbLoader, subMonitor, true));

        assertEquals(ioException, thrown);
        verify(subMonitor).setCancelled(true);
    }
}
