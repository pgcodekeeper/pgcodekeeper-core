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
package org.pgcodekeeper.core.monitor;

/**
 * A null object implementation of {@link IMonitor} that provides no-op behavior
 * for all monitoring operations except cancellation state management.
 * This class can be used when monitoring functionality
 * is not required but an {@link IMonitor} instance must be provided.
 */
public class NullMonitor implements IMonitor {

    private volatile boolean cancelled = false;

    @Override
    public void setCanceled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    @Override
    public boolean isCanceled() {
        return cancelled;
    }

    @Override
    public void worked(int i) {
        // no impl
    }

    @Override
    public IMonitor createSubMonitor() {
        return new NullMonitor();
    }

    @Override
    public void setWorkRemaining(int size) {
        // no impl
    }
}
