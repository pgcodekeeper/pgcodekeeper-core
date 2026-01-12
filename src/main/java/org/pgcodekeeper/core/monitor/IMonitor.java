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
package org.pgcodekeeper.core.monitor;

/**
 * Interface for monitoring progress and cancellation of long-running operations.
 * Provides support for work progress tracking, cancellation state management,
 * and hierarchical monitoring through sub-monitors.
 */
public interface IMonitor {
    /**
     * Sets the cancellation state of this monitor.
     *
     * @param cancelled {@code true} to cancel the operation, {@code false} otherwise
     */
    void setCanceled(boolean cancelled);

    /**
     * Returns whether this monitor has been canceled.
     *
     * @return {@code true} if the operation has been canceled, {@code false} otherwise
     */
    boolean isCanceled();

    /**
     * Notifies that a given number of work units have been completed.
     *
     * @param i the number of work units completed
     */
    void worked(int i);

    /**
     * Creates a sub-monitor for tracking a portion of this monitor's work.
     *
     * @return a new sub-monitor instance
     */
    IMonitor createSubMonitor();

    /**
     * Sets the number of work units remaining for this monitor.
     *
     * @param size the number of work units remaining
     */
    void setWorkRemaining(int size);

    /**
     * Checks if progress monitor has been cancelled.
     *
     * @param monitor the progress monitor to check
     * @throws InterruptedException if monitor is cancelled
     */
    static void checkCancelled(IMonitor monitor)
            throws InterruptedException {
        if (monitor != null && monitor.isCanceled()) {
            throw new InterruptedException();
        }
    }
}
