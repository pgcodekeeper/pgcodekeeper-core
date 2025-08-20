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
package org.pgcodekeeper.core.parsers.antlr.base;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.utils.DaemonThreadFactory;
import org.pgcodekeeper.core.exception.MonitorCancelledRuntimeException;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Manages execution and completion of asynchronous ANTLR parsing tasks.
 * Uses a fixed thread pool for parallel parsing operations.
 */
public final class AntlrTaskManager {

    private static final int POOL_SIZE = Integer.max(1,
            Integer.getInteger(Consts.POOL_SIZE, Runtime.getRuntime().availableProcessors() - 1));

    private static final ExecutorService ANTLR_POOL =
            Executors.newFixedThreadPool(POOL_SIZE, new DaemonThreadFactory());

    /**
     * Gets the size of the thread pool used for ANTLR parsing tasks.
     *
     * @return number of threads in the pool
     */
    public static int getPoolSize() {
        return POOL_SIZE;
    }

    /**
     * Submits a parsing task for asynchronous execution.
     *
     * @param <T>  type of the parsing result
     * @param task the parsing task to execute
     * @return Future representing the pending result
     */
    public static <T> Future<T> submit(Callable<T> task) {
        return ANTLR_POOL.submit(task);
    }

    /**
     * Submits a parsing task with completion handler.
     *
     * @param <T>        type of the parsing result
     * @param antlrTasks queue to store the created task
     * @param task       the parsing task to execute
     * @param finalizer  consumer to process the result when complete
     */
    public static <T> void submit(Queue<AntlrTask<?>> antlrTasks, Callable<T> task, Consumer<T> finalizer) {
        Future<T> future = submit(task);
        antlrTasks.add(new AntlrTask<>(future, finalizer));
    }

    /**
     * Processes all tasks in the queue until completion or failure.
     *
     * @param antlrTasks queue of tasks to process
     * @throws InterruptedException if task processing was interrupted
     * @throws IOException          if an I/O error occurred during parsing
     */
    public static void finish(Queue<AntlrTask<?>> antlrTasks) throws InterruptedException, IOException {
        AntlrTask<?> task;
        try {
            while ((task = antlrTasks.poll()) != null) {
                task.finish();
            }
        } catch (ExecutionException ex) {
            handleAntlrTaskException(ex);
        } catch (MonitorCancelledRuntimeException ex) {
            // finalizing parser listeners' cancellations will reach here
            throw new InterruptedException();
        }
    }

    /**
     * Unwraps and rethrows specific exceptions from ExecutionException wrapper.
     * Handles InterruptedException and IOException by rethrowing them directly.
     * All other exceptions are wrapped in IllegalStateException.
     *
     * @param ex the ExecutionException to unwrap
     * @throws InterruptedException if the task was interrupted
     * @throws IOException if an I/O error occurred during parsing
     * @throws IllegalStateException for any other exception types
     */
    private static void handleAntlrTaskException(ExecutionException ex)
            throws InterruptedException, IOException {
        Throwable t = ex.getCause();
        if (t instanceof InterruptedException in) {
            throw in;
        }
        if (t instanceof IOException io) {
            throw io;
        }

        throw new IllegalStateException(ex);
    }

    private AntlrTaskManager() {
        // only static
    }
}