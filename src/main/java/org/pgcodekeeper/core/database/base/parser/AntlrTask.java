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
package org.pgcodekeeper.core.database.base.parser;

import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Wrapper for asynchronous ANTLR parsing tasks that combines a Future with a completion handler.
 * Allows executing parsing tasks in background and processing results when complete.
 *
 * @param <T> type of the result produced by the parsing task
 */
public class AntlrTask<T> {

    private final Future<T> future;
    private final Consumer<T> finalizer;

    /**
     * Creates a new ANTLR task with a Future and completion handler.
     *
     * @param future    the Future representing the asynchronous parsing task
     * @param finalizer consumer that will process the parsing result when task completes
     */
    public AntlrTask(Future<T> future, Consumer<T> finalizer) {
        this.future = future;
        this.finalizer = finalizer;
    }

    /**
     * Waits for the task to complete and processes the result with the finalizer.
     * Propagates any exceptions that occurred during execution.
     *
     * @throws ExecutionException if the computation threw an exception
     */
    public void finish() throws ExecutionException {
        T t;
        try {
            t = future.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ExecutionException(ex);
        }
        finalizer.accept(t);
    }
}
