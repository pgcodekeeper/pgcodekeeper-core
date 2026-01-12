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
package org.pgcodekeeper.core.utils;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Wrapper for creation and automatic recursive deletion of a temporary directory.
 * Intended for try-with-resources usage to ensure proper cleanup.
 * Automatically deletes the directory and all its contents when closed.
 *
 * @author Alexander Levsha
 */
public final class TempDir implements AutoCloseable {

    private final Path dir;

    /**
     * Creates a temporary directory with specified prefix in the system temp directory.
     *
     * @param prefix the directory name prefix
     * @throws IOException if directory creation fails
     */
    public TempDir(String prefix) throws IOException {
        this.dir = FileUtils.createTempDirectory(prefix);
    }

    /**
     * Creates a temporary directory with specified prefix in the given parent directory.
     *
     * @param dir    the parent directory
     * @param prefix the directory name prefix
     * @throws IOException if directory creation fails
     */
    public TempDir(Path dir, String prefix) throws IOException {
        this.dir = FileUtils.createTempDirectory(dir, prefix);
    }

    /**
     * Returns the path to the temporary directory.
     *
     * @return path to the temporary directory
     */
    public Path get() {
        return dir;
    }

    @Override
    public void close() throws IOException {
        FileUtils.deleteRecursive(dir);
    }
}
