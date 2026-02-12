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
 * Wrapper for creation and automatic deletion of a temporary file.
 * Intended for try-with-resources usage to ensure proper cleanup.
 * Automatically deletes the file when closed.
 *
 * @author Alexander Levsha
 */
public final class TempFile implements AutoCloseable {

    private final Path f;

    /**
     * Creates a temporary file with specified prefix and suffix in the given directory.
     *
     * @param dir    the directory to create file in
     * @param prefix the file name prefix
     * @param suffix the file name suffix
     * @throws IOException if file creation fails
     */
    public TempFile(Path dir, String prefix, String suffix) throws IOException {
        this.f = FileUtils.createTempFile(dir, prefix, suffix);
    }

    /**
     * Returns the path to the temporary file.
     *
     * @return path to the temporary file
     */
    public Path get() {
        return f;
    }

    @Override
    public void close() throws IOException {
        FileUtils.removeReadOnly(f);
    }
}
