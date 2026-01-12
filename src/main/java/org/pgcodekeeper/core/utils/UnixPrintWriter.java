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

import java.io.*;
import java.nio.charset.Charset;

/**
 * A {@link PrintWriter} implementation that ensures Unix-style line endings ('\n')
 * regardless of the platform.
 */
public class UnixPrintWriter extends PrintWriter {

    private boolean autoFlush = false;

    /**
     * Creates a new UnixPrintWriter without automatic line flushing.
     *
     * @param out the underlying writer
     */
    public UnixPrintWriter(Writer out) {
        super(out);
    }

    /**
     * Creates a new UnixPrintWriter from an OutputStream.
     *
     * @param out the underlying output stream
     */
    public UnixPrintWriter(OutputStream out) {
        super(out);
    }

    /**
     * Creates a new UnixPrintWriter that writes to a file.
     *
     * @param fileName path to the output file
     * @throws FileNotFoundException if the file cannot be opened
     */
    public UnixPrintWriter(String fileName) throws FileNotFoundException {
        super(fileName);
    }

    /**
     * Creates a new UnixPrintWriter that writes to a file.
     *
     * @param file the output file
     * @throws FileNotFoundException if the file cannot be opened
     */
    public UnixPrintWriter(File file) throws FileNotFoundException {
        super(file);
    }

    /**
     * Creates a new UnixPrintWriter with optional auto-flush.
     *
     * @param out       the underlying writer
     * @param autoFlush if true, flushes after every println call
     */
    public UnixPrintWriter(Writer out, boolean autoFlush) {
        super(out, autoFlush);
        this.autoFlush = autoFlush;
    }

    /**
     * Creates a new UnixPrintWriter from an OutputStream with optional auto-flush.
     *
     * @param out       the underlying output stream
     * @param autoFlush if true, flushes after every println call
     */
    public UnixPrintWriter(OutputStream out, boolean autoFlush) {
        super(out, autoFlush);
        this.autoFlush = autoFlush;
    }

    /**
     * Creates a new UnixPrintWriter that writes to a file with specified charset.
     *
     * @param fileName path to the output file
     * @param csn      charset name for encoding
     * @throws FileNotFoundException        if the file cannot be opened
     * @throws UnsupportedEncodingException if the charset is not supported
     */
    public UnixPrintWriter(String fileName, String csn) throws FileNotFoundException,
    UnsupportedEncodingException {
        super(fileName, csn);
    }

    /**
     * Creates a new UnixPrintWriter that writes to a file with specified charset.
     *
     * @param file the output file
     * @param csn  charset name for encoding
     * @throws FileNotFoundException        if the file cannot be opened
     * @throws UnsupportedEncodingException if the charset is not supported
     */
    public UnixPrintWriter(File file, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(file, csn);
    }

    /**
     * Creates a new UnixPrintWriter from an OutputStream with specified charset.
     *
     * @param out the underlying output stream
     * @param csn charset name for encoding
     * @throws UnsupportedEncodingException if the charset is not supported
     */
    public UnixPrintWriter(OutputStream out, String csn) throws UnsupportedEncodingException {
        this(new BufferedWriter(new OutputStreamWriter(out, csn)), false);
    }

    /**
     * Creates a new UnixPrintWriter that writes to a file with specified charset.
     *
     * @param fileName path to the output file
     * @param csn      charset for encoding
     * @throws FileNotFoundException if the file cannot be opened
     */
    public UnixPrintWriter(String fileName, Charset csn) throws FileNotFoundException {
        this(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(fileName)), csn)), false);
    }

    /**
     * Creates a new UnixPrintWriter that writes to a file with specified charset.
     *
     * @param file the output file
     * @param csn  charset for encoding
     * @throws FileNotFoundException if the file cannot be opened
     */
    public UnixPrintWriter(File file, Charset csn) throws FileNotFoundException {
        this(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), csn)), false);
    }

    /**
     * {@inheritDoc}
     * <br><br>
     * This version always terminates with '\n'.
     */
    @Override
    public void println() {
        try {
            synchronized (lock) {
                print('\n');
                if (autoFlush) {
                    out.flush();
                }
            }
        }
        catch (InterruptedIOException x) {
            Thread.currentThread().interrupt();
        }
        catch (IOException x) {
            setError();
        }
    }
}
