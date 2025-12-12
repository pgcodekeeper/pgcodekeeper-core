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
package org.pgcodekeeper.core.database.api.schema;

/**
 * Interface for database objects that support compression options.
 * Provides functionality for managing compression type, level, and block size settings.
 */
public interface ICompressOptionContainer {

    /**
     * Default compression type.
     */
    String DEFAULT_COMPRESS_TYPE = "none";
    
    /**
     * Default compression level.
     */
    int DEFAULT_COMPRESS_LEVEL = 0;
    
    /**
     * Default block size.
     */
    int DEFAULT_BLOCK_SIZE = 32768;

    /**
     * Sets the compression type for this object.
     *
     * @param compressType the compression type to set
     */
    void setCompressType(String compressType);

    /**
     * Sets the compression level for this object.
     *
     * @param compressLevel the compression level to set
     */
    void setCompressLevel(int compressLevel);

    /**
     * Sets the block size for this object.
     *
     * @param blockSize the block size to set
     */
    void setBlockSize(int blockSize);

    /**
     * Parses and fills compression options from a comma-separated string.
     *
     * @param statement the statement to apply options to
     * @param compressOptions the options string to parse
     */
    static void fillCompressOptions(ICompressOptionContainer statement, String compressOptions) {
        for (String pair : compressOptions.split(",")) {
            int sep = pair.indexOf('=');
            if (sep != -1) {
                String option = pair.substring(0, sep).trim();
                String value = pair.substring(sep + 1);
                switch (option) {
                    case "compresstype":
                        statement.setCompressType(value);
                        break;
                    case "compresslevel":
                        statement.setCompressLevel(Integer.parseInt(value));
                        break;
                    case "blocksize":
                        statement.setBlockSize(Integer.parseInt(value));
                        break;
                }
            }
        }
    }
}