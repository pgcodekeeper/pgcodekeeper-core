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
package org.pgcodekeeper.core.database.ch.jdbc;

import org.pgcodekeeper.core.database.api.jdbc.ISupportedVersion;

/**
 * Enumeration of supported ClickHouse versions.
 * Provides version comparison and lookup functionality for database compatibility checking.
 */
public enum ChSupportedVersion implements ISupportedVersion {

    DEFAULT(0, "0");

    private final int version;
    private final String text;

    /**
     * Creates a new ClickHouse version entry.
     *
     * @param version the numeric version identifier
     * @param text    the human-readable version string
     */
    ChSupportedVersion(int version, String text) {
        this.version = version;
        this.text = text;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public String getText() {
        return text;
    }

    /**
     * Returns the highest supported version that is less than or equal to the specified version.
     * If no matching version is found, returns VERSION_9_4 as the default.
     *
     * @param checkVersion the version to check
     * @return the matching supported version or VERSION_9_4 as default
     */
    public static ChSupportedVersion valueOf(int checkVersion) {
        return ISupportedVersion.valueOf(checkVersion, ChSupportedVersion.values(), ChSupportedVersion.DEFAULT);
    }
}