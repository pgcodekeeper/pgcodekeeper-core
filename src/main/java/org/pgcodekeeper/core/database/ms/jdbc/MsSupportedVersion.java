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
package org.pgcodekeeper.core.database.ms.jdbc;

import org.pgcodekeeper.core.database.api.jdbc.ISupportedVersion;

/**
 * Enumeration of supported Microsoft SQL Server versions.
 * Provides version comparison and lookup functionality for database compatibility checking.
 */
public enum MsSupportedVersion implements ISupportedVersion {
    VERSION_17(14, "2017"),
    VERSION_19(15, "2019"),
    VERSION_22(16, "2022");

    private final int version;
    private final String text;

    /**
     * Creates a new Microsoft SQL Server version entry.
     *
     * @param version the numeric version identifier
     * @param text    the human-readable version string
     */
    MsSupportedVersion(int version, String text) {
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
     * If no matching version is found, returns VERSION_12 as the default.
     *
     * @param checkVersion the version to check
     * @return the matching supported version or VERSION_12 as default
     */
    public static MsSupportedVersion valueOf(int checkVersion) {
        return ISupportedVersion.valueOf(checkVersion, MsSupportedVersion.values(), MsSupportedVersion.VERSION_17);
    }
}