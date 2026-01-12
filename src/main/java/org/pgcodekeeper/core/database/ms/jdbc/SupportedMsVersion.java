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
package org.pgcodekeeper.core.database.ms.jdbc;

/**
 * Enumeration of supported Microsoft SQL Server versions.
 * Provides version comparison and lookup functionality for database compatibility checking.
 */
public enum SupportedMsVersion {
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
    SupportedMsVersion(int version, String text) {
        this.version = version;
        this.text = text;
    }

    public int getVersion() {
        return version;
    }

    public String getText() {
        return text;
    }

    /**
     * Checks if this version is less than or equal to the specified version.
     *
     * @param version the version to compare against
     * @return true if this version is less than or equal to the specified version
     */
    public boolean isLE(int version) {
        return this.version <= version;
    }

    /**
     * Returns the highest supported version that is less than or equal to the specified version.
     * If no matching version is found, returns VERSION_12 as the default.
     *
     * @param checkVersion the version to check
     * @return the matching supported version or VERSION_12 as default
     */
    public static SupportedMsVersion valueOf(int checkVersion) {
        SupportedMsVersion[] set = SupportedMsVersion.values();

        for (int i = set.length - 1; i >= 0; i--) {
            SupportedMsVersion verEnum = set[i];
            if (verEnum.isLE(checkVersion)) {
                return verEnum;
            }
        }

        return SupportedMsVersion.VERSION_17;
    }
}