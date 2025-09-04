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
package org.pgcodekeeper.core.loader.pg;

/**
 * Enumeration of supported PostgreSQL and Greenplum versions.
 * Provides version comparison and lookup functionality for database compatibility checking.
 */
public enum SupportedPgVersion {
    GP_VERSION_6(90400, "9.4"),
    GP_VERSION_7(120012, "12.12"),
    VERSION_14(140000, "14.0"),
    VERSION_15(150000, "15.0"),
    VERSION_16(160000, "16.0"),
    VERSION_17(170000, "17.0"),
    VERSION_18(180000, "18.0");

    private final int version;
    private final String text;

    /**
     * Creates a new PostgreSQL version entry.
     *
     * @param version the numeric version identifier
     * @param text    the human-readable version string
     */
    SupportedPgVersion(int version, String text) {
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
     * If no matching version is found, returns VERSION_9_4 as the default.
     *
     * @param checkVersion the version to check
     * @return the matching supported version or VERSION_9_4 as default
     */
    public static SupportedPgVersion valueOf(int checkVersion) {
        SupportedPgVersion[] set = SupportedPgVersion.values();

        for (int i = set.length - 1; i >= 0; i--) {
            SupportedPgVersion verEnum = set[i];
            if (verEnum.isLE(checkVersion)) {
                return verEnum;
            }
        }

        return SupportedPgVersion.GP_VERSION_6;
    }
}