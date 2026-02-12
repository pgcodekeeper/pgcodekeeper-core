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
package org.pgcodekeeper.core.database.api.jdbc;

/**
 * Interface for supported database versions.
 * Provides version comparison and lookup functionality for database compatibility checking.
 */
public interface ISupportedVersion {
    int getVersion();

    String getText();

    /**
     * Checks if this version is less than or equal to the specified version.
     *
     * @param version the version to compare against
     * @return true if this version is less than or equal to the specified version
     */
    default boolean isLE(int version) {
        return getVersion() <= version;
    }

    /**
     * Returns the highest supported version that is less than or equal to the specified version.
     * If no matching version is found, returns default value.
     *
     * @param checkVersion the version to check
     * @param versions supported versions
     * @param defaultValue default value
     */
    static <T extends ISupportedVersion> T valueOf(int checkVersion, T[] versions, T defaultValue) {
        for (int i = versions.length - 1; i >= 0; i--) {
            T verEnum = versions[i];
            if (verEnum.isLE(checkVersion)) {
                return verEnum;
            }
        }

        return defaultValue;
    }
}