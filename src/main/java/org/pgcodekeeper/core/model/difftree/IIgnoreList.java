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
package org.pgcodekeeper.core.model.difftree;

import java.util.List;

/**
 * Interface for managing ignore lists used in database schema comparison.
 * Provides basic operations for adding ignore rules and controlling visibility behavior.
 */
public interface IIgnoreList {

    /**
     * Gets the list of ignore rules.
     * 
     * @return list of ignored objects
     */
    List<IgnoredObject> getList();

    /**
     * Adds an ignore rule to the list.
     * 
     * @param rule the ignore rule to add
     */
    void add(IgnoredObject rule);

    /**
     * Sets the default show behavior for this ignore list.
     * 
     * @param isShow true for show-all (blacklist), false for hide-all (whitelist)
     */
    void setShow(boolean isShow);

    /**
     * Gets the default show behavior.
     * 
     * @return true if default is to show objects, false if default is to hide
     */
    boolean isShow();
}
