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

import org.pgcodekeeper.core.model.difftree.IgnoredObject.AddStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of ignore list for managing database object filtering rules.
 * Provides functionality to add, merge, and evaluate ignore rules for database objects
 * with support for hierarchical rule precedence and content-based filtering.
 */
public class IgnoreList implements IIgnoreList {

    private final List<IgnoredObject> rules = new ArrayList<>();

    // black list (show all, hide some) by default
    private boolean isShow = true;

    @Override
    public boolean isShow() {
        return isShow;
    }

    @Override
    public void setShow(boolean isShow) {
        this.isShow = isShow;
    }

    @Override
    public List<IgnoredObject> getList() {
        return Collections.unmodifiableList(rules);
    }

    /**
     * Clears all ignore rules from the list.
     */
    public void clearList() {
        rules.clear();
    }

    @Override
    public void add(IgnoredObject rule) {
        IgnoredObject existing = findSameMatchingRule(rule);
        if (existing != null) {
            if (existing.isIgnoreContent() != rule.isIgnoreContent()) {
                if (!existing.isIgnoreContent()) {
                    // existing rule is narrow (nocontent), use new wider rule
                    existing.setIgnoreContent(true);
                    existing.setShow(rule.isShow());
                }
            } else {
                // from same-width alternatives choose a hiding one
                existing.setShow(existing.isShow() && rule.isShow());
            }
        } else {
            // add new
            rules.add(rule);
        }
    }

    private IgnoredObject findSameMatchingRule(IgnoredObject rule) {
        for (IgnoredObject match : rules) {
            if (match.hasSameMatchingCondition(rule)) {
                return match;
            }
        }
        return null;
    }

    /**
     * Adds all ignore rules from the collection to this list.
     * Each rule is processed through the standard add logic to handle merging.
     *
     * @param collection collection of ignore rules to add
     */
    public void addAll(Collection<IgnoredObject> collection) {
        for (IgnoredObject rule : collection) {
            add(rule);
        }
    }

    /**
     * Determines the add status for a tree element based on ignore rules.
     * Evaluates all matching rules and applies precedence logic.
     *
     * @param el           the tree element to evaluate
     * @param inAddSubtree whether currently in an add subtree context
     * @param dbNames      optional database names for rule matching
     * @return the final add status for the element
     */
    public AddStatus getNameStatus(TreeElement el, boolean inAddSubtree, String... dbNames) {
        AddStatus status = null;
        for (IgnoredObject rule : rules) {
            if (rule.match(el, dbNames)) {
                AddStatus newStatus = rule.getAddStatus();
                if (status == null) {
                    status = newStatus;
                } else if ((status == AddStatus.ADD || status == AddStatus.SKIP) &&
                        (newStatus == AddStatus.ADD_SUBTREE || newStatus == AddStatus.SKIP_SUBTREE)) {
                    // use wider rule
                    status = newStatus;
                } else if (status == AddStatus.ADD && newStatus == AddStatus.SKIP ||
                        status == AddStatus.ADD_SUBTREE && newStatus == AddStatus.SKIP_SUBTREE) {
                    // use hiding rule
                    status = newStatus;
                }
            }
        }

        if (status != null) {
            return status;
        }

        return inAddSubtree || isShow ? AddStatus.ADD : AddStatus.SKIP;
    }

    /**
     * Generates string representation of the ignore list configuration.
     *
     * @return formatted string showing all rules and default behavior
     */
    public String getListCode() {
        StringBuilder sb = new StringBuilder();
        sb.append(isShow ? "SHOW ALL\n" : "HIDE ALL\n");
        for (IgnoredObject rule : rules) {
            rule.appendRuleCode(sb, true).append('\n');
        }
        return sb.toString();
    }
}
