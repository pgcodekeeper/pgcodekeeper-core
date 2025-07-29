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

import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.IgnoredObject.AddStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of ignore list specifically for schema filtering.
 * Manages rules for showing or hiding database schemas based on pattern matching.
 * Uses black list approach by default (show all, hide some).
 */
public class IgnoreSchemaList implements IIgnoreList {

    private static final Logger LOG = LoggerFactory.getLogger(IgnoreSchemaList.class);

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
        rules.add(rule);
    }

    /**
     * Checks if a schema should be shown based on configured rules.
     * 
     * @param schema the schema name to check
     * @return true if schema should be shown, false if it should be hidden
     */
    public boolean getNameStatus(String schema) {
        for (IgnoredObject rule : rules) {
            if (rule.match(schema)) {
                AddStatus newStatus = rule.getAddStatus();
                return switch (newStatus) {
                    case ADD, ADD_SUBTREE -> true;
                    case SKIP, SKIP_SUBTREE -> {
                        LOG.debug(Messages.IgnoreSchemaList_log_ignored_schema, schema);
                        yield false;
                    }
                };
            }
        }
        return isShow;
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
