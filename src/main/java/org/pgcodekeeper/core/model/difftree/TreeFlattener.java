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

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.ignorelist.IgnoreList;
import org.pgcodekeeper.core.ignorelist.IgnoredObject;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.ignorelist.IgnoredObject.AddStatus;
import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Utility class for flattening tree structures with filtering capabilities.
 * Provides methods to filter tree elements based on selection status, edit state,
 * ignore lists, and object types while maintaining proper hierarchy traversal.
 */
public final class TreeFlattener {

    private static final Logger LOG = LoggerFactory.getLogger(TreeFlattener.class);

    private boolean onlySelected;
    private boolean onlyEdits;
    private AbstractDatabase dbSource;
    private AbstractDatabase dbTarget;
    private IgnoreList ignoreList;
    private String[] dbNames;
    private Collection<DbObjType> onlyTypes;

    private final List<TreeElement> result = new ArrayList<>();
    private final Deque<TreeElement> addSubtreeRoots = new ArrayDeque<>();

    /**
     * Configures the flattener to include only selected elements.
     *
     * @return this TreeFlattener for method chaining
     */
    public TreeFlattener onlySelected() {
        onlySelected = true;
        return this;
    }

    /**
     * Configures whether to include only selected elements.
     *
     * @param onlySelected true to include only selected elements
     * @return this TreeFlattener for method chaining
     */
    public TreeFlattener onlySelected(boolean onlySelected) {
        this.onlySelected = onlySelected;
        return this;
    }

    /**
     * Configures the flattener to include only edited elements.
     *
     * @param dbSource source database for comparison
     * @param dbTarget target database for comparison
     * @return this TreeFlattener for method chaining
     */
    public TreeFlattener onlyEdits(AbstractDatabase dbSource, AbstractDatabase dbTarget) {
        onlyEdits = dbSource != null && dbTarget != null;
        this.dbSource = dbSource;
        this.dbTarget = dbTarget;
        return this;
    }

    /**
     * Configures the flattener to use an ignore list for filtering.
     *
     * @param ignoreList the ignore list to apply
     * @return this TreeFlattener for method chaining
     */
    public TreeFlattener useIgnoreList(IgnoreList ignoreList) {
        return useIgnoreList(ignoreList, (String[]) null);
    }

    /**
     * Configures the flattener to use an ignore list with database name filtering.
     *
     * @param ignoreList the ignore list to apply
     * @param dbNames    database names for rule matching
     * @return this TreeFlattener for method chaining
     */
    public TreeFlattener useIgnoreList(IgnoreList ignoreList, String... dbNames) {
        this.ignoreList = ignoreList;
        this.dbNames = dbNames;
        return this;
    }

    /**
     * Configures the flattener to include only specific object types.
     *
     * @param onlyTypes collection of object types to include
     * @return this TreeFlattener for method chaining
     */
    public TreeFlattener onlyTypes(Collection<DbObjType> onlyTypes) {
        this.onlyTypes = onlyTypes;
        return this;
    }

    /**
     * Flattens the tree structure applying all configured filters.
     *
     * @param root the root element to start flattening from
     * @return list of filtered tree elements
     */
    public List<TreeElement> flatten(TreeElement root) {
        result.clear();
        addSubtreeRoots.clear();
        LOG.info(Messages.TreeFlattener_log_filter_obj);
        recurse(root);
        return result;
    }

    private void recurse(TreeElement el) {
        AddStatus status = ignoreList != null ? getNameStatus(el) : AddStatus.ADD;

        if (status == AddStatus.SKIP) {
            var msg = Messages.TreeFlattener_log_ignore_obj.formatted(el.getName());
            LOG.debug(msg);
        }
        if (status == AddStatus.SKIP_SUBTREE) {
            if (LOG.isDebugEnabled()) {
                var msg = Messages.TreeFlattener_log_ignore_obj.formatted(el.getName());
                LOG.debug(msg);
                writeChildrenInLog(el);
            }
            return;
        }

        if (status == AddStatus.ADD_SUBTREE) {
            addSubtreeRoots.push(el);
        }
        for (TreeElement sub : el.getChildren()) {
            recurse(sub);
        }
        if (status == AddStatus.ADD_SUBTREE) {
            addSubtreeRoots.pop();
        }

        if (el.getType() == DbObjType.DATABASE) {
            return;
        }

        if ((status == AddStatus.ADD || status == AddStatus.ADD_SUBTREE)
                && (!onlySelected || el.isSelected())
                && (onlyTypes == null || onlyTypes.isEmpty() || onlyTypes.contains(el.getType()))
                && (!onlyEdits || el.getSide() != DiffSide.BOTH
                || !el.getStatement(dbSource).compare(el.getStatement(dbTarget)))) {
            result.add(el);
        }
    }

    /**
     * Determines the add status for a tree element based on ignore rules.
     * Evaluates all matching rules and applies precedence logic.
     *
     * @param el           the tree element to evaluate
     * @return the final add status for the element
     */
    private AddStatus getNameStatus(TreeElement el) {
        AddStatus status = null;
        for (IgnoredObject rule : ignoreList.getList()) {
            if (match(rule, el)) {
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

        return !addSubtreeRoots.isEmpty() || ignoreList.isShow() ? AddStatus.ADD : AddStatus.SKIP;
    }

    /**
     * Checks if this ignore rule matches the given tree element and database names.
     *
     * @param rule    rule
     * @param el      the tree element to match against
     * @return true if the rule matches the element
     */
    private boolean match(IgnoredObject rule, TreeElement el) {
        boolean matches = rule.match(rule.isQualified() ? el.getQualifiedName() : el.getName());

        var objTypes = rule.getObjTypes();
        if (!objTypes.isEmpty()) {
            matches = matches && objTypes.contains(el.getType());
        }

        var pattern = rule.getDbRegex();
        if (matches && pattern != null) {
            if (dbNames != null && dbNames.length != 0) {
                boolean foundDbMatch = false;
                for (String dbName : dbNames) {
                    if (dbName != null && pattern.matcher(dbName).find()) {
                        foundDbMatch = true;
                        break;
                    }
                }
                matches = foundDbMatch;
            } else {
                matches = false;
            }
        }
        return matches;
    }

    private void writeChildrenInLog(TreeElement el) {
        for (TreeElement sub : el.getChildren()) {
            var msg = Messages.TreeFlattener_log_ignore_children.formatted(sub.getName());
            LOG.debug(msg);
            if (!sub.getChildren().isEmpty()) {
                writeChildrenInLog(sub);
            }
        }
    }
}
