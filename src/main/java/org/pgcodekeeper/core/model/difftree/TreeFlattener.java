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
import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;
import org.pgcodekeeper.core.schema.AbstractDatabase;
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
     * @param dbNames database names for rule matching
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
        AddStatus status;
        if (ignoreList == null) {
            status = AddStatus.ADD;
        } else {
            status = ignoreList.getNameStatus(el, !addSubtreeRoots.isEmpty(), dbNames);
        }

        if (status == AddStatus.SKIP) {
            LOG.debug(Messages.TreeFlattener_log_ignore_obj, el.getName());
        }
        if (status == AddStatus.SKIP_SUBTREE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(Messages.TreeFlattener_log_ignore_obj, el.getName());
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
                || !el.getPgStatement(dbSource).compare(el.getPgStatement(dbTarget)))) {
            result.add(el);
        }
    }

    private void writeChildrenInLog(TreeElement el) {
        for (TreeElement sub : el.getChildren()) {
            LOG.debug(Messages.TreeFlattener_log_ignore_children, sub.getName());
            if (!sub.getChildren().isEmpty()) {
                writeChildrenInLog(sub);
            }
        }
    }
}
