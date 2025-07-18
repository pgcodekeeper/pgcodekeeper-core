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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.IgnoredObject.AddStatus;
import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Sets {@link #onlySelected} setting true.
     */
    public TreeFlattener onlySelected() {
        onlySelected = true;
        return this;
    }

    public TreeFlattener onlySelected(boolean onlySelected) {
        this.onlySelected = onlySelected;
        return this;
    }

    public TreeFlattener onlyEdits(AbstractDatabase dbSource, AbstractDatabase dbTarget) {
        onlyEdits = dbSource != null && dbTarget != null;
        this.dbSource = dbSource;
        this.dbTarget = dbTarget;
        return this;
    }

    public TreeFlattener useIgnoreList(IgnoreList ignoreList) {
        return useIgnoreList(ignoreList, (String[]) null);
    }

    public TreeFlattener useIgnoreList(IgnoreList ignoreList, String... dbNames) {
        this.ignoreList = ignoreList;
        this.dbNames = dbNames;
        return this;
    }

    public TreeFlattener onlyTypes(Collection<DbObjType> onlyTypes) {
        this.onlyTypes = onlyTypes;
        return this;
    }

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
