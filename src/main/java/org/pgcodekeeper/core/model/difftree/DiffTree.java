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
package org.pgcodekeeper.core.model.difftree;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.diff.Comparison;
import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;
import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.*;

/**
 * Utility class for creating and managing diff trees that represent
 * differences between database schemas.
 */
public final class DiffTree {

    /**
     * Creates a diff tree comparing two database schemas.
     *
     * @param settings the compare settings
     * @param left     the left (old) database schema
     * @param right    the right (new) database schema
     * @return the root TreeElement representing the diff tree
     * @throws InterruptedException if the operation is interrupted
     */
    public static TreeElement create(ISettings settings, AbstractDatabase left, AbstractDatabase right)
            throws InterruptedException {
        return create(settings, left, right, null);
    }

    /**
     * Creates a diff tree comparing two database schemas with progress monitoring.
     *
     * @param settings the compare settings
     * @param left     the left (old) database schema
     * @param right    the right (new) database schema
     * @param monitor  the progress monitor for tracking operation progress
     * @return the root TreeElement representing the diff tree
     * @throws InterruptedException if the operation is interrupted
     */
    public static TreeElement create(ISettings settings, AbstractDatabase left, AbstractDatabase right, IMonitor monitor)
            throws InterruptedException {
        return new DiffTree(settings, monitor).createTree(left, right);
    }

    /**
     * Adds column differences to the tree element list.
     *
     * @param left   the left (old) column list
     * @param right  the right (new) column list
     * @param parent the parent tree element
     * @param list   the list to add column differences to
     * @deprecated this method is deprecated
     */
    @Deprecated
    public static void addColumns(List<AbstractColumn> left, List<AbstractColumn> right,
                                  TreeElement parent, List<TreeElement> list) {
        for (AbstractColumn sLeft : left) {
            AbstractColumn foundRight = right.stream().filter(
                            sRight -> sLeft.getName().equals(sRight.getName()))
                    .findAny().orElse(null);

            if (!sLeft.equals(foundRight)) {
                TreeElement col = new TreeElement(sLeft, foundRight != null ? DiffSide.BOTH : DiffSide.LEFT);
                col.setParent(parent);
                list.add(col);
            }
        }

        for (AbstractColumn sRight : right) {
            if (left.stream().noneMatch(sLeft -> sRight.getName().equals(sLeft.getName()))) {
                TreeElement col = new TreeElement(sRight, DiffSide.RIGHT);
                col.setParent(parent);
                list.add(col);
            }
        }
    }

    /**
     * Gets tables that have changed columns from the selected elements.
     *
     * @param oldDbFull the old database schema
     * @param newDbFull the new database schema
     * @param selected  the list of selected tree elements
     * @return a set of table elements that have changed columns
     */
    public static Set<TreeElement> getTablesWithChangedColumns(
            AbstractDatabase oldDbFull, AbstractDatabase newDbFull, List<TreeElement> selected) {

        Set<TreeElement> tables = new HashSet<>();
        for (TreeElement el : selected) {
            if (el.getType() == DbObjType.TABLE) {
                List<TreeElement> columns = new ArrayList<>();
                DiffSide side = el.getSide();

                List<AbstractColumn> oldColumns;

                if (side == DiffSide.LEFT || side == DiffSide.BOTH) {
                    AbstractTable oldTbl = (AbstractTable) el.getStatement(oldDbFull);
                    oldColumns = oldTbl.getColumns();
                } else {
                    oldColumns = Collections.emptyList();
                }

                List<AbstractColumn> newColumns;
                if (side == DiffSide.RIGHT || side == DiffSide.BOTH) {
                    AbstractTable newTbl = (AbstractTable) el.getStatement(newDbFull);
                    newColumns = newTbl.getColumns();
                } else {
                    newColumns = Collections.emptyList();
                }

                addColumns(oldColumns, newColumns, el, columns);

                if (!columns.isEmpty()) {
                    tables.add(el);
                }
            }
        }

        return tables;
    }

    private final IMonitor monitor;
    private final ISettings settings;

    private DiffTree(ISettings settings, IMonitor monitor) {
        this.settings = settings;
        this.monitor = monitor;
    }

    /**
     * Creates a diff tree by comparing two database schemas and building a hierarchical
     * tree structure representing the differences between them.
     *
     * @param left  the left (old) database schema to compare
     * @param right the right (new) database schema to compare
     * @return the root TreeElement representing the complete diff tree with "Database"
     * as the root node and all schema differences as child nodes
     * @throws InterruptedException if the operation is cancelled via the progress monitor
     */
    public TreeElement createTree(AbstractDatabase left, AbstractDatabase right) throws InterruptedException {
        IMonitor.checkCancelled(monitor);
        TreeElement db = new TreeElement("Database", DbObjType.DATABASE, DiffSide.BOTH);
        addChildren(left, right, db);

        return db;
    }

    private void addChildren(AbstractStatement left, AbstractStatement right, TreeElement parent) throws InterruptedException {
        for (CompareResult res : compareStatements(left, right)) {
            IMonitor.checkCancelled(monitor);
            TreeElement child = new TreeElement(res.getStatement(), res.getSide());
            parent.addChild(child);

            if (res.hasChildren()) {
                addChildren(res.left(), res.right(), child);
            }
        }
    }

    /**
     * Compare lists and put elements onto appropriate sides.
     */
    private List<CompareResult> compareStatements(AbstractStatement left, AbstractStatement right) {
        List<CompareResult> rv = new ArrayList<>();

        // add LEFT and BOTH here
        // and RIGHT in a separate pass
        if (left != null) {
            left.getChildren().forEach(sLeft -> {
                AbstractStatement foundRight = null;
                if (right != null) {
                    foundRight = right.getChildren().filter(
                                    sRight -> sLeft.getName().equals(sRight.getName())
                                            && sLeft.getStatementType() == sRight.getStatementType())
                            .findAny().orElse(null);
                }

                if (foundRight == null) {
                    rv.add(new CompareResult(sLeft, null));
                } else if (!Comparison.compare(settings, sLeft, foundRight)) {
                    rv.add(new CompareResult(sLeft, foundRight));
                }
            });
        }

        if (right != null) {
            right.getChildren().forEach(sRight -> {
                if (left == null || left.getChildren().noneMatch(
                        sLeft -> sRight.getName().equals(sLeft.getName())
                                && sLeft.getStatementType() == sRight.getStatementType())) {
                    rv.add(new CompareResult(null, sRight));
                }
            });
        }

        return rv;
    }
}

/**
 * Represents the result of comparing two database statements during diff tree creation.
 * Contains references to the left and right statements and provides methods to
 * determine the comparison side and retrieve statement information.
 */
record CompareResult(AbstractStatement left, AbstractStatement right) {

    /**
     * Determines which side of the comparison this result represents.
     *
     * @return the diff side (LEFT, RIGHT, or BOTH)
     * @throws IllegalStateException if both sides are null
     */
    public DiffSide getSide() {
        if (left != null && right != null) {
            return DiffSide.BOTH;
        }
        if (left != null) {
            return DiffSide.LEFT;
        }
        if (right != null) {
            return DiffSide.RIGHT;
        }
        throw new IllegalStateException("Both diff sides are null!");
    }

    /**
     * Gets the statement from this comparison result.
     * Returns the left statement if available, otherwise the right statement.
     *
     * @return the statement from this comparison
     * @throws IllegalStateException if both sides are null
     */
    public AbstractStatement getStatement() {
        if (left != null) {
            return left;
        }
        if (right != null) {
            return right;
        }
        throw new IllegalStateException("Both diff sides are null!");
    }

    /**
     * Checks if this comparison result has child statements.
     *
     * @return true if either the left or right statement has children, false otherwise
     */
    public boolean hasChildren() {
        if (left != null && left.hasChildren()) {
            return true;
        }

        return right != null && right.hasChildren();
    }
}
