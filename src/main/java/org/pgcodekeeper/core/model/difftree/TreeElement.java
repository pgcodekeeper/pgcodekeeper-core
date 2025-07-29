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

import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.IStatementContainer;
import org.pgcodekeeper.core.schema.PgStatement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Wrapper for database objects representing the state between old and new database schemas.
 * Provides hierarchical tree structure for organizing database objects and their relationships
 * during schema comparison operations.
 */
public final class TreeElement {

    /**
     * Represents the side of difference in schema comparison.
     */
    public enum DiffSide {
        LEFT, RIGHT, BOTH
    }

    private int hashcode;
    private final String name;
    private final DbObjType type;
    private final DiffSide side;
    private boolean selected;
    private TreeElement parent;
    private final List<TreeElement> children = new ArrayList<>();

    /**
     * Gets the name of this tree element.
     *
     * @return the element name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the database object type of this element.
     *
     * @return the object type
     */
    public DbObjType getType() {
        return type;
    }

    /**
     * Gets the diff side of this element.
     *
     * @return the diff side (LEFT, RIGHT, or BOTH)
     */
    public DiffSide getSide() {
        return side;
    }

    /**
     * Gets the list of child elements.
     *
     * @return unmodifiable list of children
     */
    public List<TreeElement> getChildren() {
        return Collections.unmodifiableList(children);
    }

    /**
     * Gets the parent element.
     *
     * @return the parent element, can be null for root
     */
    public TreeElement getParent() {
        return parent;
    }

    /**
     * Checks if this element is selected.
     *
     * @return true if selected, false otherwise
     */
    public boolean isSelected() {
        return selected;
    }

    /**
     * Sets the selection state of this element.
     *
     * @param selected true to select, false to deselect
     */
    public void setSelected(boolean selected) {
        this.selected = selected;
    }

    /**
     * Creates a tree element with specified properties.
     *
     * @param name the element name
     * @param type the database object type
     * @param side the diff side
     */
    public TreeElement(String name, DbObjType type, DiffSide side) {
        this.name = name;
        this.type = type;
        this.side = side;
    }

    /**
     * Creates a tree element from a database statement.
     *
     * @param statement the database statement
     * @param side      the diff side
     */
    public TreeElement(PgStatement statement, DiffSide side) {
        this.name = statement.getName();
        this.side = side;
        this.type = statement.getStatementType();
    }

    /**
     * Checks if this element has child elements.
     *
     * @return true if has children, false otherwise
     */
    public boolean hasChildren() {
        return !children.isEmpty();
    }

    /**
     * Adds a child element to this element.
     *
     * @param child the child element to add
     * @throws IllegalStateException if child already has a parent
     */
    public void addChild(TreeElement child) {
        if (child.parent != null) {
            throw new IllegalStateException("Cannot add a child that already has a parent!");
        }

        child.parent = this;
        child.hashcode = 0;
        children.add(child);
    }

    /**
     * Gets a child element by name and type.
     *
     * @param name the child name to find
     * @param type the child type to match, can be null to match any type
     * @return the matching child element, or null if not found
     */
    public TreeElement getChild(String name, DbObjType type) {
        for (TreeElement el : children) {
            if ((type == null || el.type == type) && el.name.equals(name)) {
                return el;
            }
        }

        return null;
    }

    /**
     * Gets a child element by name (any type).
     *
     * @param name the child name to find
     * @return the matching child element, or null if not found
     */
    public TreeElement getChild(String name) {
        return getChild(name, null);
    }

    /**
     * Gets a child element by index.
     *
     * @param index the child index
     * @return the child element at the specified index
     */
    public TreeElement getChild(int index) {
        return children.get(index);
    }

    /**
     * Counts all descendant elements recursively.
     *
     * @return total number of descendants
     */
    public int countDescendants() {
        int descendants = 0;
        for (TreeElement sub : children) {
            descendants++;
            descendants += sub.countDescendants();
        }

        return descendants;
    }

    /**
     * Counts direct child elements.
     *
     * @return number of direct children
     */
    public int countChildren() {
        return children.size();
    }

    /**
     * Gets corresponding database statement from the specified database.
     *
     * @param db the database to retrieve statement from
     * @return the corresponding database statement
     * @throws IllegalArgumentException if no statement found for parent
     */
    public PgStatement getPgStatement(AbstractDatabase db) {
        if (type == DbObjType.DATABASE) {
            return db;
        }
        PgStatement stParent = parent.getPgStatement(db);
        if (stParent == null) {
            throw new IllegalArgumentException("No statement found for " + parent);
        }
        if (type == DbObjType.COLUMN) {
            return ((AbstractTable) stParent).getColumn(name);
        }
        if (stParent instanceof IStatementContainer cont) {
            return cont.getChild(name, type);
        }

        return null;
    }

    /**
     * Gets statement from the corresponding database based on diff side.
     * BOTH side uses left database.
     *
     * @param left  the left database
     * @param right the right database
     * @return statement from the appropriate database
     */
    public PgStatement getPgStatementSide(AbstractDatabase left, AbstractDatabase right) {
        return switch (side) {
            case LEFT, BOTH -> getPgStatement(left);
            case RIGHT -> getPgStatement(right);
        };
    }

    /**
     * Finds an element in the tree by database statement.
     *
     * @param st the database statement to find
     * @return the matching tree element, or null if not found
     */
    public TreeElement findElement(PgStatement st) {
        if (st.getStatementType() == DbObjType.DATABASE) {
            TreeElement root = this;
            while (root.parent != null) {
                root = root.parent;
            }
            return root;
        }
        TreeElement par = findElement(st.getParent());
        return par == null ? null : par.getChild(st.getName(), st.getStatementType());
    }

    /**
     * Creates a copy of elements starting from current with sides reverted:
     * left -> right, right -> left, both -> both
     *
     * @return reverted copy of this element and its children
     */
    public TreeElement getRevertedCopy() {
        TreeElement copy = getRevertedElement();
        for (TreeElement child : children) {
            copy.addChild(child.getRevertedCopy());
        }
        return copy;
    }

    /**
     * Возвращает копию элемента с измененными сторонами
     */
    private TreeElement getRevertedElement() {
        DiffSide newSide = switch (side) {
            case BOTH -> DiffSide.BOTH;
            case LEFT -> DiffSide.RIGHT;
            case RIGHT -> DiffSide.LEFT;
        };
        TreeElement copy = new TreeElement(name, type, newSide);
        copy.setSelected(selected);
        return copy;
    }

    /**
     * Creates a copy of elements starting from current element.
     *
     * @return copy of this element and its children
     */
    public TreeElement getCopy() {
        TreeElement copy = new TreeElement(name, type, side);
        copy.setSelected(selected);
        for (TreeElement child : children) {
            copy.addChild(child.getCopy());
        }
        return copy;
    }

    /**
     * Marks all elements as selected starting from current element.
     */
    public void setAllChecked() {
        setSelected(true);
        for (TreeElement child : children) {
            child.setAllChecked();
        }
    }

    /**
     * Checks if there are selected elements in subtree starting from current node.
     *
     * @return true if any elements in subtree are selected
     */
    public boolean isSubTreeSelected() {
        for (TreeElement child : children) {
            if (child.isSubTreeSelected()) {
                return true;
            }
        }
        return selected;
    }

    /**
     * Checks if this element is a container type (table or view).
     *
     * @return true if element is a container type
     */
    public boolean isContainer() {
        return type.in(DbObjType.TABLE, DbObjType.VIEW);
    }

    /**
     * Checks if this element is a sub-element of a container.
     *
     * @return true if parent is a container type
     */
    public boolean isSubElement() {
        return parent != null && parent.isContainer();
    }

    @Override
    public int hashCode() {
        if (hashcode == 0) {
            int result = Objects.hash(name, side, type, getContainerQName());
            if (result == 0) {
                ++result;
            }
            hashcode = result;
        }

        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return obj instanceof TreeElement other
                && Objects.equals(name, other.name)
                && type == other.type
                && side == other.side
                && getContainerQName().equals(other.getContainerQName());
    }

    /**
     * Gets the qualified name of the container path.
     *
     * @return qualified name of container hierarchy
     */
    public String getContainerQName() {
        var qname = "";

        TreeElement par = this.parent;
        while (par != null) {
            if (par.type == DbObjType.DATABASE) {
                break;
            }
            qname = par.name + (qname.isEmpty() ? qname : '.' + qname);
            par = par.parent;
        }

        return qname;
    }

    /**
     * Gets the qualified name of this element.
     * Note: the name itself is not quoted as it may include function parameters.
     *
     * @return this element's qualified name
     */
    public String getQualifiedName() {
        String qname = getContainerQName();
        return qname.isEmpty() ? name : qname + '.' + name;
    }

    @Override
    public String toString() {
        return name == null ? "no name" : name + ' ' + side + ' ' + type;
    }

    /**
     * Sets parent element - use only for columns to create one-way relationship
     * for getting object from database.
     *
     * @param el the parent element
     * @deprecated this method should only be used for column relationships
     */
    @Deprecated
    public void setParent(TreeElement el) {
        this.parent = el;
    }
}
