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
package org.pgcodekeeper.core.model.graph;

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;
import org.pgcodekeeper.core.model.difftree.TreeFlattener;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.PgStatement;

import java.util.*;

/**
 * Finds dependent elements in tree based on user selection using dependency resolution mechanism.
 *
 * @author botov_av
 */
public final class DepcyTreeExtender {

    private final AbstractDatabase dbSource;
    private final AbstractDatabase dbTarget;
    private final SimpleDepcyResolver depRes;
    private final TreeElement root;
    /**
     * Elements selected by user for deployment to Project
     */
    private final List<TreeElement> userSelection;
    /**
     * Dependent elements from created/edited objects (contain user selection)
     */
    private final List<TreeElement> treeDepcyNewEdit = new ArrayList<>();
    /**
     * Dependent elements from deleted objects (contain user selection)
     */
    private final List<TreeElement> treeDepcyDelete = new ArrayList<>();

    /**
     * Creates a new dependency tree extender.
     *
     * @param dbSource source database schema
     * @param dbTarget target database schema
     * @param root     root element of the tree to analyze
     */
    public DepcyTreeExtender(AbstractDatabase dbSource, AbstractDatabase dbTarget, TreeElement root) {
        this.dbSource = dbSource;
        this.dbTarget = dbTarget;
        this.root = root;
        userSelection = new TreeFlattener().onlySelected().flatten(root);
        depRes = new SimpleDepcyResolver(dbSource, dbTarget, false);
    }

    /**
     * For edited state or created object, pulls dependencies from above
     * for creating or modifying the object
     */
    private void fillDepcyOfNewEdit() {
        PgStatement markedToCreate;
        Set<PgStatement> newEditDepcy = new HashSet<>();
        for (TreeElement sel : userSelection) {
            if (sel.getSide() != DiffSide.LEFT
                    && (markedToCreate = sel.getPgStatement(dbTarget)) != null) {
                newEditDepcy.addAll(depRes.getCreateDepcies(markedToCreate));
            }
        }
        fillTreeDepcies(treeDepcyNewEdit, newEditDepcy);
    }

    /**
     * When deleting an object, pulls dependencies from below
     */
    private void fillDepcyOfDeleted() {
        PgStatement markedToDelete;
        Set<PgStatement> deleteDepcy = new HashSet<>();
        for (TreeElement sel : userSelection) {
            if (sel.getSide() == DiffSide.LEFT
                    && sel.getType() != DbObjType.SEQUENCE
                    && (markedToDelete = sel.getPgStatement(dbSource)) != null) {
                deleteDepcy.addAll(depRes.getDropDepcies(markedToDelete));
            }
        }
        fillTreeDepcies(treeDepcyDelete, deleteDepcy);
    }

    /**
     * Extracts objects from tree for dependencies
     *
     * @param treeDepcy list to add dependent tree elements to
     * @param pgDecies  collection of database statement dependencies
     */
    private void fillTreeDepcies(List<TreeElement> treeDepcy, Collection<PgStatement> pgDecies) {
        for (PgStatement depcy : pgDecies) {
            TreeElement finded = root.findElement(depcy);
            if (finded != null) {
                if (finded.getSide() == DiffSide.BOTH) {
                    if (!finded.getPgStatement(dbSource).compare(finded.getPgStatement(dbTarget))) {
                        treeDepcy.add(finded);
                    }
                } else {
                    treeDepcy.add(finded);
                }
            }
        }
    }

    /**
     * Returns all dependent elements based on user selection.
     * Analyzes both create/edit and delete dependencies.
     *
     * @return set of dependent elements excluding user-selected objects
     */
    public Set<TreeElement> getDepcies() {
        Set<TreeElement> res = new HashSet<>();
        fillDepcyOfNewEdit();
        fillDepcyOfDeleted();
        res.addAll(treeDepcyNewEdit);
        res.addAll(treeDepcyDelete);
        // remove all objects selected by user
        userSelection.forEach(res::remove);
        return res;
    }
}
