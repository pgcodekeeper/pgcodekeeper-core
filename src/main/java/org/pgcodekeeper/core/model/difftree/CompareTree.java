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

import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;

import java.util.Comparator;

import static org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide.*;

/**
 * Comparator for TreeElement objects that provides custom sorting logic
 * based on element types and diff sides during schema comparison operations.
 * Prioritizes deletions (LEFT), then creations (RIGHT), then modifications (BOTH).
 */
public class CompareTree implements Comparator<TreeElement> {

    private static final int LESS = -1;
    private static final int MORE = 1;

    @Override
    public int compare(TreeElement o1, TreeElement o2) {
        DiffSide s1 = o1.getSide();
        DiffSide s2 = o2.getSide();
        int res = compareTypes(o1, o2);
        if (s1 == s2) {
            return s1 == LEFT ? -res : res;
        }

        if (s1 == LEFT) {
            return LESS;
        }
        if (s2 == LEFT) {
            return MORE;
        }
        if (res == 0) {
            if (s1 == RIGHT) {
                return LESS;
            }
            if (s1 == BOTH) {
                return MORE;
            }
        } else {
            return res;
        }

        throw new IllegalStateException("Missing compare case");
    }

    /**
     * Compares and returns the order in the list of object types as needed
     */
    private int compareTypes(TreeElement o1, TreeElement o2) {
        return o1.getType().ordinal() - o2.getType().ordinal();
    }
}
