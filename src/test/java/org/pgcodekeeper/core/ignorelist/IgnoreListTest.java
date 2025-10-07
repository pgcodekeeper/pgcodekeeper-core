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
package org.pgcodekeeper.core.ignorelist;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pgcodekeeper.core.model.difftree.DbObjType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

final class IgnoreListTest {

    private static final String SEPARATE = """
            HIDE ALL
            SHOW REGEX '.*' type=TABLE
            SHOW REGEX '.*' type=VIEW
            """;

    private static final String SIMPLE = """
            HIDE ALL
            SHOW REGEX '.*' type=VIEW,TABLE
            """;

    @Test
    void addRulesSeparateTest() {
        var list = new IgnoreList();
        list.setShow(false);
        var ignoreObj1 = new IgnoredObject(".*", null, true, true, false, false, Set.of(DbObjType.TABLE));
        var ignoreObj2 = new IgnoredObject(".*", null, true, true, false, false, Set.of(DbObjType.VIEW));

        list.add(ignoreObj1);
        list.add(ignoreObj2);

        var listOfObjs = list.getList();

        Assertions.assertEquals(2, listOfObjs.size());
        Assertions.assertTrue(listOfObjs.contains(ignoreObj1));
        Assertions.assertTrue(listOfObjs.contains(ignoreObj2));

        Assertions.assertEquals(SEPARATE, list.getListCode());
    }

    @Test
    void addAllRulesSeparateTest() {
        var list = new IgnoreList();
        list.setShow(false);
        var ignoreObj1 = new IgnoredObject(".*", null, true, true, false, false, Set.of(DbObjType.TABLE));
        var ignoreObj2 = new IgnoredObject(".*", null, true, true, false, false, Set.of(DbObjType.VIEW));

        List<IgnoredObject> tempList = new ArrayList<>();

        tempList.add(ignoreObj1);
        tempList.add(ignoreObj2);

        list.addAll(tempList);

        var listOfObjs = list.getList();

        Assertions.assertEquals(2, listOfObjs.size());
        Assertions.assertTrue(listOfObjs.contains(ignoreObj1));
        Assertions.assertTrue(listOfObjs.contains(ignoreObj2));

        Assertions.assertEquals(SEPARATE, list.getListCode());
    }

    @Test
    void addRuleTest() {
        var list = new IgnoreList();
        list.setShow(false);
        // this collection is needed to ensure order when running the test from eclipse and maven
        Set<DbObjType> objsSet = new LinkedHashSet<>();
        objsSet.add(DbObjType.VIEW);
        objsSet.add(DbObjType.TABLE);
        
        var ignoreObj1 = new IgnoredObject(".*", null, true, true, false, false, objsSet);

        list.add(ignoreObj1);

        var listOfObjs = list.getList();

        Assertions.assertEquals(1, listOfObjs.size());
        Assertions.assertTrue(listOfObjs.contains(ignoreObj1));

        Assertions.assertEquals(SIMPLE, list.getListCode());
    }
}
