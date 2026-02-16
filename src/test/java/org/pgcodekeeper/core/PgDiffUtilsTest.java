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
package org.pgcodekeeper.core;

import org.junit.jupiter.api.Test;
import org.pgcodekeeper.core.database.pg.utils.PgDiffUtils;

import static org.junit.jupiter.api.Assertions.*;

class PgDiffUtilsTest {

    @Test
    void quoteStringDollarTest() {
        String def = "asdad$_XXXXXXX_XXXXXXXasdaasdsad";
        String actual = "$_XXXXXXX_XXXXXXX_$" + def + "$_XXXXXXX_XXXXXXX_$";
        String expected = PgDiffUtils.quoteStringDollar(def);
        assertEquals(expected, actual, "Function dollars fail");
    }
}