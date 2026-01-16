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
package org.pgcodekeeper.core.database.pg.parser.expr;

import java.util.*;

import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.parser.rulectx.PgVex;
import org.pgcodekeeper.core.utils.ModPair;

/**
 * For use with value expressions that have predefined namespace.
 * @author levsha_aa
 */
public class PgValueExprWithNmspc extends PgAbstractExprWithNmspc<VexContext> {

    private final PgValueExpr vex;
    
    /**
     * Creates a ValueExprWithNmspc parser with meta container.
     *
     * @param meta the meta container with schema information
     */
    public PgValueExprWithNmspc(MetaContainer meta) {
        super(meta);
        vex = new PgValueExpr(this);
    }

    @Override
    public List<ModPair<String, String>> analyze(VexContext vex) {
        return analyze(new PgVex(vex));
    }

    /**
     * Analyzes a value expression context.
     *
     * @param vex the value expression context to analyze
     * @return list containing a single pair with expression name and type
     */
    public List<ModPair<String, String>> analyze(Vex_bContext vex) {
        return analyze(new PgVex(vex));
    }

    /**
     * Analyzes a value expression rule context.
     *
     * @param vex the value expression rule context to analyze
     * @return list containing a single pair with expression name and type
     */
    public List<ModPair<String, String>> analyze(PgVex vex) {
        return Collections.singletonList(this.vex.analyze(vex));
    }
}
