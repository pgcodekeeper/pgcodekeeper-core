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
package org.pgcodekeeper.core.database.pg.jdbc;

import org.pgcodekeeper.core.database.base.jdbc.*;
import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.utils.Utils;

public abstract class PgAbstractJdbcReader extends AbstractJdbcReader<PgJdbcLoader> implements IPgJdbcReader {

    protected final String classId;

    protected PgAbstractJdbcReader(PgJdbcLoader loader) {
        super(loader);
        String tmpClassId = getClassId();
        this.classId = tmpClassId == null ? null : Utils.quoteString(PG_CATALOG + tmpClassId);
    }

    @Override
    protected QueryBuilder makeQuery() {
        var builder = super.makeQuery();
        appendExtension(builder, loader.getExtensionSchema());
        return builder;
    }

    @Override
    public String getFormattedClassId() {
        return classId;
    }
}
