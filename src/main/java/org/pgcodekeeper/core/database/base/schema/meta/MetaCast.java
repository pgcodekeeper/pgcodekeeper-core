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
package org.pgcodekeeper.core.database.base.schema.meta;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.api.schema.ICast;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;

import java.io.Serial;

/**
 * Represents a database cast metadata object.
 * Stores information about type casting operations including source and target types
 * and the casting context (IMPLICIT, ASSIGNMENT, or EXPLICIT).
 */
public final class MetaCast extends MetaStatement implements ICast {

    @Serial
    private static final long serialVersionUID = 5369541137617339001L;

    private final String source;
    private final String target;

    private final CastContext context;

    /**
     * Creates a new cast metadata object.
     *
     * @param source  the source data type
     * @param target  the target data type
     * @param context the casting context
     */
    public MetaCast(String source, String target, CastContext context) {
        super(new GenericColumn(ICast.getSimpleName(source, target), DbObjType.CAST));
        this.source = source;
        this.target = target;
        this.context = context;
    }

    /**
     * Creates a new cast metadata object with location information.
     *
     * @param source  the source data type
     * @param target  the target data type
     * @param context the casting context
     * @param object  the object location information
     */
    public MetaCast(String source, String target, CastContext context, ObjectLocation object) {
        super(object);
        this.source = source;
        this.target = target;
        this.context = context;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public String getTarget() {
        return target;
    }

    @Override
    public CastContext getContext() {
        return context;
    }
}
