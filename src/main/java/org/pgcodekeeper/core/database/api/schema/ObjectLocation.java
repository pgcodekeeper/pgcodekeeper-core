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
package org.pgcodekeeper.core.database.api.schema;

import java.io.Serial;
import java.util.Objects;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.*;
import org.pgcodekeeper.core.database.base.parser.*;

/**
 * Represents the location of a database object in source code.
 * Contains position information, object details, and context for parsing and analysis.
 */
public class ObjectLocation extends ContextLocation {

    @Serial
    private static final long serialVersionUID = -1854478368569213500L;

    /**
     * Enumeration of location types for database objects.
     */
    public enum LocationType {
        DEFINITION,
        REFERENCE,
        VARIABLE,
        LOCAL_REF
    }

    private DangerStatement danger;

    private final int length;
    private final String action;
    private final String sql;
    private final String alias;
    private final ObjectReference objectReference;
    private final LocationType locationType;

    private ObjectLocation(String filePath, int offset, int lineNumber,
                           int charPositionInLine, ObjectReference objectReference, String action,
                           String sql, String alias, int length, LocationType locationType) {
        super(filePath, offset, lineNumber, charPositionInLine);
        this.objectReference = objectReference;
        this.sql = sql;
        this.action = action;
        this.length = length;
        this.locationType = locationType;
        this.alias = alias;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (obj instanceof ObjectLocation loc) {
            return Objects.equals(loc.getObjectReference(), getObjectReference())
                    && Objects.equals(loc.getSql(), getSql())
                    && Objects.equals(loc.getAction(), getAction());

        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (objectReference == null ? 0 : objectReference.hashCode());
        result = prime * result + ((getSql() == null) ? 0 : getSql().hashCode());
        result = prime * result + ((getAction() == null) ? 0 : getAction().hashCode());
        return result;
    }

    public void setWarning(DangerStatement danger) {
        this.danger = danger;
    }

    /**
     * Checks if this location has a danger warning.
     *
     * @return true if there is a danger warning
     */
    public boolean isDanger() {
        return danger != null;
    }

    public DangerStatement getDanger() {
        return danger;
    }

    public ObjectReference getObjectReference() {
        return objectReference;
    }

    public int getObjLength() {
        return length;
    }

    public String getAction() {
        return action;
    }

    public String getSql() {
        return sql;
    }

    public LocationType getLocationType() {
        return locationType;
    }

    /**
     * Checks if this location represents a global reference.
     *
     * @return true if the location is a definition or reference type
     */
    public boolean isGlobal() {
        return locationType == LocationType.DEFINITION || locationType == LocationType.REFERENCE;
    }

    /**
     * Gets the object name.
     *
     * @return object name or empty string if no object
     */
    public String getName() {
        return objectReference != null ? objectReference.getName() : "";
    }

    /**
     * Gets the schema name.
     *
     * @return schema name or null if no object
     */
    public String getSchema() {
        return objectReference == null ? null : objectReference.schema();
    }

    /**
     * Gets the table name.
     *
     * @return table name or null if no object
     */
    public String getTable() {
        return objectReference == null ? null : objectReference.table();
    }

    /**
     * Gets the column name.
     *
     * @return column name or null if no object
     */
    public String getColumn() {
        return objectReference == null ? null : objectReference.column();
    }

    /**
     * Gets the database object type.
     *
     * @return object type or null if no object
     */
    public DbObjType getType() {
        return objectReference == null ? null : objectReference.type();
    }

    /**
     * @return alias or name for object
     */
    public String getBareName() {
        if (alias != null) {
            return alias;
        }
        return getName();
    }

    /**
     * Compares this location with another location for equality.
     *
     * @param loc the location to compare with
     * @return true if the locations refer to the same object
     */
    public final boolean compare(ObjectLocation loc) {
        if (isGlobal() != loc.isGlobal() || !Objects.equals(alias, loc.alias)) {
            return false;
        }

        ObjectReference col = loc.getObjectReference();
        if (objectReference == null || col == null) {
            return false;
        }
        return compareTypes(col.type())
                && Objects.equals(objectReference.schema(), col.schema())
                && Objects.equals(objectReference.column(), col.column())
                && Objects.equals(objectReference.table(), col.table());
    }

    private boolean compareTypes(DbObjType objType) {
        DbObjType type = objectReference.type();
        if (type == objType) {
            return true;
        }

        return switch (objType) {
            case TABLE, VIEW, SEQUENCE -> type.in(DbObjType.TABLE, DbObjType.VIEW, DbObjType.SEQUENCE);
            case FUNCTION, AGGREGATE, PROCEDURE ->
                    type.in(DbObjType.FUNCTION, DbObjType.AGGREGATE, DbObjType.PROCEDURE);
            case TYPE, DOMAIN -> type.in(DbObjType.TYPE, DbObjType.DOMAIN);
            default -> false;
        };
    }

    /**
     * Creates a copy of this location with adjusted position offsets.
     *
     * @param offset       the offset adjustment
     * @param lineOffset   the line number adjustment
     * @param inLineOffset the character position adjustment
     * @param filePath     the new file path
     * @return a new PgObjLocation with adjusted position
     */
    public ObjectLocation copyWithOffset(int offset, int lineOffset,
                                         int inLineOffset, String filePath) {
        int newCharPosition = getLineNumber() == 1 ? getCharPositionInLine() + inLineOffset : getCharPositionInLine();
        ObjectLocation loc = new ObjectLocation(filePath,
                getOffset() + offset,
                getLineNumber() + lineOffset,
                newCharPosition,
                objectReference, action, sql, alias, length, locationType);
        loc.setWarning(danger);
        return loc;
    }

    @Override
    public String toString() {
        ObjectReference gc = getObjectReference();
        if (gc != null) {
            return gc.toString();
        }

        return super.toString();
    }

    /**
     * Builder class for constructing PgObjLocation instances.
     */
    public static final class Builder {

        private String filePath;
        private String action;
        private String sql;
        private String alias;
        private int offset;
        private int lineNumber;
        private int charPositionInLine;
        private ObjectReference reference;
        private ParserRuleContext ctx;
        private ParserRuleContext endCtx;
        private LocationType locationType = LocationType.REFERENCE;

        public Builder setFilePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public Builder setAction(String action) {
            this.action = action;
            return this;
        }

        public Builder setSql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder setAlias(String alias) {
            this.alias = alias;
            return this;
        }

        public Builder setOffset(int offset) {
            this.offset = offset;
            return this;
        }

        public Builder setLineNumber(int lineNumber) {
            this.lineNumber = lineNumber;
            return this;
        }

        public Builder setCharPositionInLine(int charPositionInLine) {
            this.charPositionInLine = charPositionInLine;
            return this;
        }

        public Builder setReference(ObjectReference reference) {
            this.reference = reference;
            return this;
        }

        public Builder setCtx(ParserRuleContext ctx) {
            this.ctx = ctx;
            return this;
        }

        public Builder setEndCtx(ParserRuleContext endCtx) {
            this.endCtx = endCtx;
            return this;
        }

        public Builder setLocationType(LocationType locationType) {
            this.locationType = locationType;
            return this;
        }

        /**
         * Builds a PgObjLocation instance from the configured parameters.
         *
         * @return PgObjLocation object
         */
        public ObjectLocation build() {
            if (ctx != null) {
                CodeUnitToken start = (CodeUnitToken) ctx.getStart();
                int startOffset = start.getCodeUnitStart();
                int line = start.getLine();
                int position = start.getCodeUnitPositionInLine();
                CodeUnitToken stop = (CodeUnitToken) (endCtx != null ? endCtx : ctx).getStop();
                int length = stop.getCodeUnitStop() - startOffset + 1;
                return new ObjectLocation(filePath, startOffset, line, position,
                        reference, action, sql, alias, length, locationType);
            }

            int length = reference == null ? 0 : reference.getName().length();
            return new ObjectLocation(filePath, offset, lineNumber, charPositionInLine,
                    reference, action, sql, alias, length, locationType);
        }
    }
}
