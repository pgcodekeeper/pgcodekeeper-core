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
package org.pgcodekeeper.core.database.pg.schema;

import java.util.Objects;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * PostgreSQL full-text search parser implementation.
 * Parsers break down documents into tokens and classify them into types.
 * Each parser consists of several functions that handle different parsing phases.
 */
public class PgFtsParser extends PgAbstractStatement implements ISearchPath {

    private static final String NEW_LINE = ",\n\t";

    private String startFunction;
    private String getTokenFunction;
    private String endFunction;
    private String headLineFunction;
    private String lexTypesFunction;

    /**
     * Creates a new PostgreSQL FTS parser.
     *
     * @param name parser name
     */
    public PgFtsParser(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FTS_PARSER;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        StringBuilder sbSql = new StringBuilder();
        sbSql.append("CREATE TEXT SEARCH PARSER ")
                .append(getQualifiedName()).append(" (\n\t")
                .append("START = ").append(startFunction).append(NEW_LINE)
                .append("GETTOKEN = ").append(getTokenFunction).append(NEW_LINE)
                .append("END = ").append(endFunction).append(NEW_LINE);
        if (headLineFunction != null) {
            sbSql.append("HEADLINE = ").append(headLineFunction).append(NEW_LINE);
        }
        sbSql.append("LEXTYPES = ").append(lexTypesFunction);

        sbSql.append(" )");
        script.addStatement(sbSql);
        appendComments(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        var newParser = (PgFtsParser) newCondition;
        if (!compareUnalterable(newParser)) {
            return ObjectState.RECREATE;
        }
        appendAlterComments(newParser, script);

        return getObjectState(script, startSize);
    }

    public void setStartFunction(final String startFunction) {
        this.startFunction = startFunction;
        resetHash();
    }

    public void setGetTokenFunction(final String getTokenFunction) {
        this.getTokenFunction = getTokenFunction;
        resetHash();
    }

    public void setEndFunction(final String endFunction) {
        this.endFunction = endFunction;
        resetHash();
    }

    public void setLexTypesFunction(final String lexTypesFunction) {
        this.lexTypesFunction = lexTypesFunction;
        resetHash();
    }

    public void setHeadLineFunction(final String headLineFunction) {
        this.headLineFunction = headLineFunction;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(startFunction);
        hasher.put(getTokenFunction);
        hasher.put(endFunction);
        hasher.put(lexTypesFunction);
        hasher.put(headLineFunction);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof PgFtsParser parser && super.compare(obj)
                && compareUnalterable(parser);
    }

    private boolean compareUnalterable(PgFtsParser parser) {
        return Objects.equals(startFunction, parser.startFunction)
                && Objects.equals(getTokenFunction, parser.getTokenFunction)
                && Objects.equals(endFunction, parser.endFunction)
                && Objects.equals(lexTypesFunction, parser.lexTypesFunction)
                && Objects.equals(headLineFunction, parser.headLineFunction);
    }

    @Override
    protected PgFtsParser getCopy() {
        PgFtsParser parserDst = new PgFtsParser(name);
        parserDst.setStartFunction(startFunction);
        parserDst.setGetTokenFunction(getTokenFunction);
        parserDst.setEndFunction(endFunction);
        parserDst.setLexTypesFunction(lexTypesFunction);
        parserDst.setHeadLineFunction(headLineFunction);
        return parserDst;
    }
}
