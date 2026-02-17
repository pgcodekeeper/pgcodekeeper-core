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

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * PostgreSQL full-text search configuration implementation.
 * Configurations specify which text search parser to use and
 * how to map token types to dictionaries for text processing.
 */
public class PgFtsConfiguration extends PgAbstractStatement implements ISearchPath {

    private static final String ALTER_CONFIGURATION = "ALTER TEXT SEARCH CONFIGURATION ";
    private static final String WITH = "\n\tWITH ";

    /**
     * key - fragment, value - dictionaries
     */
    private final Map<String, String> dictionariesMap = new HashMap<>();

    private String parser;

    /**
     * Creates a new PostgreSQL FTS configuration.
     *
     * @param name configuration name
     */
    public PgFtsConfiguration(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FTS_CONFIGURATION;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TEXT SEARCH CONFIGURATION ")
                .append(getQualifiedName()).append(" (\n\t");
        sql.append("PARSER = ").append(parser).append(" )");
        script.addStatement(sql);

        dictionariesMap.forEach((fragment, dictionaries) -> {
            StringBuilder sqlAction = new StringBuilder();
            sqlAction.append(ALTER_CONFIGURATION).append(getQualifiedName());
            sqlAction.append("\n\tADD MAPPING FOR ").append(fragment)
                    .append(WITH).append(dictionaries);
            script.addStatement(sqlAction);
        });

        appendOwnerSQL(script);
        appendComments(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgFtsConfiguration newConf = (PgFtsConfiguration) newCondition;

        if (!newConf.getParser().equals(parser)) {
            return ObjectState.RECREATE;
        }

        compareOptions(newConf, script);

        appendAlterOwner(newConf, script);
        appendAlterComments(newConf, script);

        return getObjectState(script, startSize);
    }

    private void compareOptions(PgFtsConfiguration newConf, SQLScript script) {
        Map<String, String> oldMap = dictionariesMap;
        Map<String, String> newMap = newConf.dictionariesMap;

        if (oldMap.isEmpty() && newMap.isEmpty()) {
            return;
        }

        oldMap.forEach((fragment, dictionary) -> {
            String newDictionary = newMap.get(fragment);
            if (newDictionary == null) {
                script.addStatement(getAlterConfiguration(fragment));
            } else if (!dictionary.equals(newDictionary)) {
                script.addStatement(getAlterConfiguration("ALTER", fragment, newDictionary));
            }
        });

        newMap.forEach((fragment, dictionary) -> {
            if (!oldMap.containsKey(fragment)) {
                script.addStatement(getAlterConfiguration("ADD", fragment, dictionary));
            }
        });
    }

    private String getAlterConfiguration(String fragment) {
        return getAlterConfiguration("DROP", fragment, null);
    }

    private String getAlterConfiguration(String action, String fragment, String dictionary) {
        StringBuilder sqlAction = new StringBuilder(ALTER_CONFIGURATION).append(getQualifiedName())
                .append("\n\t%s MAPPING FOR ".formatted(action))
                .append(fragment);
        if (null != dictionary) {
            sqlAction.append(WITH).append(dictionary);
        }
        return sqlAction.toString();
    }

    /**
     * Gets the text search parser used by this configuration.
     *
     * @return parser name
     */
    public String getParser() {
        return parser;
    }

    public void setParser(final String parser) {
        this.parser = parser;
        resetHash();
    }

    /**
     * Adds dictionary mapping for a token fragment.
     *
     * @param fragment     token fragment type
     * @param dictionaries list of dictionaries to use for this fragment
     */
    public void addDictionary(String fragment, List<String> dictionaries) {
        dictionariesMap.put(fragment, String.join(", ", dictionaries));
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(parser);
        hasher.put(dictionariesMap);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof PgFtsConfiguration config && super.compare(obj)
                && Objects.equals(parser, config.parser)
                && dictionariesMap.equals(config.dictionariesMap);
    }

    @Override
    protected PgFtsConfiguration getCopy() {
        PgFtsConfiguration confDst = new PgFtsConfiguration(name);
        confDst.setParser(parser);
        confDst.dictionariesMap.putAll(dictionariesMap);
        return confDst;
    }
}
