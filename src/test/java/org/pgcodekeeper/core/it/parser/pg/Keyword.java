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
package org.pgcodekeeper.core.it.parser.pg;

import static org.pgcodekeeper.core.it.parser.pg.Keyword.LabelCategory.AS_LABEL;
import static org.pgcodekeeper.core.it.parser.pg.Keyword.LabelCategory.BARE_LABEL;

import java.util.*;

import org.pgcodekeeper.core.sql.KeywordCategory;

/**
 * PostgreSQL keyword classification and management. Deprecated class. Use PgKeyword, MsKeyword
 * and ChKeyword classes.
 * Contains complete PostgreSQL keyword dictionary with categories and label information.
 * <p>
 * {@link #KEYWORDS} list maintenance:
 * <ol>
 * <li>Copy code from
 * <a href='https://github.com/postgres/postgres/blob/REL9_6_STABLE/src/include/parser/kwlist.h'>
 * kwlist.h</a>, use your desired stable branch with actual version of postgres.</li>
 * <li>Paste it into {@link #addKeywords(Map)}, replacing the code there.</li>
 * <li>In pasted code, replace <code>PG_KEYWORD\(("\w+"), \w+, (\w+), (\w+)\)</code> by
 * <code>addKw\(map, $1, $2, $3\);</code> using regular expressions.</li>
 * </ol>
 *
 * @author levsha_aa
 */
@Deprecated
public class Keyword {

    public static final Map<String, Keyword> KEYWORDS;

    /**
     * Label categories for keyword usage in different contexts.
     */
    public enum LabelCategory {
        BARE_LABEL("bare_label_keyword"),
        AS_LABEL("");

        private final String parserRule;

        LabelCategory(String parserRule) {
            this.parserRule = parserRule;
        }

        String getParserRule() {
            return parserRule;
        }
    }

    /*
     * Regex search and replacement strings for kwlist.h -> Java transformation:
     *
     * PG_KEYWORD\(("\w+"), \w+, (\w+), (\w+)\)
     * addKw\(map, KeywordCategory.$1, $2, $3\);
     */
    private static void addKeywords(Map<String, Keyword> map) {
        addKw(map, "abort", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "absent", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "absolute", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "access", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "action", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "add", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "admin", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "after", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "aggregate", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "all", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "also", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "alter", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "always", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "analyse", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);        /* British spelling */
        addKw(map, "analyze", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "and", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "any", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "array", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "as", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "asc", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "asensitive", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "assertion", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "assignment", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "asymmetric", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "at", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "atomic", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "attach", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "attribute", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "authorization", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "backward", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "before", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "begin", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "between", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "bigint", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "binary", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "bit", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "boolean", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "both", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "breadth", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "by", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "cache", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "call", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "called", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "cascade", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "cascaded", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "case", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "cast", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "catalog", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "chain", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "char", KeywordCategory.COL_NAME_KEYWORD, AS_LABEL);
        addKw(map, "character", KeywordCategory.COL_NAME_KEYWORD, AS_LABEL);
        addKw(map, "characteristics", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "check", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "checkpoint", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "class", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "close", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "cluster", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "coalesce", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "collate", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "collation", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "column", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "columns", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "comment", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "comments", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "commit", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "committed", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "compression", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "concurrently", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "conditional", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "configuration", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "conflict", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "connection", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "constraint", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "constraints", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "content", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "continue", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "conversion", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "copy", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "cost", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "create", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "cross", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "csv", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "cube", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "current", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "current_catalog", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "current_date", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "current_role", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "current_schema", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "current_time", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "current_timestamp", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "current_user", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "cursor", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "cycle", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "data", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "database", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "day", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "deallocate", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "dec", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "decimal", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "declare", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "default", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "defaults", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "deferrable", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "deferred", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "definer", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "delete", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "delimiter", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "delimiters", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "depends", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "depth", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "desc", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "detach", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "dictionary", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "disable", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "discard", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "distinct", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "do", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "document", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "domain", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "double", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "drop", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "each", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "else", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "empty", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "enable", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "encoding", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "encrypted", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "end", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "enforced", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "enum", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "error", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "escape", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "event", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "except", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "exclude", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "excluding", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "exclusive", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "execute", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "exists", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "explain", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "expression", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "extension", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "external", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "extract", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "false", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "family", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "fetch", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "filter", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "finalize", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "first", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "float", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "following", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "for", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "force", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "foreign", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "format", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "forward", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "freeze", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "from", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "full", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "function", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "functions", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "generated", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "global", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "grant", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "granted", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "greatest", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "group", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "grouping", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "groups", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "handler", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "having", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "header", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "hold", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "hour", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "identity", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "if", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "ilike", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "immediate", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "immutable", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "implicit", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "import", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "in", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "include", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "including", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "increment", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "indent", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "index", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "indexes", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "inherit", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "inherits", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "initially", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "inline", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "inner", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "inout", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "input", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "insensitive", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "insert", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "instead", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "int", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "integer", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "intersect", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "interval", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "into", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "invoker", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "is", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "isnull", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, AS_LABEL);
        addKw(map, "isolation", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "join", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "json_array", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json_arrayagg", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json_exists", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json_object", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json_objectagg", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json_query", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json_scalar", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json_serialize", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json_table", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "json_value", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "keep", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "key", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "keys", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "label", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "language", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "large", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "last", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "lateral", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "leading", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "leakproof", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "least", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "left", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "level", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "like", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "limit", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "listen", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "load", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "local", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "localtime", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "localtimestamp", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "location", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "lock", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "locked", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "logged", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "mapping", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "match", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "matched", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "materialized", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "maxvalue", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "merge", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "merge_action", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "method", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "minute", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "minvalue", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "mode", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "month", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "move", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "name", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "names", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "national", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "natural", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "nchar", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "nested", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "new", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "next", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "nfc", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "nfd", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "nfkc", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "nfkd", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "no", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "none", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "normalize", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "normalized", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "not", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "nothing", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "notify", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "notnull", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, AS_LABEL);
        addKw(map, "nowait", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "null", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "nullif", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "nulls", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "numeric", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "object", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "objects", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "of", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "off", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "offset", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "oids", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "old", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "omit", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "on", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "only", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "operator", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "option", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "options", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "or", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "order", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "ordinality", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "others", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "out", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "outer", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "over", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "overlaps", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, AS_LABEL);
        addKw(map, "overlay", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "overriding", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "owned", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "owner", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "parallel", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "parameter", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "parser", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "partial", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "partition", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "passing", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "password", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "path", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "period", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "placing", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "plan", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "plans", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "policy", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "position", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "preceding", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "precision", KeywordCategory.COL_NAME_KEYWORD, AS_LABEL);
        addKw(map, "prepare", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "prepared", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "preserve", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "primary", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "prior", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "privileges", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "procedural", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "procedure", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "procedures", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "program", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "publication", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "quote", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "quotes", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "range", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "read", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "real", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "reassign", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "recursive", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "ref", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "references", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "referencing", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "refresh", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "reindex", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "relative", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "release", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "rename", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "repeatable", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "replace", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "replica", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "reset", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "restart", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "restrict", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "return", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "returning", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "returns", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "revoke", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "right", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "role", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "rollback", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "rollup", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "routine", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "routines", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "row", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "rows", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "rule", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "savepoint", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "scalar", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "schema", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "schemas", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "scroll", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "search", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "second", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "security", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "select", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "sequence", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "sequences", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "serializable", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "server", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "session", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "session_user", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "set", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "setof", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "sets", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "share", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "show", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "similar", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "simple", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "skip_", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);  // skip is reserved by ANTLR
        addKw(map, "smallint", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "snapshot", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "some", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "source", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "sql", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "stable", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "standalone", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "start", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "statement", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "statistics", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "stdin", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "stdout", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "storage", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "stored", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "strict", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "string", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "strip", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "subscription", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "substring", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "support", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "symmetric", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "sysid", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "system", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "system_user", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "table", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "tables", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "tablesample", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "tablespace", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "target", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "temp", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "template", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "temporary", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "text", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "then", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "ties", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "time", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "timestamp", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "to", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "trailing", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "transaction", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "transform", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "treat", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "trigger", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "trim", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "true", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "truncate", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "trusted", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "type", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "types", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "uescape", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "unbounded", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "uncommitted", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "unconditional", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "unencrypted", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "union", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "unique", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "unknown", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "unlisten", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "unlogged", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "until", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "update", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "user", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "using", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "vacuum", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "valid", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "validate", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "validator", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "value", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "values", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "varchar", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "variadic", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "varying", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "verbose", KeywordCategory.TYPE_FUNC_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "version", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "view", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "views", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "virtual", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "volatile", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "when", KeywordCategory.RESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "where", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "whitespace", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "window", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "with", KeywordCategory.RESERVED_KEYWORD, AS_LABEL);
        addKw(map, "within", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "without", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "work", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "wrapper", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "write", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "xml", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "xmlattributes", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmlconcat", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmlelement", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmlexists", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmlforest", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmlnamespaces", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmlparse", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmlpi", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmlroot", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmlserialize", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "xmltable", KeywordCategory.COL_NAME_KEYWORD, BARE_LABEL);
        addKw(map, "year", KeywordCategory.UNRESERVED_KEYWORD, AS_LABEL);
        addKw(map, "yes", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
        addKw(map, "zone", KeywordCategory.UNRESERVED_KEYWORD, BARE_LABEL);
    }

    private static void addKw(Map<String, Keyword> map, String kw,
                              KeywordCategory keyword, LabelCategory label) {
        map.put(kw, new Keyword(kw, keyword, label));
    }

    static {
        Map<String, Keyword> keywords = new HashMap<>();
        addKeywords(keywords);
        KEYWORDS = Collections.unmodifiableMap(keywords);
    }

    private final String keyword;
    private final KeywordCategory category;
    private final LabelCategory labelCategory;

    /**
     * Creates a new keyword with specified properties.
     *
     * @param keyword       the keyword string
     * @param category      the keyword category
     * @param labelCategory the label category
     */
    public Keyword(String keyword, KeywordCategory category, LabelCategory labelCategory) {
        this.keyword = keyword;
        this.category = category;
        this.labelCategory = labelCategory;
    }

    public String getKeyword() {
        return keyword;
    }

    public KeywordCategory getCategory() {
        return category;
    }

    public LabelCategory getLabelCategory() {
        return labelCategory;
    }

    /**
     * INTERNAL USE ONLY
     * <p>
     * Generates a formatted string containing all SQL keywords organized by categories
     * in token format suitable for lexer grammar files. Keywords are grouped by category
     * with section headers and alphabetically sorted within each category.
     *
     * @return a formatted string with keywords in token format (e.g., "KEYWORD: 'KEYWORD';")
     */
    public static String getAllTokensByGroups() {
        KeywordCategory[] prevCat = new KeywordCategory[1];
        char[] prevFirstLetter = new char[1];
        StringBuilder result = new StringBuilder();

        Arrays.stream(KeywordCategory.values())
                .flatMap(kc -> KEYWORDS.values().stream()
                        .filter(k -> k.getCategory() == kc)
                        .sorted(Comparator.comparing(Keyword::getKeyword)))
                .forEach(v -> {
                    var currentCat = v.getCategory();
                    if (prevCat[0] != currentCat) {
                        result.append("\n");
                        result.append("    /*\n");
                        result.append("    ==================================================\n");
                        result.append("    ").append(currentCat).append("\n");
                        result.append("    ==================================================\n");
                        result.append("    */\n");
                        prevCat[0] = currentCat;
                    }
                    String kUpper = v.getKeyword().toUpperCase(Locale.ROOT);
                    char firstLetter = kUpper.charAt(0);
                    if (prevFirstLetter[0] != firstLetter) {
                        result.append("\n");
                        prevFirstLetter[0] = firstLetter;
                    }
                    result.append("    ").append(kUpper).append(": '").append(kUpper).append("';\n");
                });

        return result.toString();
    }

    /**
     * INTERNAL USE ONLY
     * <p>
     * Generates a formatted string containing all SQL keywords organized by categories.
     * Keywords are grouped by category with section
     * headers and includes a separate section for bare label keywords.
     *
     * @return a formatted string with keywords organized by categories in list format
     */
    public static String getAllWordsByGroups() {
        Map<KeywordCategory, StringBuilder> map = new EnumMap<>(KeywordCategory.class);
        StringBuilder sbBare = new StringBuilder();
        StringBuilder result = new StringBuilder();

        KEYWORDS.values().stream()
                .sorted(Comparator.comparing(Keyword::getKeyword))
                .forEach(v -> {
                    StringBuilder sb = map.computeIfAbsent(v.getCategory(), k -> new StringBuilder());
                    sb.append("    | ").append(v.getKeyword().toUpperCase(Locale.ROOT)).append("\n");
                    if (v.getLabelCategory() == BARE_LABEL) {
                        sbBare.append("    | ").append(v.getKeyword().toUpperCase(Locale.ROOT)).append("\n");
                    }
                });

        result.append(BARE_LABEL.getParserRule()).append("\n");
        result.append(sbBare.replace(4, 5, ":")).append("    ;\n\n");

        map.keySet().stream()
                .sorted()
                .forEach(k -> {
                    result.append(k.getParserRule()).append("\n");
                    result.append(map.get(k).replace(4, 5, ":")).append("    ;\n\n");
                });

        return result.toString();
    }
}
