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
package org.pgcodekeeper.core.database.pg.utils;

import java.util.Collection;
import java.util.Set;

/**
 * Constants related to PostgreSQL and Greenplum.
 * <p>
 * This interface contains a set of string constants used for working with
 * system catalogs, schemas, and other PostgreSQL/Greenplum elements.
 * </p>
 */
public class PgConsts {

  public static final String DEFAULT_SCHEMA = "public";
  public static final String PG_CATALOG = "pg_catalog";

  public enum FUNC_SIGN {
      IN("(cstring)"),
      IN_ADVANCED("(cstring, oid, integer)"),
      INTERNAL("(internal)"),
      TYPMOD_IN("(cstring[])"),
      TYPMOD_OUT("(integer)"),
      REC_ADVANCED("(internal, oid, integer)"),
      SUBTYPE_DIFF("(%1$s, %1$s)"),
      TYPE_NAME("(%s.%s)");

      private final String name;

      FUNC_SIGN(String name) {
          this.name = name;
      }

      public String getName() {
          return name;
      }
  }

  /**
   * @deprecated improve builtins detection using tokens and jdbc ways
   */
  @Deprecated
  public static final Collection<String> SYS_TYPES = Set.of(
          "abstime",
          "aclitem",
          "any",
          "anyarray",
          "anyelement",
          "anyenum",
          "anynonarray",
          "anyrange",
          "bigint",
          "bigserial",
          "bit",
          "bit varying",
          "boolean",
          "box",
          "bpchar",
          "bytea",
          "char",
          "character",
          "character varying",
          "cid",
          "cidr",
          "circle",
          "cstring",
          "date",
          "daterange",
          "double precision",
          "event_trigger",
          "fdw_handler",
          "gtsvector",
          "inet",
          "int2vector",
          "int4range",
          "int8range",
          "integer",
          "internal",
          "interval",
          "json",
          "jsonb",
          "language_handler",
          "line",
          "lseg",
          "macaddr",
          "money",
          "name",
          "numeric",
          "numrange",
          "oid",
          "oidvector",
          "opaque",
          "path",
          "pg_node_tree",
          "point",
          "polygon",
          "real",
          "record",
          "refcursor",
          "regclass",
          "regconfig",
          "regdictionary",
          "regoper",
          "regoperator",
          "regproc",
          "regprocedure",
          "regtype",
          "reltime",
          "serial",
          "smallint",
          "smgr",
          "text",
          "tid",
          "timestamp without time zone",
          "timestamp with time zone",
          "time without time zone",
          "time with time zone",
          "tinterval",
          "trigger",
          "tsquery",
          "tsrange",
          "tstzrange",
          "tsvector",
          "txid_snapshot",
          "unknown",
          "uuid",
          "void",
          "xid",
          "xml"
  );
}
