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
 *
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.it.loader.pg;

import org.junit.jupiter.api.Test;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.EventType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.database.pg.schema.PgTrigger.TgTypes;
import org.pgcodekeeper.core.loader.ProjectLoader;
import org.pgcodekeeper.core.model.exporter.ModelExporter;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.utils.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.pgcodekeeper.core.it.IntegrationTestUtils.*;

/**
 * Tests for PgDiffLoader class.
 *
 * @author fordfrog
 */
class PgAntlrLoaderTest {

    private static final String POSTGRES = "postgres";
    private static final String BIGINT = "bigint";
    private static final String BOOLEAN = "boolean";
    private static final String CHARACTER_VARYING_40 = "character varying(40)";
    private static final String INTEGER = "integer";

    void testDatabase(String fileName, AbstractDatabase d) throws IOException, InterruptedException {
        loadSchema(fileName, d);
        exportFullDb(fileName, d);
    }

    void loadSchema(String fileName, AbstractDatabase dbPredefined) throws IOException, InterruptedException {
        // first test the dump loader itself
        var settings = new CoreSettings();
        settings.setKeepNewlines(true);
        AbstractDatabase d = loadTestDump(fileName, PgAntlrLoaderTest.class, settings);

        assertDiff(d, dbPredefined, settings, "PgDumpLoader: predefined object is not equal to file " + fileName);

        // test deepCopy mechanism
        assertDiff(d, (AbstractDatabase) d.deepCopy(), settings, "PgStatement deep copy altered");
        assertDiff(dbPredefined, d, settings,"PgStatement deep copy altered original");
    }

    void exportFullDb(String fileName, AbstractDatabase dbPredefined) throws IOException, InterruptedException {
        // prepare db object from sql file
        var settings = new CoreSettings();
        settings.setKeepNewlines(true);
        AbstractDatabase dbFromFile = loadTestDump(fileName, PgAntlrLoaderTest.class, settings);

        Path exportDir;
        try (TempDir dir = new TempDir("pgCodekeeper-test-files")) {
            exportDir = dir.get();
            new ModelExporter(exportDir, dbPredefined, DatabaseType.PG, Consts.UTF_8, settings).exportFull();

            AbstractDatabase dbAfterExport = new ProjectLoader(exportDir.toString(), settings)
                    .loadAndAnalyze();

            // check the same db similarity before and after export

            assertDiff(dbPredefined, dbAfterExport, settings, "Predefined object PgDB" + fileName +
                    " is not equal to exported'n'loaded.");

            assertDiff(dbAfterExport, dbFromFile, settings,
                    "Exported predefined object is not equal to file " + fileName);
        }
    }

    private PgConstraintNotNull createNotNullConstraint(PgSimpleTable table, PgColumn col) {
        var notNullConstraint = new PgConstraintNotNull(table.getName(), col.getName());
        notNullConstraint.setParent(col);
        col.setNotNullConstraint(notNullConstraint);
        return notNullConstraint;
    }

    @Test
    void testDB1() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        PgSimpleTable table = new PgSimpleTable("fax_boxes");
        schema.addTable(table);

        PgColumn col = new PgColumn("fax_box_id");
        col.setType("serial");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("name");
        col.setType("text");
        table.addColumn(col);

        var constraintPK = new PgConstraintPk("fax_boxes_pkey", true);
        constraintPK.addColumn("fax_box_id");
        constraintPK.addInclude("name");
        table.addConstraint(constraintPK);

        table.setOwner(POSTGRES);

        table = new PgSimpleTable("faxes");
        schema.addTable(table);

        col = new PgColumn("fax_id");
        col.setType("serial");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("fax_box_id");
        col.setType(INTEGER);
        table.addColumn(col);

        col = new PgColumn("from_name");
        col.setType("text");
        table.addColumn(col);

        col = new PgColumn("from_number");
        col.setType("text");
        table.addColumn(col);

        col = new PgColumn("status");
        col.setType(INTEGER);
        table.addColumn(col);

        col = new PgColumn("pages");
        col.setType(INTEGER);
        table.addColumn(col);

        col = new PgColumn("time_received");
        col.setType("timestamp");
        col.setDefaultValue("now()");
        table.addColumn(col);

        col = new PgColumn("time_finished_received");
        col.setType("timestamp");
        table.addColumn(col);

        col = new PgColumn("read");
        col.setType("smallint");
        col.setDefaultValue("0");
        table.addColumn(col);

        col = new PgColumn("station_id");
        col.setType("text");
        table.addColumn(col);

        constraintPK = new PgConstraintPk("faxes_pkey", true);
        constraintPK.addColumn("fax_id");
        table.addConstraint(constraintPK);

        var constraintFk = new PgConstraintFk("faxes_fax_box_id_fkey");
        constraintFk.addColumn("fax_box_id");
        constraintFk.setForeignSchema("public");
        constraintFk.setForeignTable("fax_boxes");
        constraintFk.setMatch("SIMPLE");
        constraintFk.setUpdAction("RESTRICT");
        constraintFk.setDelAction("SET NULL");
        constraintFk.addDelActCol("fax_box_id");
        table.addConstraint(constraintFk);

        table = new PgSimpleTable("extensions");
        schema.addTable(table);

        col = new PgColumn("id");
        col.setType("serial");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        constraintFk = new PgConstraintFk("extensions_fax_box_id_fkey");
        constraintFk.addColumn("id");
        constraintFk.setForeignSchema("public");
        constraintFk.setForeignTable("fax_boxes");
        constraintFk.addForeignColumn("fax_box_id");
        constraintFk.setUpdAction("RESTRICT");
        constraintFk.setDelAction("RESTRICT");
        table.addConstraint(constraintFk);

        testDatabase("schema_1.sql", d);
    }

    @Test
    void testDB2() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);

        AbstractSchema schema = new PgSchema("postgis");
        d.addSchema(schema);

        PgExtension ext = new PgExtension("postgis");
        ext.setSchema("postgis");
        d.addChild(ext);
        ext.setComment("'PostGIS geometry, geography, and raster spatial types and functions'");

        schema = d.getSchema(Consts.PUBLIC);

        AbstractTable table = new PgSimpleTable("contacts");
        schema.addTable(table);

        AbstractColumn col = new PgColumn("id");
        col.setType(INTEGER);
        table.addColumn(col);

        col = new PgColumn("number_pool_id");
        col.setType(INTEGER);
        table.addColumn(col);

        col = new PgColumn("name");
        col.setType("character varying(50)");
        table.addColumn(col);

        PgIndex idx = new PgIndex("contacts_number_pool_id_idx");
        table.addIndex(idx);
        idx.addColumn(new SimpleColumn("number_pool_id"));
        testDatabase("schema_2.sql", d);
    }

    @Test
    void testDB3() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        AbstractSequence seq = new PgSequence("admins_aid_seq");
        seq.setStartWith("1");
        seq.setMinMaxInc(1L, 1000000000L, null, null, 0L);
        seq.setCache("1");
        schema.addSequence(seq);

        PgSimpleTable table = new PgSimpleTable("admins");
        schema.addTable(table);

        PgColumn col = new PgColumn("aid");
        col.setType(INTEGER);
        col.setDefaultValue("nextval('\"admins_aid_seq\"'::regclass)");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("companyid");
        col.setType(INTEGER);
        col.setDefaultValue("0");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("groupid");
        col.setType(INTEGER);
        col.setDefaultValue("0");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("username");
        col.setType("character varying");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("password");
        col.setType(CHARACTER_VARYING_40);
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("superuser");
        col.setType(BOOLEAN);
        col.setDefaultValue("'f'::bool");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("name");
        col.setType(CHARACTER_VARYING_40);
        table.addColumn(col);

        col = new PgColumn("surname");
        col.setType(CHARACTER_VARYING_40);
        table.addColumn(col);

        col = new PgColumn("email");
        col.setType("character varying(100)");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("tel");
        col.setType(CHARACTER_VARYING_40);
        table.addColumn(col);

        col = new PgColumn("mobile");
        col.setType(CHARACTER_VARYING_40);
        table.addColumn(col);

        col = new PgColumn("enabled");
        col.setType(BOOLEAN);
        col.setDefaultValue("'t'::bool");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("lastlogints");
        col.setType("timestamp with time zone");
        col.setDefaultValue("now()");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("expirienced");
        col.setType(BOOLEAN);
        col.setDefaultValue("'f'::bool");
        table.addColumn(col);

        var constraintPK = new PgConstraintPk("admins_pkey", true);
        constraintPK.addColumn("aid");
        table.addConstraint(constraintPK);

        testDatabase("schema_3.sql", d);
    }

    @Test
    void testDB4() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        PgSimpleTable table = new PgSimpleTable("call_logs");
        schema.addTable(table);

        PgColumn col = new PgColumn("id");
        col.setType(BIGINT);
        createNotNullConstraint(table, col);
        col.setDefaultValue("nextval('public.call_logs_id_seq'::regclass)");
        table.addColumn(col);

        testDatabase("schema_4.sql", d);
    }

    @Test
    void testDB5() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        PgFunction func = new PgFunction("gtsq_in");
        func.setLanguageCost("c", null);
        func.setStrict(true);
        func.setBody("'$libdir/tsearch2', 'gtsq_in'");
        func.setReturns("gtsq");

        Argument arg = new Argument(null, "cstring");
        func.addArgument(arg);

        schema.addFunction(func);

        func = new PgFunction("multiply_numbers");
        func.setLanguageCost("plpgsql", null);
        func.setStrict(true);
        func.setBody("$$\r\nbegin\r\n\treturn number1 * number2;\r\nend;\r\n$$");
        func.setReturns(INTEGER);

        arg = new Argument("number1", INTEGER);
        func.addArgument(arg);

        arg = new Argument("number2", INTEGER);
        func.addArgument(arg);

        schema.addFunction(func);

        func = new PgFunction("select_something");
        func.setLanguageCost("sql", null);
        func.setBody("$$SELECT number1 * number2$$");
        func.setReturns(INTEGER);

        arg = new Argument("number1", INTEGER);
        func.addArgument(arg);

        arg = new Argument("number2", INTEGER);
        func.addArgument(arg);

        schema.addFunction(func);

        func = new PgFunction("select_something2");
        func.setLanguageCost("sql", null);
        func.setBody("$$SELECT number1 * number2 || 'text'$$");
        func.setReturns(INTEGER);

        arg = new Argument("number1", INTEGER);
        func.addArgument(arg);

        arg = new Argument("number2", INTEGER);
        func.addArgument(arg);

        schema.addFunction(func);

        func = new PgFunction("select_something3");
        func.setLanguageCost("sql", null);
        func.setBody("$$\nSELECT number1 * number2 || 'text'\n$$");
        func.setReturns(INTEGER);

        arg = new Argument("number1", INTEGER);
        func.addArgument(arg);

        arg = new Argument("number2", INTEGER);
        func.addArgument(arg);

        schema.addFunction(func);

        testDatabase("schema_5.sql", d);
    }

    @Test
    void testDB6() throws IOException, InterruptedException {
        DatabaseType dbType = DatabaseType.PG;
        AbstractDatabase d = createDumpDB(dbType);
        AbstractSchema schema = d.getDefaultSchema();

        schema.addPrivilege(new PgPrivilege("REVOKE", "ALL", "SCHEMA public", "PUBLIC", false));
        schema.addPrivilege(new PgPrivilege("REVOKE", "ALL", "SCHEMA public", POSTGRES, false));
        schema.addPrivilege(new PgPrivilege("GRANT", "ALL", "SCHEMA public", POSTGRES, false));
        schema.addPrivilege(new PgPrivilege("GRANT", "ALL", "SCHEMA public", "PUBLIC", false));

        AbstractTable table = new PgSimpleTable("test_table");
        schema.addTable(table);

        AbstractColumn col = new PgColumn("id");
        col.setType(BIGINT);
        table.addColumn(col);

        col = new PgColumn("date_deleted");
        col.setType("timestamp without time zone");
        table.addColumn(col);

        table.setOwner(POSTGRES);

        PgIndex idx = new PgIndex("test_table_deleted");
        idx.setMethod("btree");
        idx.addColumn(new SimpleColumn("date_deleted"));
        idx.setWhere("(date_deleted IS NULL)");
        table.addIndex(idx);

        testDatabase("schema_6.sql", d);
    }

    @Test
    void testDB7() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);

        AbstractSchema schema = new PgSchema("common");
        d.addSchema(schema);
        d.setDefaultSchema("common");

        PgFunction func = new PgFunction("t_common_casttotext");
        func.setLanguageCost("sql", null);
        func.setVolatileType("IMMUTABLE");
        func.setStrict(true);
        func.setBody("$_$SELECT textin(timetz_out($1));$_$");
        func.setReturns("text");

        Argument arg = new Argument(null, "time with time zone");
        func.addArgument(arg);

        schema.addFunction(func);

        func = new PgFunction("t_common_casttotext");
        func.setLanguageCost("sql", null);
        func.setVolatileType("IMMUTABLE");
        func.setStrict(true);
        func.setBody("$_$SELECT textin(time_out($1));$_$");
        func.setReturns("text");

        arg = new Argument(null, "time without time zone");
        func.addArgument(arg);

        schema.addFunction(func);

        func = new PgFunction("t_common_casttotext");
        func.setLanguageCost("sql", null);
        func.setVolatileType("IMMUTABLE");
        func.setStrict(true);
        func.setBody("$_$SELECT textin(timestamptz_out($1));$_$");
        func.setReturns("text");

        arg = new Argument(null, "timestamp with time zone");
        func.addArgument(arg);

        schema.addFunction(func);

        func = new PgFunction("t_common_casttotext");
        func.setLanguageCost("sql", null);
        func.setVolatileType("IMMUTABLE");
        func.setStrict(true);
        func.setBody("$_$SELECT textin(timestamp_out($1));$_$");
        func.setReturns("text");

        arg = new Argument(null, "timestamp without time zone");
        func.addArgument(arg);

        schema.addFunction(func);

        testDatabase("schema_7.sql", d);
    }

    @Test
    void testDB8() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        PgCompositeType type = new PgCompositeType("testtt");
        AbstractColumn col = new PgColumn("a");
        col.setType(INTEGER);
        type.addAttr(col);
        col = new PgColumn("b");
        col.setType("text");
        type.addAttr(col);
        type.setOwner("madej");
        schema.addType(type);

        schema = new PgSchema("``54'253-=9!@#$%^&*()__<>?:\"{}[];',./");
        d.addSchema(schema);

        PgFunction func = new PgFunction(".x\".\"\".");
        func.setLanguageCost("plpgsql", null);
        func.setBody("$_$\ndeclare\nbegin\nraise notice 'inside: %', $1;\nreturn true;\nend;\n$_$");
        func.setReturns(BOOLEAN);

        Argument arg = new Argument(null, INTEGER);
        func.addArgument(arg);

        schema.addFunction(func);
        func.setOwner("madej");

        testDatabase("schema_8.sql", d);
    }

    @Test
    void testDB9() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        PgSimpleTable table = new PgSimpleTable("user_data");
        schema.addTable(table);

        PgColumn col = new PgColumn("id");
        col.setType(BIGINT);
        createNotNullConstraint(table, col);
        col.setDefaultValue("nextval('public.user_id_seq'::regclass)");
        table.addColumn(col);

        col = new PgColumn("email");
        col.setType("character varying(128)");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("created");
        col.setType("timestamp with time zone");
        col.setDefaultValue("now()");
        table.addColumn(col);
        table.setOwner(POSTGRES);

        PgRule rule = new PgRule("on_select");
        rule.setEvent(EventType.SELECT);
        rule.setCondition("(1=1)");
        rule.setInstead(true);
        table.addRule(rule);

        PgSequence seq = new PgSequence("user_id_seq");
        seq.setMinMaxInc(1L, null, null, null, 0L);
        seq.setCache("1");
        seq.setOwnedBy(new GenericColumn("public", "user_data", "id", DbObjType.COLUMN));
        schema.addSequence(seq);
        seq.setOwner(POSTGRES);

        table = new PgSimpleTable("t1");
        schema.addTable(table);

        col = new PgColumn("c1");
        col.setType(INTEGER);
        table.addColumn(col);

        PgView view = new PgView("user");
        view.setQuery("( SELECT user_data.id, user_data.email, user_data.created FROM public.user_data)",
                "(SELECT user_data.id, user_data.email, user_data.created FROM public.user_data)");
        view.addColumnDefaultValue("created", "now()");
        schema.addView(view);

        view.setOwner(POSTGRES);

        rule = new PgRule("on_delete");
        rule.setEvent(EventType.DELETE);
        rule.addCommand("DELETE FROM public.user_data WHERE (user_data.id = old.id)");
        view.addRule(rule);

        rule = new PgRule("on_insert");
        rule.setEvent(EventType.INSERT);
        rule.setInstead(true);
        rule.addCommand("INSERT INTO public.user_data (id, email, created) VALUES (new.id, new.email, new.created)");
        rule.addCommand("INSERT INTO public.t1(c1) DEFAULT VALUES");
        view.addRule(rule);

        rule = new PgRule("on_update");
        rule.setEvent(EventType.UPDATE);
        rule.setInstead(true);
        rule.addCommand("UPDATE public.user_data SET id = new.id, email = new.email, created = new.created WHERE (user_data.id = old.id)");
        view.addRule(rule);

        view = new PgView("ws_test");
        view.setQuery("SELECT ud.id \"   i   d   \" FROM public.user_data ud",
                "SELECT ud.id \"   i   d   \" FROM public.user_data ud");
        schema.addView(view);

        testDatabase("schema_9.sql", d);
    }

    @Test
    void testDB10() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = new PgSchema("admin");
        d.addSchema(schema);
        d.setDefaultSchema("admin");

        schema.setOwner(POSTGRES);

        PgSimpleTable table = new PgSimpleTable("acl_role");
        schema.addTable(table);

        PgColumn col = new PgColumn("id");
        col.setType(BIGINT);
        createNotNullConstraint(table, col);
        table.addColumn(col);

        var constraintPK = new PgConstraintPk("acl_role_pkey", true);
        constraintPK.addColumn("id");
        table.addConstraint(constraintPK);

        table.setOwner(POSTGRES);

        table = new PgSimpleTable("user");
        schema.addTable(table);

        col = new PgColumn("id");
        col.setType(BIGINT);
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("email");
        col.setType("character varying(255)");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("name");
        col.setType("character varying(255)");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("password");
        col.setType(CHARACTER_VARYING_40);
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("is_active");
        col.setType(BOOLEAN);
        col.setDefaultValue("false");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("updated");
        col.setType("timestamp without time zone");
        col.setDefaultValue("now()");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("created");
        col.setType("timestamp without time zone");
        col.setDefaultValue("now()");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("role_id");
        col.setType(BIGINT);
        createNotNullConstraint(table, col);
        table.addColumn(col);

        col = new PgColumn("last_visit");
        col.setType("timestamp without time zone");
        col.setDefaultValue("now()");
        createNotNullConstraint(table, col);
        table.addColumn(col);

        PgIndex idx = new PgIndex("fki_user_role_id_fkey");
        idx.setMethod("btree");
        idx.addColumn(new SimpleColumn("role_id"));
        table.addIndex(idx);

        var constraintFk = new PgConstraintFk("user_role_id_fkey");
        constraintFk.addColumn("role_id");
        constraintFk.setForeignSchema("admin");
        constraintFk.setForeignTable("acl_role");
        constraintFk.addForeignColumn("id");
        table.addConstraint(constraintFk);

        table.setOwner(POSTGRES);

        testDatabase("schema_10.sql", d);
    }

    @Test
    void testDB11() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        PgFunction func = new PgFunction("curdate");
        func.setLanguageCost("sql", null);
        func.setBody("$$SELECT CAST('now' AS date);\n$$");
        func.setReturns("date");
        schema.addFunction(func);

        testDatabase("schema_11.sql", d);
    }

    @Test
    void testDB12() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);

        // d.setComment("'The status : ''E'' for enabled, ''D'' for disabled.'")

        testDatabase("schema_12.sql", d);
    }

    @Test
    void testDB13() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        PgFunction func = new PgFunction("drop_fk_except_for");
        func.setLanguageCost("plpgsql", null);
        func.setBody("$$\nDECLARE\nBEGIN\n  -- aaa\nEND;\n$$");
        func.setReturns("SETOF character varying");

        Argument arg = new Argument("tables_in", "character varying[]");
        func.addArgument(arg);

        schema.addFunction(func);

        testDatabase("schema_13.sql", d);
    }

    @Test
    void testDB14() throws IOException, InterruptedException {
        DatabaseType dbType = DatabaseType.PG;
        AbstractDatabase d = createDumpDB(dbType);
        AbstractSchema schema = d.getDefaultSchema();

        schema.addPrivilege(new PgPrivilege("REVOKE", "ALL", "SCHEMA public", "PUBLIC", false));
        schema.addPrivilege(new PgPrivilege("REVOKE", "ALL", "SCHEMA public", POSTGRES, false));
        schema.addPrivilege(new PgPrivilege("GRANT", "ALL", "SCHEMA public", POSTGRES, false));
        schema.addPrivilege(new PgPrivilege("GRANT", "ALL", "SCHEMA public", "PUBLIC", false));

        // d.setComment("'comments database'")
        // schema.setComment("'public schema'")

        PgFunction func = new PgFunction("test_fnc");
        func.setLanguageCost("plpgsql", null);
        func.setBody("$$BEGIN\nRETURN true;\nEND;$$");
        func.setReturns(BOOLEAN);

        Argument arg = new Argument("arg", "character varying");
        func.addArgument(arg);

        func.setOwner("fordfrog");

        func.setComment("'test function'");

        schema.addFunction(func);

        func = new PgFunction("trigger_fnc");
        func.setLanguageCost("plpgsql", null);
        func.setBody("$$begin\nend;$$");
        func.setReturns("trigger");
        schema.addFunction(func);

        func.setOwner("fordfrog");

        PgSimpleTable table = new PgSimpleTable("test");
        schema.addTable(table);

        PgColumn col = new PgColumn("id");
        col.setType(INTEGER);
        createNotNullConstraint(table, col);
        col.setComment("'id column'");
        col.setDefaultValue("nextval('public.test_id_seq'::regclass)");
        table.addColumn(col);

        col = new PgColumn("text");
        col.setType("character varying(20)");
        createNotNullConstraint(table, col);
        col.setComment("'text column'");
        table.addColumn(col);

        var constraintCheck = new PgConstraintCheck("text_check");
        constraintCheck.setExpression("(length((text)::text) > 0)");
        constraintCheck.setComment("'text check'");
        table.addConstraint(constraintCheck);

        table.setComment("'test table'");

        var constraintPk = new PgConstraintPk("test_pkey", true);
        constraintPk.addColumn("id");
        table.addConstraint(constraintPk);

        constraintPk.setComment("'primary key'");

        table.setOwner("fordfrog");

        PgSequence seq = new PgSequence("test_id_seq");
        seq.setStartWith("1");
        seq.setMinMaxInc(1L, null, null, null, 0L);
        seq.setCache("1");
        schema.addSequence(seq);

        seq.setOwnedBy(new GenericColumn("public", "test", "id", DbObjType.COLUMN));

        seq.setOwner("fordfrog");

        seq.setComment("'test table sequence'");

        PgView view = new PgView("test_view");
        view.setQuery("SELECT test.id, test.text FROM public.test",
                "SELECT test.id, test.text FROM public.test");
        schema.addView(view);

        view.setComment("'test view'");
        view.addColumnComment("id", "'view id col'");

        view.setOwner("fordfrog");

        PgTrigger trigger = new PgTrigger("test_trigger");
        trigger.setType(TgTypes.BEFORE);
        trigger.setOnUpdate(true);
        trigger.setForEachRow(false);
        trigger.setFunction("public.trigger_fnc()");
        table.addTrigger(trigger);

        trigger.setComment("'test trigger'");

        testDatabase("schema_14.sql", d);
    }

    @Test
    void testDB15() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        AbstractTable table = new PgSimpleTable("test");
        schema.addTable(table);

        AbstractColumn col = new PgColumn("id");
        col.setType(BIGINT);
        table.addColumn(col);

        table.setComment("'multiline\ncomment\n'");

        testDatabase("schema_15.sql", d);
    }

    @Test
    void testDB16() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        // table1
        AbstractTable table = new PgSimpleTable("t_work");
        schema.addTable(table);

        AbstractColumn col = new PgColumn("id");
        col.setType(INTEGER);
        table.addColumn(col);

        // table2
        AbstractTable table2 = new PgSimpleTable("t_chart");
        schema.addTable(table2);
        col = new PgColumn("id");
        col.setType(INTEGER);
        table2.addColumn(col);

        // view
        PgView view = new PgView("v_subselect");
        view.setQuery("SELECT c.id, t.id FROM ( SELECT t_work.id FROM public.t_work) t "
                        + "JOIN public.t_chart c ON t.id = c.id",
                "SELECT c.id, t.id FROM (SELECT t_work.id FROM public.t_work) t "
                        + "JOIN public.t_chart c ON t.id = c.id");
        schema.addView(view);

        testDatabase("schema_16.sql", d);
    }

    @Test
    void testDB17() throws IOException, InterruptedException {
        AbstractDatabase d = createDumpDB(DatabaseType.PG);
        AbstractSchema schema = d.getDefaultSchema();

        // table1
        AbstractTable table = new PgSimpleTable("t_work");
        schema.addTable(table);

        AbstractColumn col = new PgColumn("id");
        col.setType(INTEGER);
        table.addColumn(col);

        // table2
        AbstractTable table2 = new PgSimpleTable("t_chart");
        schema.addTable(table2);
        col = new PgColumn("id");
        col.setType(INTEGER);
        table2.addColumn(col);

        // table 3
        AbstractTable table3 = new PgSimpleTable("t_memo");
        schema.addTable(table3);
        col = new PgColumn("name");
        col.setType("text");
        table3.addColumn(col);

        // view
        PgView view = new PgView("v_subselect");
        view.setQuery("""
                        SELECT c.id, t.id AS second, t.name
                          FROM (( SELECT w.id, m.name
                            FROM (( SELECT t_work.id FROM public.t_work) w
                             JOIN public.t_memo m ON (((w.id)::text = m.name)))) t
                              JOIN public.t_chart c ON ((t.id = c.id)))""",

                """
                        SELECT c.id, t.id AS second, t.name \
                        FROM ((SELECT w.id, m.name FROM ((SELECT t_work.id FROM public.t_work) w \
                        JOIN public.t_memo m ON (((w.id) :: text = m.name)))) t \
                        JOIN public.t_chart c ON ((t.id = c.id)))""");
        schema.addView(view);

        testDatabase("schema_17.sql", d);
    }
}
