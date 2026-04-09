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
package org.pgcodekeeper.core.localizations;

import java.lang.reflect.Field;
import java.util.ResourceBundle;

/**
 * Internationalization message constants for pgCodeKeeper core components.
 * <p>
 * This class provides externalized string constants for user-facing messages,
 * error messages, log entries, and other text content throughout the pgCodeKeeper
 * core library.
 */
public final class Messages {

    public static final String BUNDLE_NAME = "org.pgcodekeeper.core.localizations.messages"; //$NON-NLS-1$

    public static String Version;

    public static String AbstractJdbcConnector_url_validation_failed;

    public static String AbstractLibraryLoader_error_while_read_library;

    public static String AbstractAnalysisLauncher_error_prefix;

    public static String AbstractExprWithNmspc_log_ambiguos_ref;

    public static String AbstractExprWithNmspc_log_cte_contains_cols;

    public static String AbstractExprWithNmspc_log_dupl_col_alias;

    public static String AbstractExprWithNmspc_log_dupl_cte;

    public static String AbstractExprWithNmspc_log_dupl_unaliased_table;

    public static String AbstractExprWithNmspc_log_not_alternative;

    public static String AbstractPgTable_log_inherits_not_found;

    public static String AbstractPgTable_log_schemas_not_found;

    public static String AbstractProjectLoader_failed_to_read_ignore_lists;

    public static String AbstractSearchPathJdbcReader_no_schema_found;

    public static String AbstractStatementReader_start;

    public static String DependenciesReader_parser_error;

    public static String AbstractStatement_already_has_a_parent;

    public static String AbstractStatement_null_statement;

    // common
    public static String Utils_unsupported_sequence_type;

    public static String Utils_not_object_in_database;

    public static String Utils_failed_to_load_databases;

    public static String Utils_loading_databases;

    public static String Utils_loading_new_database;

    public static String Utils_loading_old_database;

    public static String Utils_log_err_deserialize;

    public static String Utils_log_err_serialize;

    // pgdiff.loader
    public static String CompareTree_missing_compare;

    public static String Connection_DatabaseJdbcAccessError;

    public static String Constraint_WarningMismatchedConstraintTypeForClusterOn;

    public static String CustomAntlrErrorListener_error;

    public static String CustomParserListener_statement_context_is_missing;

    public static String ProjectUpdater_error_backup_restore;

    public static String ProjectUpdater_error_no_tempdir;

    public static String ProjectUpdater_error_update;

    public static String ProjectUpdater_log_restoring_err;

    public static String ProjectUpdater_log_start_full_update;

    public static String ProjectUpdater_log_start_partial_update;

    public static String ProjectUpdater_log_update_err_restore_proj;

    public static String ProjectUpdater_old_db_null;

    public static String MsXmlReader_not_root_element;

    public static String ObjectCreationException_with_parent;

    public static String ObjectCreationException_without_parent;

    public static String ParserAbstract_location_error;

    public static String ParserAbstract_schema_error;

    public static String PgCastsReader_unknown_cast_method;

    public static String PgCodeKeeperApi_building_script;

    public static String PgCodeKeeperApi_checking_dangerous_statements;

    public static String PgCodeKeeperApi_creating_tree;

    public static String PgCodeKeeperApi_executing_script;

    public static String PgCodeKeeperApi_exporting_project;

    public static String PgCodeKeeperApi_parsing_script;

    public static String PgColumn_no_such_object_of_inheritance;

    public static String PgCommentOn_table_name_is_missing;

    public static String PgConstraintsReader_unsupported_constraint_type;

    public static String PgCustomParserListener_unsupported_search_path;

    public static String PgDiffUtils_error_constructing_object_name;

    public static String PgDiff_read_error;

    public static String PgFunctionsReader_doesnt_support_by_aggregate;

    public static String PgJdbcPrivilege_no_enum_constant;

    public static String PgJdbcSystemLoader_unknown_cast_context;

    public static String PgSelect_without_the_asterisk;

    public static String PgSequencesReader_no_select_privileges_for_sequence;

    public static String PgSequencesReader_no_usage_privileges_for_schema;

    public static String PgSequencesReader_sequences_data_query;

    public static String PgTableAbstract_number_columns_not_match;

    public static String PgTableAbstract_unsupported_constraint_type;

    public static String DbObjType_unsupported_type;

    public static String JdbcLoaderBase_unsupported_ms_sql_version;

    public static String DepcyGraph_log_col_is_missed;

    public static String DepcyGraph_log_no_such_table;

    public static String DepcyGraph_log_remove_deps;

    public static String DiffTree_both_diff_sides_are_null;

    public static String FileUtils_creating_temp_directory;

    public static String FileUtils_creating_temp_file;

    public static String FileUtils_error_while_read_uri_lib;

    public static String Function_log_variable_not_found;

    public static String IPgJdbcReader_ConcurrentModificationException;

    public static String IgnoreParser_log_ignor_list_analyzing_err;

    public static String IgnoreParser_log_ignor_list_parser_tree;

    public static String IgnoreParser_log_load_ignored_list;

    public static String IgnoreSchemaList_log_ignored_schema;

    public static String JdbcChLoader_log_connection_db;

    public static String JdbcLoader_log_read_db_objects;

    public static String JdbcLoader_log_reading_db_jdbc;

    public static String JdbcLoader_log_succes_queried;

    public static String JdbcReader_column_null_value_error_message;

    public static String JdbcRunner_script_execution;

    public static String JdbcLoaderBase_log_check_extension;

    public static String JdbcLoaderBase_log_check_gp_db;

    public static String JdbcLoaderBase_log_current_obj;

    public static String JdbcLoaderBase_log_event_trigger_disabled;

    public static String JdbcLoaderBase_log_get_last_oid;

    public static String JdbcLoaderBase_log_get_last_system_obj_oid;

    public static String JdbcLoaderBase_log_get_list_system_types;

    public static String JdbcLoaderBase_log_get_result_gp;

    public static String JdbcLoaderBase_log_get_roles;

    public static String JdbcLoaderBase_log_load_version;

    public static String JdbcLoaderBase_log_not_support_privil;

    public static String JdbcLoaderBase_log_old_version_used;

    public static String JdbcLoaderBase_log_reading_ms_version;

    public static String JdbcLoaderBase_log_reading_pg_version;

    public static String JdbcLoaderBase_unsupported_pg_version;

    public static String JdbcLoaderBase_unsupported_gp_version;

    public static String ModelExporter_log_create_dirs;

    public static String ModelExporter_log_create_dir_err_contains_dir;

    public static String ModelExporter_log_create_dir_err_no_dir;

    public static String ModelExporter_log_delete_file;

    public static String ModelExporter_log_old_database_not_null;

    public static String ModelExporter_log_output_dir_no_exist_err;

    public static String Table_TypeParameterChange;

    public static String TreeElement_already_has_a_parent;

    public static String TreeElement_no_statement_found;

    public static String TreeFlattener_log_filter_obj;

    public static String TreeFlattener_log_ignore_children;

    public static String TreeFlattener_log_ignore_obj;

    public static String QueriesBatchCallable_executing_batch;

    public static String QueriesBatchCallable_script_finished;

    public static String QueriesBatchCallable_starting_batch;

    public static String ScriptParser_errors_while_parse_script;

    public static String ScriptParser_log_load_dump;

    public static String Select_log_aster_qual_not_found;

    public static String Select_log_not_alter_item;

    public static String Select_log_not_alter_prim;

    public static String Select_log_not_alter_right_part;

    public static String Select_log_not_alter_select;

    public static String Select_log_not_alter_selectops;

    public static String SequencesReader_log_not_found_table;

    public static String SimpleDepcyResolver_new_database_not_defined;

    public static String Statement_Unhandled_DbObjType;

    public static String Statement_unsupported_child_type;

    public static String Storage_WarningUnableToDetermineStorageType;

    public static String ActionsToScriptConverter_not_allowed_object;

    public static String ActionsToScriptConverter_not_implemented_action;

    public static String AlterTriggerError;

    public static String XmlStore_read_error;

    public static String XmlStore_root_error;

    public static String XmlStore_write_error;

    public static String AbstractExpr_duplicate_aliases;

    public static String AbstractExpr_line;

    public static String AbstractExpr_log_column_not_found_in_complex;

    public static String AbstractExpr_log_complex_not_found;

    public static String AbstractExpr_log_unknown_column_ref;

    public static String AbstractExpr_log_column_not_found_in_relation;

    public static String AbstractExpr_log_relation_not_found;

    public static String AbstractExpr_log_tableless_column_not_resolved;

    public static String ValueExpr_log_no_vex_alternative;

    public static String ValueExpr_log_no_primary_alternative;

    public static String ValueExpr_log_subselect_empty;

    public static String ValueExpr_log_long_indirection;

    public static String ValueExpr_log_duplicate_named_arg;

    public static String ValueExpr_log_no_function_special_alternative;

    public static String ValueExpr_log_no_datetime_alternative;

    public static String ValueExpr_log_no_literal_alternative;

    public static String MsAlterBatch_UnsupportedOperationException;

    public static String MsParserAbstract_unsupported_generated_always;

    public static String MsSelect_log_not_alter_item;

    public static String ChCreateTable_unsupported_Table_element_exprContext;

    static {
        ResourceBundle bundle = ResourceBundle.getBundle(BUNDLE_NAME);
        for (String key : bundle.keySet()) {
            try {
                Field field = Messages.class.getField(key);
                if (field.getType().equals(String.class)) {
                    field.set(null, bundle.getString(key));
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                // ignore
            }
        }
    }

    private Messages() {
    }
}
