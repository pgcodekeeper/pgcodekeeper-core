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
package org.pgcodekeeper.core.ignorelist;

import org.antlr.v4.runtime.RuleContext;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrParser;
import org.pgcodekeeper.core.parsers.antlr.base.generated.IgnoreListParser;
import org.pgcodekeeper.core.parsers.antlr.base.generated.IgnoreListParser.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Parser for ignore list files that determine which database objects
 * should be included or excluded during processing. Supports both whitelist and
 * blacklist patterns with various matching flags.
 * <p>
 * Whitelist rules have precedence over blacklist rules when both are present.
 */
public final class IgnoreParser {

    private static final Logger LOG = LoggerFactory.getLogger(IgnoreParser.class);

    private final IIgnoreList list;

    /**
     * Creates a new IgnoreParser that will populate the specified ignore list.
     *
     * @param list the ignore list implementation to populate with parsed rules
     */
    public IgnoreParser(IIgnoreList list) {
        this.list = list;
    }

    /**
     * Parses an ignore list configuration file and adds the rules to the ignore list.
     *
     * @param listFile path to the ignore list configuration file
     * @return this parser instance for method chaining
     * @throws IOException if there's an error reading the configuration file
     */
    public IgnoreParser parse(Path listFile) throws IOException {
        String parsedObjectName = listFile.toString();
        var msg = Messages.IgnoreParser_log_load_ignored_list.formatted(parsedObjectName);
        LOG.info(msg);
        var parser = AntlrParser.createIgnoreListParser(listFile);

        try {
            parse(parser);
        } catch (Exception ex) {
            var errorMsg = Messages.IgnoreParser_log_ignor_list_analyzing_err.formatted(parsedObjectName);
            LOG.error(errorMsg, ex);
        }
        return this;
    }

    private void parse(IgnoreListParser parser) {
        LOG.info(Messages.IgnoreParser_log_ignor_list_parser_tree);
        Rule_listContext rules = parser.compileUnit().rule_list();
        WhiteContext white = rules.white();
        if (white != null) {
            // white lists are hiding by default and therefore have precedence over black lists
            list.setShow(false);
            white(white);
        } else {
            black(rules.black());
        }
    }

    private void white(WhiteContext white) {
        for (Show_ruleContext showRule : white.show_rule()) {
            ruleRest(showRule.rule_rest(), true);
        }
    }

    private void black(BlackContext black) {
        for (Hide_ruleContext hideRule : black.hide_rule()) {
            ruleRest(hideRule.rule_rest(), false);
        }
    }

    private void ruleRest(Rule_restContext ruleRest, boolean isShow) {
        boolean isRegular = false;
        boolean ignoreContent = false;
        boolean isQualified = false;
        for (FlagContext flag : ruleRest.flags().flag()) {
            if (flag.CONTENT() != null) {
                ignoreContent = true;
            } else if (flag.REGEX() != null) {
                isRegular = true;
            } else if (flag.QUALIFIED() != null) {
                isQualified = true;
            }
        }
        String dbRegex = ruleRest.db == null ? null : ruleRest.db.getText();

        Set<DbObjType> objTypes = ruleRest.type.stream()
                .map(RuleContext::getText)
                .map(DbObjType::valueOf)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(DbObjType.class)));

        list.add(new IgnoredObject(ruleRest.obj.getText(), dbRegex,
                isShow, isRegular, ignoreContent, isQualified, objTypes));
    }

    /**
     * Parses an ignore list configuration files and adds the rules to the ignore list.
     *
     * @param ignoreLists - collection of paths to files containing objects to ignore
     * @return ignore list with merged rules
     * @throws IOException if there's an error reading the configuration files
     */
    public static IgnoreList parseLists(Collection<String> ignoreLists) throws IOException {
        IgnoreList ignoreList = new IgnoreList();
        IgnoreParser ignoreParser = new IgnoreParser(ignoreList);
        if (ignoreLists != null) {
            for (String ignoreListPath : ignoreLists) {
                ignoreParser.parse(Paths.get(ignoreListPath));
            }
        }
        return ignoreList;
    }
}
