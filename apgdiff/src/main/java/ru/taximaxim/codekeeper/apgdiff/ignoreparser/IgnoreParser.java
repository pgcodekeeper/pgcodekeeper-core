package ru.taximaxim.codekeeper.apgdiff.ignoreparser;

import java.io.IOException;
import java.io.InputStream;

import cz.startnet.utils.pgdiff.parsers.antlr.AntlrParser;
import cz.startnet.utils.pgdiff.parsers.antlr.IgnoreListParser;
import cz.startnet.utils.pgdiff.parsers.antlr.IgnoreListParser.BlackContext;
import cz.startnet.utils.pgdiff.parsers.antlr.IgnoreListParser.FlagContext;
import cz.startnet.utils.pgdiff.parsers.antlr.IgnoreListParser.Hide_ruleContext;
import cz.startnet.utils.pgdiff.parsers.antlr.IgnoreListParser.Rule_listContext;
import cz.startnet.utils.pgdiff.parsers.antlr.IgnoreListParser.Rule_restContext;
import cz.startnet.utils.pgdiff.parsers.antlr.IgnoreListParser.Show_ruleContext;
import cz.startnet.utils.pgdiff.parsers.antlr.IgnoreListParser.WhiteContext;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.IgnoreList;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.IgnoredObject;

public class IgnoreParser {

    private final IgnoreList list;

    public IgnoreList getIgnoreList() {
        return list;
    }

    public IgnoreParser() {
        this(new IgnoreList());
    }

    public IgnoreParser(IgnoreList list) {
        this.list = list;
    }

    public IgnoreParser parse(InputStream stream, String parsedObjectName) throws IOException {
        parse(AntlrParser.makeBasicParser(IgnoreListParser.class, stream, "UTF-8", parsedObjectName));
        return this;
    }

    private void parse(IgnoreListParser parser) {
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
            rule_rest(showRule.rule_rest(), true);
        }
    }

    private void black(BlackContext black) {
        for (Hide_ruleContext hideRule : black.hide_rule()) {
            rule_rest(hideRule.rule_rest(), false);
        }
    }

    private void rule_rest(Rule_restContext ruleRest, boolean isShow) {
        boolean isRegular = false, ignoreContent = false;
        for (FlagContext flag : ruleRest.flags().flag()) {
            if (flag.CONTENT() != null) {
                ignoreContent = true;
            } else if (flag.REGEX() != null) {
                isRegular = true;
            }
        }
        list.add(new IgnoredObject(ruleRest.obj.getText(), isShow, isRegular, ignoreContent));
    }
}
