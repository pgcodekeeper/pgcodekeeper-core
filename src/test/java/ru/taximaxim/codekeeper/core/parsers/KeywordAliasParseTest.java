package ru.taximaxim.codekeeper.core.parsers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.taximaxim.codekeeper.core.PgDiffTest;
import ru.taximaxim.codekeeper.core.parsers.antlr.AntlrParser;
import ru.taximaxim.codekeeper.core.parsers.antlr.SQLParser;
import ru.taximaxim.codekeeper.core.sql.Keyword;
import ru.taximaxim.codekeeper.core.sql.Keyword.LabelCategory;

@RunWith(value = Parameterized.class)
public class KeywordAliasParseTest {

    private static final Logger LOG = LoggerFactory.getLogger(PgDiffTest.class);

    @Parameters
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            {LabelCategory.BARE_LABEL},
            {LabelCategory.AS_LABEL},
        });
    }

    private final LabelCategory labelCategory;
    private final String select;

    public KeywordAliasParseTest(LabelCategory labelCategory) {
        LOG.debug(labelCategory.name());
        this.labelCategory = labelCategory;

        String as = labelCategory == LabelCategory.AS_LABEL ? "AS " : "";
        StringBuilder sb = new StringBuilder("SELECT ");
        Keyword.KEYWORDS.values().stream()
        .filter(k -> k.getLabelCategory() == labelCategory)
        .forEach(k -> sb.append("\nCol ").append(as).append(k.getKeyword()).append(','));

        sb.setLength(sb.length() - 1);
        sb.append(';');
        select = sb.toString();
    }

    @Test
    public void testAliases() {
        List<Object> errors = new ArrayList<>();
        SQLParser p = AntlrParser.makeBasicParser(SQLParser.class, select, labelCategory.name(), errors);
        p.sql();
        Assert.assertTrue("KeywordAliasParseTest: " + labelCategory + " - ANTLR Error", errors.isEmpty());
    }
}
