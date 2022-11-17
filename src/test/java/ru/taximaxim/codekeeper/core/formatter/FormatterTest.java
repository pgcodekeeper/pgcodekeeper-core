package ru.taximaxim.codekeeper.core.formatter;

import java.io.IOException;
import java.net.URISyntaxException;
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
import ru.taximaxim.codekeeper.core.fileutils.FileUtils;
import ru.taximaxim.codekeeper.core.formatter.FormatConfiguration.IndentType;

@RunWith(value = Parameterized.class)
public class FormatterTest {

    private static final Logger LOG = LoggerFactory.getLogger(PgDiffTest.class);

    @Parameters
    public static Iterable<FormatConfigProvider[]> parameters() {
        List<FormatConfigProvider[]> p = Arrays.asList(new FormatConfigProvider[][] {
            {new DefaultConfigProvider()},
            {new IndentTypeConfigProvider()},
            {new RemoveTrailingWhitespaceConfigProvider()},
            {new AddWhitespaceAfterOpConfigProvider()},
            {new AddWhitespaceBeforeOpConfigProvider()},
            {new AddSpacesForTabsConfigProvider()},
            {new IndentSizeConfigProvider()},
            {new IndentTypeTabConfigProvider()},
        });
        return p.stream()::iterator;
    }

    private final FormatConfigProvider args;

    public FormatterTest(FormatConfigProvider args) {
        this.args = args;
        LOG.debug(args.getClass().getSimpleName());
    }

    @Test
    public void testFormatterParams() throws IOException, URISyntaxException, FormatterException {
        FileFormatter fileform = new FileFormatter(args.getOldFile(), 0,
                args.getOldFile().length(), args.getConfig(), false);
        Assert.assertEquals(
                args.getClass().getSimpleName() + ": Formatted files are different. ",
                args.getNewFile(), fileform.formatText());
    }
}

interface FormatConfigProvider {
    String getOldFile() throws IOException, URISyntaxException;
    String getNewFile() throws IOException, URISyntaxException;

    void fillConfig(FormatConfiguration config);

    default FormatConfiguration getConfig() {
        FormatConfiguration config = new FormatConfiguration();
        fillConfig(config);
        return config;
    }

    default String getFileContent(String fileName) throws IOException, URISyntaxException {
        return FileUtils.readResource(FormatterTest.class, fileName);
    }
}

/**
Testing default parameteres
 */
class DefaultConfigProvider implements FormatConfigProvider {

    @Override
    public String getNewFile() throws IOException, URISyntaxException {
        return getFileContent("new_Default_config.sql");
    }

    @Override
    public String getOldFile() throws IOException, URISyntaxException {
        return getFileContent("old_Default_config.sql");
    }

    @Override
    public void fillConfig(FormatConfiguration config) {
        config.setAddWhitespaceAfterOp(true);
        config.setAddWhitespaceBeforeOp(true);
        config.setIndentSize(2);
        config.setIndentType(IndentType.WHITESPACE);
        config.setRemoveTrailingWhitespace(true);
    }
}

/**
Testing setIndentType(IndentType.Tab)
 */
class IndentTypeConfigProvider implements FormatConfigProvider {

    @Override
    public String getOldFile() throws IOException, URISyntaxException {
        return getFileContent("old.sql" );
    }

    @Override
    public String getNewFile() throws IOException, URISyntaxException {
        return getFileContent("new_indent_type.sql");
    }
    @Override
    public void fillConfig(FormatConfiguration config) {
        config.setIndentType(IndentType.TAB);
        config.setIndentSize(1);
    }
}

/**
 Testing RemoveTrailingWhitespace option
 */
class RemoveTrailingWhitespaceConfigProvider implements FormatConfigProvider {

    @Override
    public String getOldFile() throws IOException, URISyntaxException {
        return getFileContent("old_RemoveTrailingWhitespace.sql");
    }

    @Override
    public String getNewFile() throws IOException, URISyntaxException {
        return getFileContent("new_RemoveTrailingWhitespace.sql");
    }

    @Override
    public void fillConfig(FormatConfiguration config) {
        config.setRemoveTrailingWhitespace(true);
    }
}

/**
Testing AddWhitespaceAfterOp option
 */
class AddWhitespaceAfterOpConfigProvider implements FormatConfigProvider {

    @Override
    public String getOldFile() throws IOException, URISyntaxException {
        return getFileContent("old_AddWhitespaceOp.sql");
    }

    @Override
    public String getNewFile() throws IOException, URISyntaxException {
        return getFileContent("new_AddWhitespaceAfterOp.sql");
    }

    @Override
    public void fillConfig(FormatConfiguration config) {
        config.setAddWhitespaceAfterOp(true);
    }
}

/**
Testing WhitespaceBeforeOp option
 */
class AddWhitespaceBeforeOpConfigProvider implements FormatConfigProvider {

    @Override
    public String getOldFile() throws IOException, URISyntaxException {
        return getFileContent("old_AddWhitespaceOp.sql");
    }
    @Override
    public String getNewFile() throws IOException, URISyntaxException {
        return getFileContent("new_AddWhitespaceBeforeOp.sql");
    }

    @Override
    public void fillConfig(FormatConfiguration config) {
        config.setAddWhitespaceBeforeOp(true);
    }
}

/**
Testing SpacesForTabs option
 */
class AddSpacesForTabsConfigProvider implements FormatConfigProvider {

    @Override
    public String getOldFile() throws IOException, URISyntaxException {
        return getFileContent("old_SpacesForTabs.sql");
    }
    @Override
    public String getNewFile() throws IOException, URISyntaxException {
        return getFileContent("new_SpacesForTabs.sql");
    }

    @Override
    public void fillConfig(FormatConfiguration config) {
        config.setIndentType(IndentType.WHITESPACE);
        config.setIndentSize(8);
    }
}

/**
Testing IndentSize option
 */
class IndentSizeConfigProvider implements FormatConfigProvider {

    @Override
    public String getOldFile() throws IOException, URISyntaxException {
        return getFileContent("old.sql");
    }
    @Override
    public String getNewFile() throws IOException, URISyntaxException {
        return getFileContent("new_IndentSize.sql");
    }

    @Override
    public void fillConfig(FormatConfiguration config) {
        config.setIndentType(IndentType.WHITESPACE);
        config.setIndentSize(8);
    }
}

/**
Testing IndentTypeTab option
 */
class IndentTypeTabConfigProvider implements FormatConfigProvider {

    @Override
    public String getOldFile() throws IOException, URISyntaxException {
        return getFileContent("old.sql");
    }
    @Override
    public String getNewFile() throws IOException, URISyntaxException {
        return getFileContent("new_IndentTypeTab.sql");
    }

    @Override
    public void fillConfig(FormatConfiguration config) {
        config.setIndentType(IndentType.TAB);
        config.setIndentSize(2);
    }
}