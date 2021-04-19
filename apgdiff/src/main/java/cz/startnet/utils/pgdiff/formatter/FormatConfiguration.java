package cz.startnet.utils.pgdiff.formatter;

public class FormatConfiguration {

    public enum IndentType {
        DISABLE, TAB, WHITESPACE
    }

    private boolean addWhitespaceBeforeOp;
    private boolean addWhitespaceAfterOp;
    private boolean removeTrailingWhitespace;

    private IndentType indentType = IndentType.DISABLE;
    private int indentSize;
    private int spacesForTabs = -1;

    private String tabReplace;

    public void setAddWhitespaceBeforeOp(boolean addWhitespaceBeforeOp) {
        this.addWhitespaceBeforeOp = addWhitespaceBeforeOp;
    }

    public void setAddWhitespaceAfterOp(boolean addWhitespaceAfterOp) {
        this.addWhitespaceAfterOp = addWhitespaceAfterOp;
    }

    public void setRemoveTrailingWhitespace(boolean removeTrailingWhitespace) {
        this.removeTrailingWhitespace = removeTrailingWhitespace;
    }

    public void setIndentSize(int indentSize) {
        this.indentSize = indentSize;
    }

    public void setSpacesForTabs(int spacesForTabs) {
        this.spacesForTabs = spacesForTabs;
        tabReplace = spacesForTabs > 0 ? String.format("%1$" + spacesForTabs + 's', "") : "";
    }

    public boolean isAddWhitespaceAfterOp() {
        return addWhitespaceAfterOp;
    }

    public boolean isAddWhitespaceBeforeOp() {
        return addWhitespaceBeforeOp;
    }

    public boolean isRemoveTrailingWhitespace() {
        return removeTrailingWhitespace;
    }

    public int getIndentSize() {
        return indentSize;
    }

    public int getSpacesForTabs() {
        return spacesForTabs;
    }

    public String getTabReplace() {
        return tabReplace;
    }

    public IndentType getIndentType() {
        return indentType;
    }

    public void setIndentType(IndentType indentType) {
        this.indentType = indentType;
    }
}
