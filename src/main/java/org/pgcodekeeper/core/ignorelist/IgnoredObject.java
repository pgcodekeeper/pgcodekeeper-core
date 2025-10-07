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

import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.model.difftree.DbObjType;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Represents an ignore rule for database objects during schema comparison.
 * This class defines patterns and conditions to determine which database objects
 * should be ignored or shown in diff operations.
 */
public class IgnoredObject {

    /**
     * Defines the status of how an ignored object should be handled in the diff tree.
     */
    public enum AddStatus {
        ADD,
        ADD_SUBTREE,
        SKIP,
        SKIP_SUBTREE
    }

    private final String name;
    private final Pattern regex;
    private final String dbRegexStr;
    private final Pattern dbRegex;
    private Set<DbObjType> objTypes;
    private boolean isShow;
    private boolean isRegular;
    private boolean ignoreContent;
    private boolean isQualified;

    /**
     * Creates an ignored object rule.
     *
     * @param name          the name pattern to match against database objects
     * @param isRegular     true if the name should be treated as a regular expression
     * @param ignoreContent true if the content of matching objects should be ignored
     * @param isQualified   true if the name pattern should match against qualified names
     * @param objTypes      the set of database object types this rule applies to
     */
    public IgnoredObject(String name, boolean isRegular,
                         boolean ignoreContent, boolean isQualified, Set<DbObjType> objTypes) {
        this(name, null, false, isRegular, ignoreContent, isQualified, objTypes);
    }

    /**
     * Creates an ignored object rule with database name filtering.
     *
     * @param name          the name pattern to match against database objects
     * @param dbRegex       regular expression pattern to match database names
     * @param isShow        true if matching objects should be shown, false if hidden
     * @param isRegular     true if the name should be treated as a regular expression
     * @param ignoreContent true if the content of matching objects should be ignored
     * @param isQualified   true if the name pattern should match against qualified names
     * @param objTypes      the set of database object types this rule applies to
     */
    public IgnoredObject(String name, String dbRegex, boolean isShow, boolean isRegular,
                         boolean ignoreContent, boolean isQualified, Set<DbObjType> objTypes) {
        this.name = name;
        this.isShow = isShow;
        this.isRegular = isRegular;
        this.ignoreContent = ignoreContent;
        this.isQualified = isQualified;
        this.objTypes = objTypes;
        this.regex = isRegular ? Pattern.compile(name) : null;
        this.dbRegexStr = dbRegex;
        this.dbRegex = dbRegex == null ? null : Pattern.compile(dbRegex);
    }

    public String getName() {
        return name;
    }

    public boolean isShow() {
        return isShow;
    }

    public boolean isRegular() {
        return isRegular;
    }

    public boolean isIgnoreContent() {
        return ignoreContent;
    }

    public boolean isQualified() {
        return isQualified;
    }

    public Set<DbObjType> getObjTypes() {
        return objTypes;
    }

    public void setShow(boolean isShow) {
        this.isShow = isShow;
    }

    public void setRegular(boolean isRegular) {
        this.isRegular = isRegular;
    }

    public void setIgnoreContent(boolean ignoreContent) {
        this.ignoreContent = ignoreContent;
    }

    public void setQualified(boolean isQualified) {
        this.isQualified = isQualified;
    }

    public void setObjTypes(Set<DbObjType> objTypes) {
        this.objTypes = objTypes;
    }

    public Pattern getDbRegex() {
        return dbRegex;
    }

    /**
     * Creates a copy of this ignore rule with a different name pattern.
     *
     * @param name the new name pattern for the copied rule
     * @return a new IgnoredObject with the same properties but different name
     */
    public IgnoredObject copy(String name) {
        return new IgnoredObject(name, dbRegexStr, isShow, isRegular,
                ignoreContent, isQualified, EnumSet.copyOf(objTypes));
    }

    public boolean match(String objName) {
        if (isRegular) {
            return regex.matcher(objName).find();
        }

        return name.equals(objName);
    }

    boolean hasSameMatchingCondition(IgnoredObject rule) {
        return Objects.equals(name, rule.name)
                && Objects.equals(dbRegexStr, rule.dbRegexStr)
                && Objects.equals(objTypes, rule.objTypes);
    }

    /**
     * Gets the add status based on the show and ignore content flags.
     *
     * @return the appropriate AddStatus for this rule
     */
    public AddStatus getAddStatus() {
        if (isShow) {
            return ignoreContent ? AddStatus.ADD_SUBTREE : AddStatus.ADD;
        }
        return ignoreContent ? AddStatus.SKIP_SUBTREE : AddStatus.SKIP;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoreContent, isQualified, isRegular, isShow, name, dbRegexStr, objTypes);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof IgnoredObject other
                    && ignoreContent == other.ignoreContent
                    && isQualified == other.isQualified
                    && isRegular == other.isRegular
                    && isShow == other.isShow
                    && Objects.equals(name, other.name)
                    && Objects.equals(dbRegexStr, other.dbRegexStr)
                    && objTypes.equals(other.objTypes);
    }

    @Override
    public String toString() {
        return appendRuleCode(new StringBuilder(), true).toString();
    }

    /**
     * Appends the rule code representation to the given StringBuilder.
     *
     * @param sb        the StringBuilder to append to
     * @param isAddType true if this is an add-type rule
     * @return the StringBuilder with the rule code appended
     */
    public StringBuilder appendRuleCode(StringBuilder sb, boolean isAddType) {
        if (isAddType) {
            sb.append(isShow ? "SHOW " : "HIDE ");
        }
        if (ignoreContent || isRegular || isQualified) {
            if (ignoreContent) {
                sb.append("CONTENT").append(',');
            }
            if (isRegular) {
                sb.append("REGEX").append(',');
            }
            if (isQualified) {
                sb.append("QUALIFIED").append(',');
            }
            sb.setLength(sb.length() - 1);
            sb.append(" ");
        } else if (isAddType) {
            sb.append("NONE ");
        }
        sb.append(getValidId(name));

        if (dbRegex != null) {
            sb.append(" db=");
            sb.append(getValidId(dbRegexStr));
        }

        if (!objTypes.isEmpty()) {
            sb.append(" type=");
            sb.append(objTypes.stream().map(Enum::toString).map(IgnoredObject::getValidId)
                    .collect(Collectors.joining(",")));
        }

        return sb;
    }

    private static String getValidId(String id) {
        if (PgDiffUtils.isValidId(id, true, true) && !isKeyword(id)) {
            return id;
        }

        return quoteWithDq(id) ? PgDiffUtils.quoteName(id) : PgDiffUtils.quoteString(id);
    }

    private static boolean isKeyword(String id) {
        return switch (id) {
            case "QUALIFIED", "HIDE", "SHOW", "ALL", "CONTENT", "REGEX", "NONE" -> true;
            default -> false;
        };
    }

    private static boolean quoteWithDq(String str) {
        int dq = 0;
        int sq = 0;
        for (int i = 0; i < str.length(); ++i) {
            switch (str.charAt(i)) {
                case '\'':
                    ++sq;
                    break;
                case '"':
                    ++dq;
                    break;
                default:
                    break;
            }
        }
        return sq > dq;
    }
}
