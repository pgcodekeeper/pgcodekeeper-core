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
package org.pgcodekeeper.core.database.base.project;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.project.IWorkDirs;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.ISearchPath;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ISubElement;
import org.pgcodekeeper.core.utils.FileUtils;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

/**
 * Base implementation of {@link IWorkDirs}. Holds the directory mapping and
 * loads overrides from {@value #ALT_DIRS_FILENAME} when the project path is
 * provided. Subclasses supply defaults via {@link #getDefaultDirNames()} and
 * the initial split-by-schema flag via {@link #isSplitBySchemaByDefault()}.
 */
public abstract class AbstractWorkDirs implements IWorkDirs {

    public static final String ALT_DIRS_FILENAME = "structure.properties";
    public static final String IS_SPLIT_BY_SCHEMA = "is_split_by_schema";

    private final Map<String, DirRule> dirMapping;
    private final boolean isSplitBySchema;

    /**
     * Creates WorkDirs with the defaults from {@link #getDefaultDirNames()} and
     * applies overrides from the given properties file. The file may redefine
     * the directory name of any key present in {@link #getDefaultDirNames()}
     * (typically {@link DbObjType} names plus custom keys) and the reserved
     * {@value #IS_SPLIT_BY_SCHEMA} flag.
     * <p>
     * The file may have any name; project-loader callers conventionally pass
     * {@code <projectRoot>/}{@value #ALT_DIRS_FILENAME}, but external callers
     * may supply any path. If {@code altDirsFile} is {@code null} or does not
     * exist as a regular file, defaults are used unchanged.
     *
     * @param altDirsFile path to the alt-dirs properties file (any filename),
     *                    or {@code null} for defaults only
     */
    protected AbstractWorkDirs(Path altDirsFile) {
        dirMapping = getDefaultDirNames();

        boolean temp = isSplitBySchemaByDefault();
        if (altDirsFile != null && Files.isRegularFile(altDirsFile)) {
            var props = new Properties();
            try (Reader reader = Files.newBufferedReader(altDirsFile, StandardCharsets.UTF_8)) {
                props.load(reader);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            for (var entry : props.entrySet()) {
                var key = (String) entry.getKey();
                var value = (String) entry.getValue();
                if (IS_SPLIT_BY_SCHEMA.equals(key)) {
                    temp = Boolean.parseBoolean(value);
                    continue;
                }

                var rule = dirMapping.get(key);
                if (rule != null) {
                    rule.setDirName(value);
                }
            }
        }
        isSplitBySchema = temp;
    }

    /**
     * Resolves the alt-dirs file path inside the given project directory.
     */
    public static Path resolveAltDirsFile(Path projectPath) {
        return projectPath == null ? null : projectPath.resolve(ALT_DIRS_FILENAME);
    }

    @Override
    public boolean isSplitBySchema() {
        return isSplitBySchema;
    }

    @Override
    public Map<String, DirRule> getDirMapping() {
        return dirMapping;
    }

    @Override
    public Path getRelativeFilePath(IStatement st) {
        while (st instanceof ISubElement sub) {
            st = sub.getParent();
        }
        DirRule rule = resolveRule(st);
        if (rule == null) {
            throw new IllegalStateException("No directory rule for " + st.getStatementType());
        }

        String fileName = buildFileName(st);
        if (isSplitBySchema) {
            String containerDir = dirMapping.get(SCHEMA_KEY).getDirName();
            if (st.getStatementType() == DbObjType.SCHEMA) {
                String schemaName = FileUtils.getValidFilename(st.getBareName());
                return Path.of(containerDir, schemaName, fileName);
            }
            if (rule.isSubElement()) {
                String schemaName = FileUtils.getValidFilename(((ISearchPath) st).getSchemaName());
                return Path.of(containerDir, schemaName).resolve(rule.getDirName()).resolve(fileName);
            }
        }
        return Path.of(rule.getDirName()).resolve(fileName);
    }

    @Override
    public String getDirNameForType(DbObjType type) {
        for (DirRule rule : dirMapping.values()) {
            if (rule.getType() == type && !rule.isSpecific()) {
                return rule.getDirName();
            }
        }
        return null;
    }

    @Override
    public void saveAltDirs(Path projectPath) throws IOException {
        Path altDirsFile = projectPath.resolve(ALT_DIRS_FILENAME);
        if (isSplitBySchema == isSplitBySchemaByDefault() && dirMapping.equals(getDefaultDirNames())) {
            Files.deleteIfExists(altDirsFile);
        } else {
            var props = new Properties();
            props.setProperty(IS_SPLIT_BY_SCHEMA, Boolean.toString(isSplitBySchema));
            for (var pair : dirMapping.entrySet()) {
                props.setProperty(pair.getKey(), pair.getValue().getDirName());
            }

            try (var writer = Files.newBufferedWriter(altDirsFile, StandardCharsets.UTF_8)) {
                props.store(writer, null);
            }
        }
    }

    private DirRule resolveRule(IStatement st) {
        DirRule generic = null;
        for (DirRule rule : dirMapping.values()) {
            if (rule.getPredicate().test(st)) {
                if (rule.isSpecific()) {
                    return rule;
                }
                if (generic == null) {
                    generic = rule;
                }
            }
        }
        return generic;
    }

    private String buildFileName(IStatement st) {
        String fileName = FileUtils.getValidFilename(st.getBareName()) + Consts.SQL_POSTFIX;
        if (!isSplitBySchema && st instanceof ISearchPath sp) {
            fileName = FileUtils.getValidFilename(sp.getSchemaName()) + '.' + fileName;
        }
        return fileName;
    }

    protected abstract Map<String, DirRule> getDefaultDirNames();

    protected abstract boolean isSplitBySchemaByDefault();
}
