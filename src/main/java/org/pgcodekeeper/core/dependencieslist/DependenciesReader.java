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
package org.pgcodekeeper.core.dependencieslist;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.parser.ParserUtils;
import org.pgcodekeeper.core.database.base.parser.QNameParser;
import org.pgcodekeeper.core.database.base.parser.generated.DependenciesListParser;
import org.pgcodekeeper.core.database.base.parser.generated.DependenciesListParser.DefinitionContext;
import org.pgcodekeeper.core.database.base.parser.generated.DependenciesListParser.IdentifierContext;
import org.pgcodekeeper.core.localizations.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads and parses dependency definitions from a file.
 */
public final class DependenciesReader {

    private static final Logger LOG = LoggerFactory.getLogger(DependenciesReader.class);

    private DependenciesReader() {}

    public static List<Dependency> getDependencies(Path depsPath) {
        try {
            var parser = ParserUtils.createDependenciesListParser(depsPath);
            return new DependenciesReader().getDependencies(parser);
        } catch (Exception ex) {
            LOG.error(Messages.DependenciesReader_parser_error.formatted(depsPath), ex);
        }
        return new ArrayList<>();
    }

    private List<Dependency> getDependencies(DependenciesListParser parser) {
        List<Dependency> depsies = new ArrayList<>();
        var depsDefinitions = parser.compileUnit().deps_definition();
        for (var depsDefinition : depsDefinitions) {
            var source = getObjReference(depsDefinition.source);
            var target = getObjReference(depsDefinition.target);
            depsies.add(new Dependency(source, target));
        }
        return depsies;
    }

    /**
     * Creates an {@link ObjectReference} from a definition context.
     * <p>
     * This method extracts schema name, object name, and object type from the provided
     * definition context. The object name may include function arguments if applicable.
     * For objects without an explicit schema, the object reference is created with a null schema.
     * </p>
     *
     * @param definition the definition context containing object identification information
     * @return an object reference containing schema, name, and type of the database object
     */
    private ObjectReference getObjReference(DefinitionContext definition) {
        var objNameCtx = definition.schema_qualified_name();
        var ids = objNameCtx.identifier();

        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);
        ParserRuleContext objCtx = QNameParser.getFirstNameCtx(ids);

        // get one name
        if (schemaCtx == null) {
            return new ObjectReference(objCtx.getText(), getObjectType(definition));
        }

        var objName = objCtx.getText();
        if (ids.size() > 2) {
            // get table name
            ParserRuleContext tableNameCtx = QNameParser.getSecondNameCtx(ids);
            return new ObjectReference(schemaCtx.getText(), tableNameCtx.getText(), objName,
                    getObjectType(definition));
        }

        // get function
        var funcArgs = getFunctionWithArgsText(definition);
        if (funcArgs != null) {
            objName = objName + funcArgs;
        }

        return new ObjectReference(schemaCtx.getText(), objName, getObjectType(definition));
    }

    /**
     * Extracts the database object type from a definition context.
     *
     * @param definition the definition context containing object type information
     * @return the corresponding {@link DbObjType} enumeration value
     * @throws IllegalArgumentException if the extracted type string doesn't match any DbObjType
     */
    private DbObjType getObjectType(DefinitionContext definition) {
        List<IdentifierContext> identifiers = definition.object_type().identifier();

        String objectType = identifiers.stream()
            .map(IdentifierContext::getText)
            .collect(Collectors.joining("_"))
            .toUpperCase(Locale.ROOT);

        try {
            return DbObjType.valueOf(objectType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Messages.DbObjType_unsupported_type + objectType);
        }
    }

    private static String getFunctionWithArgsText(DefinitionContext def) {
        var args = def.function_args();
        if (args == null) {
            return null;
        }
        Token start = args.getStart();
        Token stop = args.getStop();

        if (start != null && stop != null) {
            CharStream input = start.getInputStream();
            return input.getText(Interval.of(start.getStartIndex(), stop.getStopIndex()));
        }

        return args.getText();
    }
}
