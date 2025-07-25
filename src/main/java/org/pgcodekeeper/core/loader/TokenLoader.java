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
package org.pgcodekeeper.core.loader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.eclipse.core.runtime.NullProgressMonitor;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.parsers.antlr.AntlrParser;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.SqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.verification.VerificationParserListener;
import org.pgcodekeeper.core.parsers.antlr.verification.VerificationProperties;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TokenLoader extends DatabaseLoader {

    private final ISettings settings;
    private final VerificationProperties rules;
    private static final Logger LOG = LoggerFactory.getLogger(TokenLoader.class);

    private TokenLoader(ISettings settings, VerificationProperties rules) {
        this.settings = settings;
        this.rules = rules;
    }

    @Override
    public PgDatabase load() throws IOException, InterruptedException {
        throw new IllegalStateException("Unsupported operation");
    }

    public static List<Object> verify(ISettings settings, Path path, Collection<String> sources)
            throws InterruptedException, IOException {
        return verify(settings, VerificationProperties.readProperties(path), sources);
    }

    public static List<Object> verify(ISettings args, VerificationProperties rules, Collection<String> sources)
            throws InterruptedException, IOException {
        TokenLoader loader = new TokenLoader(args, rules);

        for (String srcPath : sources) {
            Path path = Paths.get(srcPath);
            if (Files.isDirectory(path)) {
                verifyDirectory(loader, path);
            } else {
                verifyFile(loader, path);
            }
        }

        return loader.getErrors();
    }

    private static void verifyDirectory(TokenLoader loader, Path f) throws IOException, InterruptedException {
        try (Stream<Path> stream = Files.list(f)) {
            for (Path sub : (Iterable<Path>) stream::iterator) {
                if (Files.isDirectory(sub)) {
                    verifyDirectory(loader, sub);
                } else {
                    verifyFile(loader, sub);
                }
            }
        }
    }

    private static void verifyFile(TokenLoader loader, Path f) throws IOException, InterruptedException {
        try {
            loader.processVerification(f);
        } catch (NoSuchFileException e) {
            LOG.info(Messages.TokenLoader_log_file_not_exist);
        }
    }

    private void processVerification(Path inputFile) throws InterruptedException, IOException {
        String fileName = inputFile.getFileName().toString();
        var dbType = settings.getDbType();
        SqlContextProcessor listener = switch (dbType) {
            case PG -> new VerificationParserListener(rules, fileName, errors);
            // TODO add other later
            default -> throw new IllegalArgumentException(Messages.DatabaseType_unsupported_type + dbType);
        };

        InputStreamProvider input = () -> Files.newInputStream(inputFile);
        AntlrParser.parseSqlStream(input, settings.getInCharsetName(), inputFile.toString(), errors,
                new NullProgressMonitor(), 0, listener, antlrTasks);
        finishLoaders();
    }
}
