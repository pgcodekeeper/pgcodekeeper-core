/**
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 */
package cz.startnet.utils.pgdiff;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Set;
import java.util.stream.Collectors;

import org.kohsuke.args4j.CmdLineException;

import cz.startnet.utils.pgdiff.schema.PgDatabase;
import ru.taximaxim.codekeeper.apgdiff.UnixPrintWriter;
import ru.taximaxim.codekeeper.apgdiff.licensing.License;
import ru.taximaxim.codekeeper.apgdiff.licensing.LicenseException;
import ru.taximaxim.codekeeper.apgdiff.localizations.Messages;
import ru.taximaxim.codekeeper.apgdiff.model.exporter.ModelExporter;

/**
 * Compares two PostgreSQL dumps and outputs information about differences in
 * the database schemas.
 *
 * @author fordfrog
 */
public final class Main {

    /**
     * @return success value
     */
    public static boolean main(String[] args)
            throws IOException, InterruptedException, URISyntaxException {
        PrintWriter writer = new PrintWriter(System.out, true);
        PgDiffArguments arguments = new PgDiffArguments();
        try {
            if (!arguments.parse(writer, args)) {
                return true;
            }
            License l = new License(arguments.getLicensePath());
            l.verify(false);
            arguments.setLicense(l);
            if(arguments.isModeParse()) {
                parse(arguments);
                return true;
            } else {
                return diff(writer, arguments);
            }
        } catch (CmdLineException | LicenseException | NotAllowedObjectException ex) {
            System.err.println(ex.getLocalizedMessage());
            return false;
        }
    }

    private static boolean diff(PrintWriter writer, PgDiffArguments arguments)
            throws InterruptedException, IOException, LicenseException, URISyntaxException {
        PgDiffScript script;
        try (PrintWriter encodedWriter = getDiffWriter(arguments)) {
            script = PgDiff.createDiff(encodedWriter != null ? encodedWriter : writer, arguments);
        }
        if (arguments.isSafeMode()) {
            Set<DangerStatement> dangerTypes = script.findDangers(arguments.getAllowedDangers());
            if (!dangerTypes.isEmpty()) {
                String msg = MessageFormat.format(Messages.Main_danger_statements,
                        dangerTypes.stream().map(DangerStatement::name)
                        .collect(Collectors.joining(", ")));
                writer.println(msg);
                try (PrintWriter encodedWriter = getDiffWriter(arguments)) {
                    if (encodedWriter != null) {
                        encodedWriter.println("-- " + msg);
                    }
                }
                return false;
            }
        }
        return true;
    }

    private static PrintWriter getDiffWriter(PgDiffArguments arguments)
            throws FileNotFoundException, UnsupportedEncodingException {
        String outFile = arguments.getOutputTarget();
        return outFile == null ? null : new UnixPrintWriter(
                arguments.getOutputTarget(), arguments.getOutCharsetName());
    }

    private static void parse(PgDiffArguments arguments)
            throws IOException, InterruptedException, LicenseException, URISyntaxException {
        PgDatabase d = PgDiff.loadDatabaseSchema(
                arguments.getNewSrcFormat(), arguments.getNewSrc(), arguments);
        new ModelExporter(new File(arguments.getOutputTarget()),
                d, arguments.getOutCharsetName()).exportFull();
    }

    private Main() {
    }
}
