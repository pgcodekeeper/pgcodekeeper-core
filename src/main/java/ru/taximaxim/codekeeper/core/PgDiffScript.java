package ru.taximaxim.codekeeper.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import ru.taximaxim.codekeeper.core.PgDiffStatement.DiffStatementType;
import ru.taximaxim.codekeeper.core.schema.PgStatement;

/**
 * Contains list of SQL statements.<br>
 * A set is used to ensure that {@link #addStatementUnique} does not add multiple entries.
 *
 * @author Alexander Levsha
 */
public class PgDiffScript {

    private final List<PgDiffStatement> statements = new ArrayList<>();

    // this is faster because HashSet.contains() is O(1)
    // List.contains() is O(n)
    // also String caches hashcodes, so that's a minor performance plus
    private final Set<PgDiffStatement> unique = new HashSet<>();

    public void addStatement(String statement) {
        PgDiffStatement st = new PgDiffStatement(DiffStatementType.OTHER, null, statement.trim());
        PgDiffStatement last = statements.isEmpty() ? null : statements.get(statements.size() - 1);
        if (statements.isEmpty() || !st.equals(last)){
            statements.add(st);
        }
    }

    public void addDrop(PgStatement obj, String comment, String statement) {
        addStatementUnique(DiffStatementType.DROP, obj, comment, statement, false);
    }

    public void addCreate(PgStatement obj, String comment, String statement,
            boolean replaceExisting) {
        addStatementUnique(DiffStatementType.CREATE, obj, comment, statement,
                replaceExisting);
    }

    /**
     * Adds statement only if it's not present in the statements list.
     */
    private void addStatementUnique(DiffStatementType type, PgStatement obj,
            String comment, String statement, boolean replaceExisting) {
        if (type != DiffStatementType.DROP && type != DiffStatementType.CREATE) {
            throw new IllegalArgumentException(
                    "Only DROPs and CREATEs can be tracked as unique statements!");
        }

        PgDiffStatement st = new PgDiffStatement(type, obj, statement);
        if (!unique.contains(st)) {
            if (comment != null){
                addStatement(comment);
            }
            statements.add(st);
            unique.add(st);
        } else if (replaceExisting) {
            statements.set(statements.indexOf(st), st);
        }
    }

    public String getText() {
        return statements.stream().map(st -> st.statement.trim())
                .collect(Collectors.joining("\n\n"));
    }
}
