package ru.taximaxim.codekeeper.core.schema;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Map.Entry;

public interface PgForeignOptionContainer extends PgOptionContainer {

    static final String ALTER_FOREIGN_OPTION = "{0} OPTIONS ({1} {2} {3});";
    static final String DELIM = ",\n    ";

    String getAlterHeader();

    default void appendOptions(StringBuilder sb) {
        Map<String, String> options = getOptions();
        if (!options.isEmpty()) {
            sb.append("OPTIONS (\n    ");
            for (Entry<String, String> entry : options.entrySet()) {
                sb.append(entry.getKey())
                .append(' ')
                .append(entry.getValue())
                .append(DELIM);
            }
            sb.setLength(sb.length() - DELIM.length());
            sb.append("\n)");
        }
    }

    @Override
    default void compareOptions(PgOptionContainer newContainer, StringBuilder sb) {
        Map <String, String> oldForeignOptions = getOptions();
        Map <String, String> newForeignOptions = newContainer.getOptions();
        if (!oldForeignOptions.isEmpty() || !newForeignOptions.isEmpty()) {
            oldForeignOptions.forEach((key, value) -> {
                String newValue = newForeignOptions.get(key);
                if (newValue != null) {
                    if (!value.equals(newValue)) {
                        sb.append(MessageFormat.format(ALTER_FOREIGN_OPTION,
                                getAlterHeader(), "SET", key, newValue));
                    }
                } else {
                    sb.append(MessageFormat.format(ALTER_FOREIGN_OPTION,
                            getAlterHeader(), "DROP", key, ""));
                }
            });

            newForeignOptions.forEach((key, value) -> {
                if (!oldForeignOptions.containsKey(key)) {
                    sb.append(MessageFormat.format(ALTER_FOREIGN_OPTION,
                            getAlterHeader(), "ADD", key, value));
                }
            });
        }
    }
}
