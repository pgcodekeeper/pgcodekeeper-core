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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.database.pg.schema;

import java.util.*;
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * PostgreSQL schema implementation.
 * Schemas are namespaces that contain database objects like tables, functions, types, and operators.
 * Each schema can have its own set of permissions and provides object organization.
 */
public class PgSchema extends PgAbstractStatement implements ISchema {

    private final Map<String, PgAbstractFunction> functions = new LinkedHashMap<>();
    private final Map<String, PgSequence> sequences = new LinkedHashMap<>();
    private final Map<String, PgAbstractTable> tables = new LinkedHashMap<>();
    private final Map<String, PgAbstractView> views = new LinkedHashMap<>();
    private final Map<String, PgAbstractType> types = new LinkedHashMap<>();
    private final Map<String, PgDomain> domains = new LinkedHashMap<>();
    private final Map<String, PgFtsParser> parsers = new LinkedHashMap<>();
    private final Map<String, PgFtsTemplate> templates = new LinkedHashMap<>();
    private final Map<String, PgFtsDictionary> dictionaries = new LinkedHashMap<>();
    private final Map<String, PgFtsConfiguration> configurations = new LinkedHashMap<>();
    private final Map<String, PgOperator> operators = new LinkedHashMap<>();
    private final Map<String, PgCollation> collations = new LinkedHashMap<>();
    private final Map<String, PgStatistics> statistics = new LinkedHashMap<>();

    /**
     * Creates a new PostgreSQL schema.
     *
     * @param name schema name
     */
    public PgSchema(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE SCHEMA ");
        appendIfNotExists(sbSQL, script.getSettings());
        sbSQL.append(getQualifiedName());
        script.addStatement(sbSQL);

        appendOwnerSQL(script);
        appendPrivileges(script);
        appendComments(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        var newSchema = (PgSchema) newCondition;
        appendAlterOwner(newSchema, script);
        alterPrivileges(newSchema, script);
        appendAlterComments(newSchema, script);

        return getObjectState(script, startSize);
    }

    public PgDomain getDomain(String name) {
        return getChildByName(domains, name);
    }

    public PgCollation getCollation(final String name) {
        return getChildByName(collations, name);
    }

    public PgFtsParser getFtsParser(final String name) {
        return getChildByName(parsers, name);
    }

    public PgFtsTemplate getFtsTemplate(final String name) {
        return getChildByName(templates, name);
    }

    public PgFtsDictionary getFtsDictionary(final String name) {
        return getChildByName(dictionaries, name);
    }

    public PgFtsConfiguration getFtsConfiguration(final String name) {
        return getChildByName(configurations, name);
    }

    /**
     * Gets an operator by its signature.
     *
     * @param signature operator signature including arguments
     * @return operator or null if not found
     */
    public PgOperator getOperator(final String signature) {
        return getChildByName(operators, signature);
    }

    public PgStatistics getStatistics(final String name) {
        return getChildByName(statistics, name);
    }

    /**
     * Gets all operators in this schema.
     *
     * @return unmodifiable collection of operators
     */
    public Collection<IOperator> getOperators() {
        return Collections.unmodifiableCollection(operators.values());
    }

    private void addCollation(final PgCollation collation) {
        addUnique(collations, collation);
    }

    private void addDomain(PgDomain dom) {
        addUnique(domains, dom);
    }

    private void addFtsParser(final PgFtsParser parser) {
        addUnique(parsers, parser);
    }

    private void addFtsTemplate(final PgFtsTemplate template) {
        addUnique(templates, template);
    }

    private void addFtsDictionary(final PgFtsDictionary dictionary) {
        addUnique(dictionaries, dictionary);
    }

    private void addFtsConfiguration(final PgFtsConfiguration configuration) {
        addUnique(configurations, configuration);
    }

    private void addOperator(final PgOperator oper) {
        addUnique(operators, oper);
    }

    private void addStatistics(final PgStatistics rule) {
        addUnique(statistics, rule);
    }

    /**
     * Adds a type to this schema.
     *
     * @param type the table to add
     */
    private void addType(final PgAbstractType type) {
        // replace shell type by real type
        PgAbstractType oldType = getType(type.getName());
        if (oldType instanceof PgShellType && !(type instanceof PgShellType)) {
            types.remove(type.getName());
            oldType.setParent(null);
        }
        addUnique(types, type);
    }

    /**
     * Finds type according to specified type {@code name}.
     *
     * @param name name of the type to be searched
     * @return found type or null if no such type has been found
     */
    public PgAbstractType getType(final String name) {
        return getChildByName(types, name);
    }

    @Override
    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        l.add(functions.values());
        l.add(sequences.values());
        l.add(tables.values());
        l.add(views.values());
        l.add(types.values());
        l.add(domains.values());
        l.add(parsers.values());
        l.add(templates.values());
        l.add(dictionaries.values());
        l.add(configurations.values());
        l.add(operators.values());
        l.add(collations.values());
        l.add(statistics.values());
    }

    @Override
    public void fillDescendantsList(List<Collection<? extends AbstractStatement>> l) {
        fillChildrenList(l);
        for (var table : tables.values()) {
            table.fillDescendantsList(l);
        }
        for (var view : views.values()) {
            view.fillDescendantsList(l);
        }
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case DOMAIN -> getDomain(name);
            case FTS_PARSER -> getFtsParser(name);
            case FTS_TEMPLATE -> getFtsTemplate(name);
            case FTS_DICTIONARY -> getFtsDictionary(name);
            case FTS_CONFIGURATION -> getFtsConfiguration(name);
            case OPERATOR -> getOperator(name);
            case COLLATION -> getCollation(name);
            case STATISTICS -> getStatistics(name);
            case FUNCTION, PROCEDURE, AGGREGATE -> {
                PgAbstractFunction func = getFunction(name);
                yield func != null && func.getStatementType() == type ? func : null;
            }
            case SEQUENCE -> getSequence(name);
            case TYPE -> getType(name);
            case TABLE -> getTable(name);
            case VIEW -> getView(name);
            default -> null;
        };
    }

    @Override
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        return switch (type) {
            case DOMAIN -> Collections.unmodifiableCollection(domains.values());
            case FTS_PARSER -> Collections.unmodifiableCollection(parsers.values());
            case FTS_TEMPLATE -> Collections.unmodifiableCollection(templates.values());
            case FTS_DICTIONARY -> Collections.unmodifiableCollection(dictionaries.values());
            case FTS_CONFIGURATION -> Collections.unmodifiableCollection(configurations.values());
            case OPERATOR -> Collections.unmodifiableCollection(operators.values());
            case COLLATION -> Collections.unmodifiableCollection(collations.values());
            case STATISTICS -> Collections.unmodifiableCollection(statistics.values());
            case FUNCTION, PROCEDURE, AGGREGATE -> Collections.unmodifiableCollection(functions.values());
            case SEQUENCE -> Collections.unmodifiableCollection(sequences.values());
            case TYPE -> Collections.unmodifiableCollection(types.values());
            case TABLE -> Collections.unmodifiableCollection(tables.values());
            case VIEW -> Collections.unmodifiableCollection(views.values());
            default -> List.of();
        };
    }

    /**
     * Finds function according to specified function {@code signature}.
     *
     * @param signature signature of the function to be searched
     * @return found function or null if no such function has been found
     */
    public PgAbstractFunction getFunction(final String signature) {
        return getChildByName(functions, signature);
    }

    /**
     * Finds sequence according to specified sequence {@code name}.
     *
     * @param name name of the sequence to be searched
     * @return found sequence or null if no such sequence has been found
     */
    public PgSequence getSequence(final String name) {
        return getChildByName(sequences, name);
    }

    /**
     * Finds table according to specified table {@code name}.
     *
     * @param name name of the table to be searched
     * @return found table or null if no such table has been found
     */
    public PgAbstractTable getTable(final String name) {
        return getChildByName(tables, name);
    }

    public Collection<PgAbstractTable> getTables() {
        return Collections.unmodifiableCollection(tables.values());
    }

    /**
     * Finds view according to specified view {@code name}.
     *
     * @param name name of the view to be searched
     * @return found view or null if no such view has been found
     */
    public PgAbstractView getView(final String name) {
        return getChildByName(views, name);
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case DOMAIN:
                addDomain((PgDomain) st);
                break;
            case FTS_CONFIGURATION:
                addFtsConfiguration((PgFtsConfiguration) st);
                break;
            case FTS_DICTIONARY:
                addFtsDictionary((PgFtsDictionary) st);
                break;
            case FTS_PARSER:
                addFtsParser((PgFtsParser) st);
                break;
            case FTS_TEMPLATE:
                addFtsTemplate((PgFtsTemplate) st);
                break;
            case OPERATOR:
                addOperator((PgOperator) st);
                break;
            case COLLATION:
                addCollation((PgCollation) st);
                break;
            case STATISTICS:
                addStatistics((PgStatistics) st);
                break;
            case AGGREGATE, FUNCTION, PROCEDURE:
                addFunction((PgAbstractFunction) st);
                break;
            case SEQUENCE:
                addSequence((PgSequence) st);
                break;
            case TABLE:
                addTable((PgAbstractTable) st);
                break;
            case TYPE:
                addType((PgAbstractType) st);
                break;
            case VIEW:
                addView((PgAbstractView) st);
                break;
            default:
                throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    private void addView(PgAbstractView st) {
        addUnique(views, st);
    }

    private void addTable(PgAbstractTable st) {
        addUnique(tables, st);
    }

    private void addSequence(PgSequence st) {
        addUnique(sequences, st);
    }

    private void addFunction(PgAbstractFunction st) {
        addUnique(functions, st);
    }

    /**
     * @return found relation or null if no such relation has been found
     */
    @Override
    public IRelation getRelation(String name) {
        IRelation result = getTable(name);
        if (result == null) {
            result = getView(name);
        }
        if (result == null) {
            result = getSequence(name);
        }
        return result;
    }

    @Override
    public Stream<IRelation> getRelations() {
        return Stream.concat(Stream.concat(tables.values().stream(), views.values().stream()),
                sequences.values().stream());
    }

    /**
     * Getter for {@link #functions}. The list cannot be modified.
     *
     * @return {@link #functions}
     */
    public Collection<IFunction> getFunctions() {
        return Collections.unmodifiableCollection(functions.values());
    }

    public Collection<PgSequence> getSequences() {
        return Collections.unmodifiableCollection(sequences.values());
    }

    /**
     * Gets a statement container by name.
     *
     * @param name the name of the container to find
     * @return the statement container with the given name, or null if not found
     */
    @Override
    public PgAbstractStatementContainer getStatementContainer(String name) {
        PgAbstractStatementContainer container = getTable(name);
        return container == null ? getView(name) : container;
    }

    /**
     * Gets a stream of all statement containers in this schema.
     *
     * @return a stream of statement containers
     */
    public Stream<PgAbstractStatementContainer> getStatementContainers() {
        return Stream.concat(tables.values().stream(), views.values().stream());
    }

    /**
     * Finds an index by name across all tables and views in this schema.
     *
     * @param indexName the name of the index to find
     * @return the index with the given name, or null if not found
     */
    public PgIndex getIndexByName(String indexName) {
        return getStatementContainers()
                .map(c -> c.getIndex(indexName))
                .filter(Objects::nonNull)
                .findAny().orElse(null);
    }

    /**
     * Finds a constraint by name across all tables and views in this schema.
     *
     * @param constraintName the name of the constraint to find
     * @return the constraint with the given name, or null if not found
     */
    public PgConstraint getConstraintByName(String constraintName) {
        return (PgConstraint) getStatementContainers()
                .map(c -> c.getChild(constraintName, DbObjType.CONSTRAINT))
                .filter(Objects::nonNull)
                .findAny().orElse(null);
    }

    @Override
    public void computeHash(Hasher hasher) {
        // all hashable fields in AbstractStatement
    }

    @Override
    protected void computeChildrenHash(Hasher hasher) {
        super.computeChildrenHash(hasher);
        hasher.putUnordered(sequences);
        hasher.putUnordered(functions);
        hasher.putUnordered(views);
        hasher.putUnordered(tables);
        hasher.putUnordered(types);
        hasher.putUnordered(domains);
        hasher.putUnordered(collations);
        hasher.putUnordered(parsers);
        hasher.putUnordered(templates);
        hasher.putUnordered(dictionaries);
        hasher.putUnordered(configurations);
        hasher.putUnordered(operators);
        hasher.putUnordered(statistics);
    }

    @Override
    public boolean compare(IStatement obj) {
        return this == obj || super.compare(obj);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        return obj instanceof PgSchema schema && super.compareChildren(obj)
                && sequences.equals(schema.sequences)
                && functions.equals(schema.functions)
                && views.equals(schema.views)
                && tables.equals(schema.tables)
                && types.equals(schema.types)
                && domains.equals(schema.domains)
                && collations.equals(schema.collations)
                && parsers.equals(schema.parsers)
                && templates.equals(schema.templates)
                && dictionaries.equals(schema.dictionaries)
                && configurations.equals(schema.configurations)
                && operators.equals(schema.operators)
                && statistics.equals(schema.statistics);
    }

    @Override
    protected PgSchema getCopy() {
        return new PgSchema(name);
    }
}
