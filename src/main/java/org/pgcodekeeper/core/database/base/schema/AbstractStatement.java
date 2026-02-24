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
package org.pgcodekeeper.core.database.base.schema;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.exception.ObjectCreationException;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.hasher.IHashable;
import org.pgcodekeeper.core.hasher.JavaHasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.*;
import java.util.stream.Stream;

/**
 * Abstract base class for all database statements and objects.
 * Provides common functionality including naming, ownership, privileges, dependencies,
 * and metadata management. All changes to hashed fields of extending classes must be
 * followed by a {@link #resetHash()} call.
 *
 * @author Alexander Levsha
 */
public abstract class AbstractStatement implements IStatement, IHashable {

    protected static final String IF_EXISTS = "IF EXISTS ";
    protected static final String ALTER_TABLE = "ALTER TABLE ";

    private static final String SEPARATOR = ";";

    protected final String name;
    protected final Set<IPrivilege> privileges = new LinkedHashSet<>();
    protected final Set<ObjectReference> deps = new LinkedHashSet<>();
    protected final StatementMeta meta = new StatementMeta();

    protected String owner;
    protected String comment;
    protected String qualifiedName;
    protected AbstractStatement parent;

    // 0 means not calculated yet and/or hash has been reset
    private int hash;

    protected AbstractStatement(String name) {
        this.name = name;
    }

    /**
     * Appends comment SQL to the script if this statement has comments.
     *
     * @param script the SQL script to append comments to
     */
    public void appendComments(SQLScript script) {
        if (checkComments()) {
            appendCommentSql(script);
        }
    }

    /**
     * Appends ALTER comment SQL if the comment has changed.
     *
     * @param newObj the new statement to compare comments with
     * @param script the SQL script to append ALTER comments to
     */
    public void appendAlterComments(AbstractStatement newObj, SQLScript script) {
        if (!Objects.equals(getComment(), newObj.getComment())) {
            newObj.appendCommentSql(script);
        }
    }

    protected void appendCommentSql(SQLScript script) {
        StringBuilder sb = new StringBuilder();
        sb.append("COMMENT ON ").append(getTypeName()).append(' ');
        appendFullName(sb);
        sb.append(" IS ").append(checkComments() ? comment : "NULL");
        script.addCommentStatement(sb.toString());
    }

    protected void appendAlterOwner(AbstractStatement newObj, SQLScript script) {
        if (!Objects.equals(owner, newObj.owner)) {
            newObj.alterOwnerSQL(script);
        }
    }

    protected void alterOwnerSQL(SQLScript script) {
        appendOwnerSQL(script);
    }

    public void appendPrivileges(SQLScript script) {
        IPrivilege.appendPrivileges(privileges, script);
    }

    protected void alterPrivileges(AbstractStatement newObj, SQLScript script) {
        Set<IPrivilege> newPrivileges = newObj.getPrivileges();

        // first drop (revoke) missing grants
        for (IPrivilege privilege : privileges) {
            if (!privilege.isRevoke() && !newPrivileges.contains(privilege)) {
                script.addStatement(privilege.getDropSQL());
            }
        }

        // now set all privileges if there are any changes
        if (!privileges.equals(newPrivileges)) {
            appendDefaultPrivileges(newObj, script);
            IPrivilege.appendPrivileges(newPrivileges, script);
        }
    }

    protected void appendDefaultPrivileges(IStatement statement, SQLScript script) {
        // no imp
    }

    @Override
    public String getSQL(boolean isFormatted, ISettings settings) {
        SQLScript script = new SQLScript(settings, getSeparator());
        getCreationSQL(script);
        String sql = script.getFullScript();
        if (!isFormatted || !settings.isAutoFormatObjectCode()) {
            return sql;
        }
        return formatSql(sql, 0, sql.length(), settings.getFormatConfiguration());
    }

    /**
     * Generates DROP SQL for this statement using settings from the script.
     *
     * @param script the SQL script to append the DROP statement to
     */
    public final void getDropSQL(SQLScript script) {
        getDropSQL(script, script.getSettings().isGenerateExists());
    }

    @Override
    public void getDropSQL(SQLScript script, boolean generateExists) {
        final StringBuilder sb = new StringBuilder();
        sb.append("DROP ").append(getTypeName()).append(' ');
        if (generateExists) {
            sb.append(IF_EXISTS);
        }
        appendFullName(sb);
        script.addStatement(sb);
    }

    protected void appendIfNotExists(StringBuilder sb, ISettings settings) {
        if (settings.isGenerateExists()) {
            sb.append("IF NOT EXISTS ");
        }
    }

    @Override
    public boolean canDropBeforeCreate() {
        return false;
    }

    /**
     * Determines the object state based on changes made to the script.
     *
     * @param script    the SQL script to check for changes
     * @param startSize the initial size of the script before changes
     * @return the object state indicating the type of change
     */
    public ObjectState getObjectState(SQLScript script, int startSize) {
        return getObjectState(false, script, startSize);
    }

    /**
     * Determines the object state based on changes made to the script.
     *
     * @param isNeedDepcies whether dependencies need to be considered
     * @param script        the SQL script to check for changes
     * @param startSize     the initial size of the script before changes
     * @return the object state: NOTHING if no changes, ALTER_WITH_DEP if dependencies needed, ALTER otherwise
     */
    public ObjectState getObjectState(boolean isNeedDepcies, SQLScript script, int startSize) {
        if (script.getSize() == startSize) {
            return ObjectState.NOTHING;
        }

        return isNeedDepcies ? ObjectState.ALTER_WITH_DEP : ObjectState.ALTER;
    }


    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * @return Always returns just the object's name.
     */
    @Override
    public final String getBareName() {
        return name;
    }

    /**
     * Gets the parent statement that contains this statement.
     *
     * @return the parent statement, or null if this is a top-level statement
     */
    @Override
    public AbstractStatement getParent() {
        return parent;
    }

    @Override
    public ObjectLocation getLocation() {
        return meta.getLocation();
    }

    /**
     * Sets the location information for this statement.
     *
     * @param location the location where this statement is defined
     */
    @Override
    public void setLocation(ObjectLocation location) {
        meta.setLocation(location);
    }

    @Override
    public boolean isLib() {
        return meta.isLib();
    }

    @Override
    public String getLibName() {
        return meta.getLibName();
    }

    /**
     * Sets the name of the library this statement comes from.
     *
     * @param libName the library name to set
     */
    @Override
    public void setLibName(String libName) {
        meta.setLibName(libName);
    }

    /**
     * Gets the author of this statement.
     *
     * @return the author name, or null if not specified
     */
    public String getAuthor() {
        return meta.getAuthor();
    }

    /**
     * Sets the author of this statement.
     *
     * @param author the author name to set
     */
    public void setAuthor(String author) {
        meta.setAuthor(author);
    }

    /**
     * Sets the parent statement for this statement.
     *
     * @param parent the parent statement to set
     * @throws IllegalStateException if this statement already has a parent
     */
    public void setParent(AbstractStatement parent) {
        if (parent != null && this.parent != null) {
            throw new IllegalStateException("Statement already has a parent: "
                    + this.getClass() + " Name: " + this.getName());
        }

        qualifiedName = null;
        this.parent = parent;
    }

    @Override
    public Set<ObjectReference> getDependencies() {
        return Collections.unmodifiableSet(deps);
    }

    @Override
    public void addDependency(ObjectReference dep) {
        deps.add(dep);
    }

    @Override
    public String getComment() {
        return comment;
    }

    /**
     * Checks if this statement has non-empty comments.
     *
     * @return true if the statement has comments
     */
    public boolean checkComments() {
        return comment != null && !comment.isEmpty();
    }

    public void setComment(String comment) {
        this.comment = comment;
        resetHash();
    }

    /**
     * Gets an unmodifiable set of privileges for this statement.
     *
     * @return unmodifiable set of privileges
     */
    @Override
    public Set<IPrivilege> getPrivileges() {
        return Collections.unmodifiableSet(privileges);
    }

    /**
     * Adds a privilege to this statement.
     *
     * @param privilege the privilege to add
     * @throws IllegalArgumentException if database type is unsupported
     */
    public void addPrivilege(IPrivilege privilege) {
        privileges.add(privilege);
        resetHash();
    }

    @Override
    public void clearPrivileges() {
        privileges.clear();
        resetHash();
    }

    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public void setOwner(String owner) {
        this.owner = owner;
        resetHash();
    }

    @Override
    public IStatement getTwin(IDatabase db) {
        // fast path for getting a "twin" from the same database
        // return the same object immediately
        return getDatabase() == db ? this : getTwinRecursive(db);
    }

    private IStatement getTwinRecursive(IDatabase db) {
        DbObjType type = getStatementType();
        if (DbObjType.DATABASE == type) {
            return db;
        }
        IStatement twinParent = parent.getTwinRecursive(db);
        if (twinParent == null) {
            return null;
        }
        if (DbObjType.COLUMN == type) {
            return ((ITable) twinParent).getColumn(getName());
        }
        if (twinParent instanceof IStatementContainer cont) {
            return cont.getChild(getName(), type);
        }

        return null;
    }

    @Override
    public final Stream<AbstractStatement> getDescendants() {
        List<Collection<? extends AbstractStatement>> l = new ArrayList<>();
        fillDescendantsList(l);
        return l.stream().flatMap(Collection::stream);
    }

    @Override
    public final Stream<AbstractStatement> getChildren() {
        List<Collection<? extends AbstractStatement>> l = new ArrayList<>();
        fillChildrenList(l);
        return l.stream().flatMap(Collection::stream);
    }

    /**
     * Checks if this statement has any child statements.
     *
     * @return true if this statement has children, false otherwise
     */
    @Override
    public boolean hasChildren() {
        return getChildren().anyMatch(e -> true);
    }

    public void fillDescendantsList(List<Collection<? extends AbstractStatement>> l) {
        fillChildrenList(l);
    }

    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        // default no op
    }

    /**
     * @return fully qualified (up to schema) dot-delimited object name.
     * Identifiers are quoted.
     */
    @Override
    public String getQualifiedName() {
        if (qualifiedName == null) {
            StringBuilder sb = new StringBuilder(getQuotedName());

            AbstractStatement par = this.parent;
            while (par != null && !(par instanceof IDatabase)) {
                sb.insert(0, '.').insert(0, par.getQuotedName());
                par = par.parent;
            }

            qualifiedName = sb.toString();
        }

        return qualifiedName;
    }

    protected void assertUnique(AbstractStatement found, AbstractStatement newSt) {
        if (found != null) {
            AbstractStatement foundParent = found.parent;
            throw foundParent instanceof ISearchPath
                    ? new ObjectCreationException(newSt, foundParent)
                    : new ObjectCreationException(newSt);
        }
    }

    protected <T extends AbstractStatement> void addUnique(Map<String, T> map, T newSt) {
        AbstractStatement found = map.putIfAbsent(getNameInCorrectCase(newSt.getName()), newSt);
        assertUnique(found, newSt);
        newSt.setParent(this);
        resetHash();
    }

    protected <T extends AbstractStatement> T getChildByName(Map<String, T> map, String name) {
        String lowerCaseName = getNameInCorrectCase(name);
        return map.get(lowerCaseName);
    }

    protected String getNameInCorrectCase(String name) {
        return name;
    }

    protected void appendFullName(StringBuilder sb) {
        sb.append(getQualifiedName());
    }

    @Override
    public String getSeparator() {
        return SEPARATOR;
    }

    /**
     * Calls {@link #computeHash}. Modifies that value with combined hashcode
     * of all parents of this object in the tree to complement
     * {@link #parentNamesEquals(AbstractStatement)} and {@link #equals(Object)}<br>
     * Caches the hashcode value until recalculation is requested via {@link #resetHash()}.
     * Always request recalculation when you change the hashed fields.<br>
     * Do actual hashing in {@link #computeHash}.
     * <hr><br>
     * {@inheritDoc}
     */
    @Override
    public final int hashCode() {
        int h = hash;
        if (h == 0) {
            JavaHasher hasher = new JavaHasher();
            computeLocalHash(hasher);
            computeHash(hasher);
            computeChildrenHash(hasher);
            computeNamesHash(hasher);
            h = hasher.getResult();

            if (h == 0) {
                h = Integer.MAX_VALUE;
            }
            hash = h;
        }
        return h;
    }

    private void computeLocalHash(Hasher hasher) {
        hasher.put(name);
        hasher.put(owner);
        hasher.put(comment);
        hasher.putUnordered(privileges);
    }

    protected void resetHash() {
        AbstractStatement st = this;
        while (st != null) {
            st.hash = 0;
            st = st.parent;
        }
    }

    protected void computeChildrenHash(Hasher hasher) {
        // subclasses with children must override
    }

    private void computeNamesHash(Hasher hasher) {
        AbstractStatement p = parent;
        while (p != null) {
            String pName = p.getName();
            hasher.put(pName);
            p = p.parent;
        }
    }

    /**
     * Compares this object and all its children with another statement.
     * <hr><br>
     * {@inheritDoc}
     */
    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof AbstractStatement st) {
            return this.compare(st)
                    && this.parentNamesEquals(st)
                    && this.compareChildren(st);
        }
        return false;
    }

    /**
     * This method does not account for nested child PgStatements.
     * Shallow version of {@link #equals(Object)}
     */
    @Override
    public boolean compare(IStatement obj) {
        return obj instanceof AbstractStatement statement
                && getStatementType() == obj.getStatementType()
                && Objects.equals(name, statement.name)
                && Objects.equals(owner, statement.owner)
                && Objects.equals(comment, statement.comment)
                && privileges.equals(statement.privileges);
    }

    /**
     * Recursively compares objects' parents
     * to ensure their equal position in their object trees.
     */
    private boolean parentNamesEquals(AbstractStatement st) {
        AbstractStatement p = parent;
        AbstractStatement p2 = st.parent;
        while (p != null && p2 != null) {
            if (!Objects.equals(p.getName(), p2.getName())) {
                return false;
            }
            p = p.parent;
            p2 = p2.parent;
        }
        return p == null && p2 == null;
    }

    /**
     * Deep part of {@link #equals(Object)}.
     * Compares all object's child PgStatements for equality.
     */
    public boolean compareChildren(AbstractStatement obj) {
        if (obj == null) {
            throw new IllegalArgumentException("Null Statement!");
        }
        return true;
    }

    @Override
    public final IStatement deepCopy() {
        IStatement copy = shallowCopy();
        if (copy instanceof IStatementContainer cont) {
            getChildren().forEach(st -> cont.addChild(st.deepCopy()));
        }
        return copy;
    }

    @Override
    public final AbstractStatement shallowCopy() {
        AbstractStatement copy = getCopy();
        copy.setOwner(owner);
        copy.setComment(comment);
        copy.deps.addAll(deps);
        copy.privileges.addAll(privileges);
        copy.meta.copy(meta);
        return copy;
    }

    protected abstract AbstractStatement getCopy();

    @Override
    public String toString() {
        return name == null ? "Unnamed object" : name;
    }
}