package ru.taximaxim.codekeeper.core.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.taximaxim.codekeeper.core.Consts;
import ru.taximaxim.codekeeper.core.MsDiffUtils;
import ru.taximaxim.codekeeper.core.PgDiffArguments;
import ru.taximaxim.codekeeper.core.PgDiffUtils;
import ru.taximaxim.codekeeper.core.formatter.FileFormatter;
import ru.taximaxim.codekeeper.core.formatter.FormatterException;
import ru.taximaxim.codekeeper.core.hashers.Hasher;
import ru.taximaxim.codekeeper.core.hashers.IHashable;
import ru.taximaxim.codekeeper.core.hashers.JavaHasher;
import ru.taximaxim.codekeeper.core.hashers.ShaHasher;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.parsers.antlr.exception.ObjectCreationException;

/**
 * The superclass for general pgsql statement.
 * All changes to hashed fields of extending classes must be
 * followed by a {@link #resetHash()} call.
 *
 * @author Alexander Levsha
 */
public abstract class PgStatement implements IStatement, IHashable {

    private static final Logger LOG = LoggerFactory.getLogger(PgColumn.class);

    //TODO move to MS SQL statement abstract class.
    public static final String GO = "\nGO";
    protected final String name;
    protected String owner;
    protected String comment;
    protected final Set<PgPrivilege> privileges = new LinkedHashSet<>();

    private PgStatement parent;
    protected final Set<GenericColumn> deps = new LinkedHashSet<>();

    private final PgStatementMeta meta = new PgStatementMeta();

    // 0 means not calculated yet and/or hash has been reset
    private int hash;

    public PgStatement(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    //TODO enum later
    public boolean isPostgres() {
        return true;
    }

    public boolean canDrop() {
        return true;
    }

    public boolean isOwned() {
        switch (getStatementType()) {
        case FOREIGN_DATA_WRAPPER:
        case SERVER:
        case FTS_CONFIGURATION:
        case FTS_DICTIONARY:
        case TABLE:
        case VIEW:
        case SCHEMA:
        case FUNCTION:
        case OPERATOR:
        case PROCEDURE:
        case AGGREGATE:
        case SEQUENCE:
        case COLLATION:
        case TYPE:
        case DOMAIN:
        case ASSEMBLY:
            return true;
        default :
            return false;
        }
    }

    /**
     * @return Always returns just the object's name.
     */
    // TODO super?
    @Override
    public final String getBareName() {
        return name;
    }

    @Override
    public PgStatement getParent() {
        return parent;
    }

    public PgObjLocation getLocation() {
        return getMeta().getLocation();
    }

    public void setLocation(PgObjLocation location) {
        getMeta().setLocation(location);
    }

    public boolean isLib() {
        return getMeta().isLib();
    }

    public String getLibName() {
        return getMeta().getLibName();
    }

    public void setLibName(String libName) {
        getMeta().setLibName(libName);
    }

    public String getAuthor() {
        return getMeta().getAuthor();
    }

    public void setAuthor(String author) {
        getMeta().setAuthor(author);
    }

    public PgStatementMeta getMeta() {
        return meta;
    }

    public abstract PgDatabase getDatabase();

    public void setParent(PgStatement parent) {
        if(this.parent != null) {
            throw new IllegalStateException("Statement already has a parent: "
                    + this.getClass() + " Name: " + this.getName());
        }

        this.parent = parent;
    }

    public Set<GenericColumn> getDeps() {
        return Collections.unmodifiableSet(deps);
    }

    public void addDep(GenericColumn dep){
        // TODO remove after fix
        if (dep == null) {
            Exception ex = new Exception("null dependency added for " + getQualifiedName());
            LOG.error(ex.getLocalizedMessage(), ex);
        }
        deps.add(dep);
    }

    public void addAllDeps(Collection<GenericColumn> deps){
        // TODO remove after fix
        if (deps.contains(null)) {
            Exception ex = new Exception("null dependency added for " + getQualifiedName());
            LOG.error(ex.getLocalizedMessage(), ex);
        }
        this.deps.addAll(deps);
    }

    @Override
    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
        resetHash();
    }

    protected String getTypeName() {
        return getStatementType().toString();
    }

    protected StringBuilder appendFullName(StringBuilder sb) {
        return sb.append(getQualifiedName());
    }

    /**
     * Sets {@link #comment} with newlines as requested in arguments.
     */
    public void setComment(PgDiffArguments args, String comment) {
        setComment(args.isKeepNewlines() ? comment : comment.replace("\r", ""));
    }

    protected StringBuilder appendCommentSql(StringBuilder sb) {
        sb.append("COMMENT ON ").append(getTypeName()).append(' ');
        appendFullName(sb);
        return sb.append(" IS ")
                .append(comment == null || comment.isEmpty() ? "NULL" : comment)
                .append(';');
    }

    public Set<PgPrivilege> getPrivileges() {
        return Collections.unmodifiableSet(privileges);
    }

    public void addPrivilege(PgPrivilege privilege) {
        if (isPostgres()) {
            String locOwner;
            if (owner == null && getStatementType() == DbObjType.SCHEMA
                    && Consts.PUBLIC.equals(getName())) {
                locOwner = "postgres";
            } else {
                locOwner = owner;
            }

            // Skip filtering if statement type is COLUMN, because of the
            // specific relationship with table privileges.
            // The privileges of columns for role are not set lower than for the
            // same role in the parent table, they may be the same or higher.
            if (DbObjType.COLUMN != getStatementType()
                    && "ALL".equalsIgnoreCase(privilege.getPermission())) {
                addPrivilegeFiltered(privilege, locOwner);
            } else {
                privileges.add(privilege);
            }

        } else {
            privileges.add(privilege);
        }
        resetHash();
    }

    private void addPrivilegeFiltered(PgPrivilege privilege, String locOwner) {
        if ("PUBLIC".equals(privilege.getRole())) {
            boolean isFunc;
            switch (getStatementType()) {
            // revoke public is non-default for funcs etc
            // grant public is default for them
            case FUNCTION:
            case PROCEDURE:
            case AGGREGATE:
            case DOMAIN:
            case TYPE:
                isFunc = true;
                break;
            default:
                isFunc = false;
                break;
            }
            if (isFunc != privilege.isRevoke()) {
                return;
            }
        }

        if (!privilege.isRevoke() && privilege.getRole().equals(locOwner)) {
            PgPrivilege delRevoke = privileges.stream()
                    .filter(p -> p.isRevoke()
                            && p.getRole().equals(privilege.getRole())
                            && p.getPermission().equals(privilege.getPermission()))
                    .findAny().orElse(null);
            if (delRevoke != null) {
                privileges.remove(delRevoke);
                return;
            }
        }
        privileges.add(privilege);
    }

    public void clearPrivileges() {
        privileges.clear();
        resetHash();
    }

    protected StringBuilder appendPrivileges(StringBuilder sb) {
        PgPrivilege.appendPrivileges(privileges, isPostgres(), sb);
        return sb;
    }

    protected void alterPrivileges(PgStatement newObj, StringBuilder sb) {
        // first drop (revoke) missing grants
        Set<PgPrivilege> newPrivileges = newObj.getPrivileges();
        for (PgPrivilege privilege : privileges) {
            if (!privilege.isRevoke() && !newPrivileges.contains(privilege)) {
                sb.append('\n').append(privilege.getDropSQL()).append(isPostgres() ? ';' : "\nGO");
            }
        }

        // now set all privileges if there are any changes
        if (!privileges.equals(newPrivileges)) {
            if (newObj.isPostgres()) {
                // first, reset all default privileges
                // this generates noisier bit more correct scripts
                // we may have revoked implicit owner GRANT in the previous step, it needs to be restored
                // any privileges in non-default state will be set to their final state in the next step
                // this solution also requires the least amount of handling code: no edge cases
                PgPrivilege.appendDefaultPrivileges(newObj, sb);
            }
            newObj.appendPrivileges(sb);
        }
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
        resetHash();
    }

    protected StringBuilder appendOwnerSQL(StringBuilder sb) {
        return appendOwnerSQL(this, owner, true, sb);
    }

    public StringBuilder alterOwnerSQL(StringBuilder sb) {
        if (!isPostgres() && owner == null) {
            sb.append("\n\nALTER AUTHORIZATION ON ");
            DbObjType type = getStatementType();
            if (DbObjType.TYPE == type || DbObjType.SCHEMA == type
                    || DbObjType.ASSEMBLY == type) {
                sb.append(type).append("::");
            }

            sb.append(getQualifiedName()).append(" TO ");

            if (DbObjType.SCHEMA == type || DbObjType.ASSEMBLY == type) {
                sb.append("[dbo]");
            } else {
                sb.append("SCHEMA OWNER");
            }

            sb.append(GO);
        } else {
            appendOwnerSQL(sb);
        }
        return sb;
    }

    public static StringBuilder appendOwnerSQL(PgStatement st, String owner,
            boolean addNewLine, StringBuilder sb) {
        if (owner == null || !st.isOwned()) {
            return sb;
        }
        if (addNewLine) {
            sb.append("\n\n");
        }
        sb.append("ALTER ");
        if (st.isPostgres()) {
            sb.append(st.getTypeName()).append(' ');
            st.appendFullName(sb);
            sb.append(" OWNER TO ")
            .append(PgDiffUtils.getQuotedName(owner))
            .append(';');
        } else {
            sb.append("AUTHORIZATION ON ");
            DbObjType type = st.getStatementType();
            if (DbObjType.TYPE == type || DbObjType.SCHEMA == type
                    || DbObjType.ASSEMBLY == type) {
                sb.append(type).append("::");
            }

            sb.append(st.getQualifiedName()).append(" TO ")
            .append(MsDiffUtils.quoteName(owner)).append(GO);
        }

        return sb;
    }

    public abstract String getCreationSQL();

    public String getFullSQL() {
        return getCreationSQL();
    }

    public String getFullFormattedSQL() {
        return formatSQL(getFullSQL());
    }

    public String getFormattedCreationSQL() {
        return formatSQL(getCreationSQL());
    }

    private String formatSQL(String sql) {
        PgDiffArguments args = getDatabase().getArguments();
        if (args == null || !args.isAutoFormatObjectCode()) {
            return sql;
        }
        FileFormatter fileForm = new FileFormatter(sql, 0, sql.length(), args.getFormatConfiguration(), !isPostgres());
        try {
            return fileForm.formatText();
        } catch (FormatterException e) {
            LOG.error(e.getLocalizedMessage(), e);
            return sql;
        }
    }

    public final String getDropSQL() {
        PgDiffArguments args = getDatabase().getArguments();
        return getDropSQL(args != null && args.isGenerateExists());
    }

    protected String getDropSQL(boolean generateExists) {
        final StringBuilder sbString = new StringBuilder();
        sbString.append("DROP ").append(getTypeName()).append(' ');
        if (generateExists) {
            sbString.append("IF EXISTS ");
        }
        appendFullName(sbString);
        sbString.append(isPostgres() ? ';' : GO);
        return sbString.toString();
    }

    protected void appendDropBeforeCreate(StringBuilder sb) {
        PgDiffArguments args = getDatabase().getArguments();
        if (args != null && args.isDropBeforeCreate()) {
            sb.append(getDropSQL(true));
            sb.append("\n\n");
        }
    }

    protected void appendIfNotExists(StringBuilder sb) {
        PgDiffArguments args = getDatabase().getArguments();
        if (args != null && args.isGenerateExists()) {
            sb.append("IF NOT EXISTS ");
        }
    }

    /**
     * ?????????? ?????????????????? sb ???????????????????? ?????????????????? ??????????????, ?????????? ???? ???????????????? ???????????? ALTER.<br>
     * <br>
     *
     * ?????????????????? ???????????? ???????????? ???????????????????????? ???? ???????? ????????????????: ???????????????????????? ???????????????? ?? ?????????? sb.length().<br>
     * ???????????????????????? ???????????????? ?????????????? ?? ?????????????? ??????????????: ?????????????????? ?????? ??????.<br>
     * sb.length() ?????????????? ?? ?????????????????????????????????????? ?????????????????? ?????????????? ALTER???? (???????? ???????????? ???????????? ??????????????????).<br>
     * <br>
     *
     * {@code sb == 0 and rv == false} - ???? ?????????????????? ????????????????<br>
     * {@code sb >  0 and rv == false} - illegal state, ???????????????????????? ???????????? ?? ALTER<br>
     * {@code sb == 0 and rv == true} - ALTER ????????????????????, ???????????? DROP/CREATE<br>
     * {@code sb >  0 and rv == true} - ???????????? ALTER
     *
     * @param newCondition
     *            ?????????? ???????????????????? ??????????????
     * @param sb
     *            ???????????? ??????????????????
     * @param isNeedDepcies
     *            out ????????????????: ?????????? ???? ???????????????????????? ?????????????????????? ??????????????
     * @return true - ???????????????????? ???????????????? ????????????, ?????????????????? DROP ?? ???????????? ?????????????????????????? ALTER, false - ???????????? ????
     *         ??????????????????
     */
    public abstract boolean appendAlterSQL(PgStatement newCondition, StringBuilder sb,
            AtomicBoolean isNeedDepcies);

    /**
     * Copies all object properties into a new object and leaves all its children empty.
     *
     * @return shallow copy of a DB object.
     */
    public abstract PgStatement shallowCopy();

    /**
     * Performs {@link #shallowCopy()} on this object and all its children.
     *
     * @return a fully recursive copy of this statement.
     */
    public final PgStatement deepCopy() {
        PgStatement copy = shallowCopy();
        getChildren().forEach(st -> copy.addChild(st.deepCopy()));
        return copy;
    }

    /**
     * This method does not account for nested child PgStatements.
     * Shallow version of {@link #equals(Object)}
     */
    public boolean compare(PgStatement obj) {
        return getStatementType() == obj.getStatementType()
                && Objects.equals(name, obj.name)
                && Objects.equals(owner, obj.owner)
                && Objects.equals(comment, obj.comment)
                && privileges.equals(obj.privileges);
    }

    protected final void copyBaseFields(PgStatement copy) {
        copy.setOwner(owner);
        copy.setComment(comment);
        copy.deps.addAll(deps);
        copy.privileges.addAll(privileges);
        copy.meta.copy(meta);
    }

    /**
     * @return an element in another db sharing the same name and location
     */
    public PgStatement getTwin(PgDatabase db) {
        // fast path for getting a "twin" from the same database
        // return the same object immediately
        return getDatabase() == db ? this : getTwinRecursive(db);
    }

    private PgStatement getTwinRecursive(PgDatabase db) {
        DbObjType type = getStatementType();
        if (DbObjType.DATABASE == type) {
            return db;
        }
        PgStatement twinParent = getParent().getTwinRecursive(db);
        if (twinParent == null) {
            return null;
        }
        return DbObjType.COLUMN == type ? ((AbstractTable) twinParent).getColumn(getName())
                : twinParent.getChild(getName(), type);
    }

    /**
     * Returns all subtree elements
     */
    public final Stream<PgStatement> getDescendants() {
        List<Collection<? extends PgStatement>> l = new ArrayList<>();
        fillDescendantsList(l);
        return l.stream().flatMap(Collection::stream);
    }

    /**
     * Returns all subelements of current element
     */
    public final Stream<PgStatement> getChildren() {
        List<Collection<? extends PgStatement>> l = new ArrayList<>();
        fillChildrenList(l);
        return l.stream().flatMap(Collection::stream);
    }

    public PgStatement getChild(String name, DbObjType type) {
        return null;
    }

    public boolean hasChildren() {
        return getChildren().anyMatch(e -> true);
    }

    protected void fillDescendantsList(List<Collection<? extends PgStatement>> l) {
        fillChildrenList(l);
    }

    protected void fillChildrenList(List<Collection<? extends PgStatement>> l) {
        // default no op
    }

    public void addChild(PgStatement st) {
        //  subclasses with children must override
        throw new IllegalStateException("Statement can't have child");
    }

    /**
     * Deep part of {@link #equals(Object)}.
     * Compares all object's child PgStatements for equality.
     */
    public boolean compareChildren(PgStatement obj) {
        if (obj == null) {
            throw new IllegalArgumentException("Null PgStatement!");
        }
        return true;
    }

    /**
     * Compares this object and all its children with another statement.
     * <hr><br>
     * {@inheritDoc}
     */
    @Override
    public final boolean equals(Object obj){
        if (this == obj) {
            return true;
        } else if (obj instanceof PgStatement) {
            PgStatement st = (PgStatement) obj;
            return this.compare(st)
                    && this.parentNamesEquals(st)
                    && this.compareChildren(st);
        }
        return false;
    }

    /**
     * Recursively compares objects' parents
     * to ensure their equal position in their object trees.
     */
    private boolean parentNamesEquals(PgStatement st){
        PgStatement p = parent;
        PgStatement p2 = st.getParent();
        while (p != null && p2 != null){
            if (!Objects.equals(p.getName(), p2.getName())){
                return false;
            }
            p = p.getParent();
            p2 = p2.getParent();
        }
        return p == null && p2 == null;
    }

    /**
     * Calls {@link #computeHash}. Modifies that value with combined hashcode
     * of all parents of this object in the tree to complement
     * {@link #parentNamesEquals(PgStatement)} and {@link #equals(Object)}<br>
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


    /**
     * Compute local hash
     *
     * @return hash byte array
     */
    public final byte[] shaHash() {
        ShaHasher hasher = new ShaHasher();
        computeLocalHash(hasher);
        computeHash(hasher);
        return hasher.getArray();
    }

    private final void computeLocalHash(Hasher hasher) {
        hasher.put(name);
        hasher.put(owner);
        hasher.put(comment);
        hasher.putUnordered(privileges);
    }

    protected void resetHash(){
        PgStatement st = this;
        while (st != null){
            st.hash = 0;
            st = st.getParent();
        }
    }

    protected void computeChildrenHash(Hasher hasher) {
        // subclasses with children must override
    }

    private void computeNamesHash(Hasher hasher) {
        PgStatement p = parent;
        while (p != null) {
            String pName = p.getName();
            hasher.put(pName);
            p = p.getParent();
        }
    }

    /**
     * @return fully qualified (up to schema) dot-delimited object name.
     *          Identifiers are quoted.
     */
    @Override
    public String getQualifiedName() {
        Function<String, String> quoter = isPostgres() ? PgDiffUtils::getQuotedName : MsDiffUtils::quoteName;
        StringBuilder sb = new StringBuilder(quoter.apply(getName()));

        PgStatement par = this.parent;
        while (par != null && !(par instanceof PgDatabase)) {
            sb.insert(0, '.').insert(0, quoter.apply(par.getName()));
            par = par.getParent();
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return name == null ? "Unnamed object" : name;
    }

    protected void assertUnique(PgStatement found, PgStatement newSt) {
        if (found != null) {
            PgStatement foundParent = found.getParent();
            throw foundParent instanceof PgStatementWithSearchPath
            ? new ObjectCreationException(newSt, foundParent)
                    : new ObjectCreationException(newSt);
        }
    }

    protected <T extends PgStatement>
    void addUnique(Map<String, T> map, T newSt) {
        PgStatement found = map.putIfAbsent(newSt.getName(), newSt);
        assertUnique(found, newSt);
        newSt.setParent(this);
        resetHash();
    }
}
