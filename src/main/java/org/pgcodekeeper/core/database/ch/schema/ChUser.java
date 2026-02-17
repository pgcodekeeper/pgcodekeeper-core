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
package org.pgcodekeeper.core.database.ch.schema;

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Represents a ClickHouse user account with authentication and authorization settings.
 * It supports hosts restrictions, role assignments, default database, and grantee configurations.
 */
public class ChUser extends ChAbstractStatement {

    private static final String DEF_STORAGE = "local_directory";
    private static final String EXCEPT = " EXCEPT ";
    private static final String DEFAULT = "DEFAULT ROLE ";
    private static final String DELIM = "\n\t";

    private final List<String> hosts = new ArrayList<>();
    private final List<String> grantees = new ArrayList<>();
    private final List<String> exGrantees = new ArrayList<>();
    private final List<String> defRoles = new ArrayList<>();
    private final List<String> exceptRoles = new ArrayList<>();

    private String storageType = DEF_STORAGE;
    private String defDb;

    /**
     * Creates a new ClickHouse user with the specified name.
     *
     * @param name the name of the user
     */
    public ChUser(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.USER;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE USER ");
        appendIfNotExists(sbSQL, script.getSettings());
        sbSQL.append(getQuotedName());
        if (!hosts.isEmpty()) {
            sbSQL.append(DELIM).append("HOST ").append(String.join(", ", hosts));
        }

        if (!DEF_STORAGE.equals(storageType)) {
            sbSQL.append(DELIM).append("IN ").append(storageType);
        }
        //append role names
        appendRoles(defRoles, exceptRoles, DEFAULT, "ALL", true, sbSQL);

        if (defDb != null) {
            sbSQL.append(DELIM).append("DEFAULT DATABASE ").append(defDb);
        }

        //append grantees's names
        appendRoles(grantees, exGrantees, "GRANTEES ", "ANY", true, sbSQL);

        script.addStatement(sbSQL);
        appendPrivileges(script);
    }

    private void appendRoles(Collection<String> roles, Collection<String> excepts, String prefix, String allText,
                             boolean isCreate, StringBuilder sbSQL) {
        if (!roles.isEmpty()) {
            sbSQL.append(DELIM);
            sbSQL.append(prefix);
            sbSQL.append(String.join(", ", roles));
        } else if (!excepts.isEmpty() || !isCreate) {
            sbSQL.append(DELIM);
            sbSQL.append(prefix);
            sbSQL.append(allText);
        }

        if (!excepts.isEmpty()) {
            sbSQL.append(EXCEPT).append(String.join(", ", excepts));
        }
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        ChUser newUser = (ChUser) newCondition;

        StringBuilder sbSql = new StringBuilder();
        compareHosts(hosts, newUser.hosts, sbSql);
        if (!Objects.equals(defRoles, newUser.defRoles) || !Objects.equals(exceptRoles, newUser.exceptRoles)) {
            appendRoles(newUser.defRoles, newUser.exceptRoles, DEFAULT, "ALL", false, sbSql);
        }
        if (!Objects.equals(defDb, newUser.defDb)) {
            sbSql.append(DELIM).append("DEFAULT DATABASE ");
            sbSql.append(newUser.defDb == null ? "NONE" : newUser.defDb);
        }
        if (!Objects.equals(grantees, newUser.grantees) || !Objects.equals(exGrantees, newUser.exGrantees)) {
            appendRoles(newUser.grantees, newUser.exGrantees, "GRANTEES ", "ANY", false, sbSql);
        }

        if (!sbSql.isEmpty()) {
            script.addStatement(new StringBuilder("ALTER USER ").append(getQualifiedName()).append(sbSql));
        }

        if (!Objects.equals(storageType, newUser.storageType)) {
            StringBuilder sql = new StringBuilder();
            sql.append("MOVE ROLE ")
                    .append(getQualifiedName()).append(" TO ")
                    .append(newUser.storageType);
            script.addStatement(sql);
        }
        alterPrivileges(newUser, script);
        return getObjectState(script, startSize);
    }

    private void compareHosts(List<String> oldHosts, List<String> newHosts, StringBuilder sb) {
        if (Objects.equals(oldHosts, newHosts)) {
            return;
        }
        sb.append(DELIM).append("HOST ");

        if (newHosts.isEmpty()) {
            sb.append("ANY");
        } else {
            sb.append(String.join(", ", newHosts));
        }
    }

    public void setDefaultDatabase(String defaultDatabase) {
        this.defDb = defaultDatabase;
        resetHash();
    }

    /**
     * Adds a default role for this user.
     *
     * @param defRole the role name to add as default
     */
    public void addDefRole(String defRole) {
        defRoles.add(defRole);
        resetHash();
    }

    /**
     * Adds a role to the EXCEPT list for default roles.
     *
     * @param exceptRole the role name to exclude from defaults
     */
    public void addExceptRole(String exceptRole) {
        exceptRoles.add(exceptRole);
        resetHash();
    }

    /**
     * Adds a grantee role for this user.
     *
     * @param grantee the grantee role name to add
     */
    public void addGrantee(String grantee) {
        grantees.add(grantee);
        resetHash();
    }

    /**
     * Adds a role to the EXCEPT list for grantees.
     *
     * @param exGrantee the grantee role name to exclude
     */
    public void addExGrantee(String exGrantee) {
        exGrantees.add(exGrantee);
        resetHash();
    }

    /**
     * Checks if this user has host restrictions.
     *
     * @return true if no host restrictions are defined, false otherwise
     */
    public boolean hasHosts() {
        return hosts.isEmpty();
    }

    /**
     * Adds a host restriction for this user.
     *
     * @param host the host pattern to add
     */
    public void addHost(String host) {
        hosts.add(host);
        resetHash();
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(hosts);
        hasher.put(storageType);
        hasher.put(defRoles);
        hasher.put(exceptRoles);
        hasher.put(grantees);
        hasher.put(exGrantees);
        hasher.put(defDb);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ChUser user
                && super.compare(obj)
                && hosts.equals(user.hosts)
                && Objects.equals(storageType, user.storageType)
                && defRoles.equals(user.defRoles)
                && exceptRoles.equals(user.exceptRoles)
                && grantees.equals(user.grantees)
                && exGrantees.equals(user.exGrantees)
                && Objects.equals(defDb, user.defDb);
    }

    @Override
    protected AbstractStatement getCopy() {
        ChUser userDst = new ChUser(name);
        userDst.hosts.addAll(hosts);
        userDst.setStorageType(storageType);
        userDst.defRoles.addAll(defRoles);
        userDst.exceptRoles.addAll(exceptRoles);
        userDst.grantees.addAll(grantees);
        userDst.exGrantees.addAll(exGrantees);
        userDst.setDefaultDatabase(defDb);
        return userDst;
    }
}