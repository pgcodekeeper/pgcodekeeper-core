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
package org.pgcodekeeper.core.database.ms.schema;

import java.util.Objects;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.ms.utils.MsConsts;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Represents a Microsoft SQL database user that can be associated with a login
 * and have specific schema, language, and encryption settings.
 */
public class MsUser extends MsAbstractStatement {

    // TODO PASSWORD, DEFAULT_LANGUAGE, ALLOW_ENCRYPTED_VALUE_MODIFICATIONS
    private String schema;
    private String login;
    private String language;
    private boolean allowEncrypted;

    /**
     * Creates a new Microsoft SQL user.
     *
     * @param name the username
     */
    public MsUser(String name) {
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
        sbSQL.append(getQuotedName());
        if (login != null) {
            sbSQL.append(" FOR LOGIN ").append(quote(login));
        }
        if (schema != null || language != null || allowEncrypted) {
            sbSQL.append(" WITH ");
            if (schema != null) {
                sbSQL.append("DEFAULT_SCHEMA = ").append(quote(schema)).append(", ");
            }
            if (language != null) {
                sbSQL.append("DEFAULT_LANGUAGE = ").append(language).append(", ");
            }
            if (allowEncrypted) {
                sbSQL.append("ALLOW_ENCRYPTED_VALUE_MODIFICATIONS = ON, ");
            }
            sbSQL.setLength(sbSQL.length() - 2);
        }
        script.addStatement(sbSQL);
        appendPrivileges(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        MsUser newUser = (MsUser) newCondition;

        StringBuilder sbSql = new StringBuilder();

        if (!Objects.equals(login, newUser.login)) {
            sbSql.append("LOGIN = ").append(quote(newUser.login)).append(", ");
        }

        String newSchema = newUser.schema;
        if (!Objects.equals(schema, newSchema)) {
            if (newSchema == null) {
                newSchema = MsConsts.DEFAULT_SCHEMA;
            }
            sbSql.append("DEFAULT_SCHEMA = ").append(quote(newSchema)).append(", ");
        }
        if (!Objects.equals(language, newUser.language)) {
            sbSql.append("DEFAULT_LANGUAGE = ")
                    .append(newUser.language == null ? "NONE" : newUser.language)
                    .append(", ");
        }
        if (!allowEncrypted == newUser.allowEncrypted) {
            sbSql.append("ALLOW_ENCRYPTED_VALUE_MODIFICATIONS = ").append(newUser.allowEncrypted ? "ON" : "OFF")
                    .append(", ");
        }

        if (!sbSql.isEmpty()) {
            sbSql.setLength(sbSql.length() - 2);
            StringBuilder sql = new StringBuilder();
            sql.append("ALTER USER ").append(getQuotedName()).append(" WITH ").append(sbSql);
            script.addStatement(sql);
        }

        alterPrivileges(newUser, script);

        return getObjectState(script, startSize);
    }

    /**
     * Sets the default schema for this user. If the schema is 'dbo', it is ignored.
     *
     * @param schema the default schema name
     */
    public void setSchema(String schema) {
        if (MsConsts.DEFAULT_SCHEMA.equals(schema)) {
            return;
        }
        this.schema = schema;
        resetHash();
    }

    public void setLogin(String login) {
        this.login = login;
        resetHash();
    }

    public void setLanguage(String defaultLng) {
        this.language = defaultLng;
        resetHash();
    }

    public void setAllowEncrypted(boolean allowEncrypted) {
        this.allowEncrypted = allowEncrypted;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(schema);
        hasher.put(login);
        hasher.put(language);
        hasher.put(allowEncrypted);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj == this) {
            return true;
        }
        return obj instanceof MsUser user
                && super.compare(user)
                && Objects.equals(schema, user.schema)
                && Objects.equals(login, user.login)
                && Objects.equals(language, user.language)
                && allowEncrypted == user.allowEncrypted;
    }

    @Override
    protected MsUser getCopy() {
        MsUser userDst = new MsUser(name);
        userDst.setSchema(schema);
        userDst.setLogin(login);
        userDst.setLanguage(language);
        userDst.setAllowEncrypted(allowEncrypted);
        return userDst;
    }
}
