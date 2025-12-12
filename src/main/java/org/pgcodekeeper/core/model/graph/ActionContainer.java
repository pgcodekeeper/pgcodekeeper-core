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
package org.pgcodekeeper.core.model.graph;

import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;

import java.util.Objects;

/**
 * Container class used to combine actions with database objects (CREATE ALTER DROP).
 * Also stores the object that initiated the action.
 */
public class ActionContainer {
    private final IStatement oldObj;
    private final IStatement newObj;
    private final ObjectState state;
    private final IStatement starter;

    /**
     * Creates an action container with the specified objects and action.
     *
     * @param oldObj  the old version of the database object
     * @param newObj  the new version of the database object
     * @param action  the action to be performed (CREATE, ALTER, DROP)
     * @param starter the object that initiated this action
     */
    public ActionContainer(IStatement oldObj, IStatement newObj,
                           ObjectState action, IStatement starter) {
        this.oldObj = oldObj;
        this.newObj = newObj;
        this.state = action;
        this.starter = starter;
    }

    public IStatement getOldObj() {
        return oldObj;
    }

    public IStatement getNewObj() {
        return newObj;
    }

    public ObjectState getState() {
        return state;
    }

    public IStatement getStarter() {
        return starter;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        result = prime * result + ((oldObj == null) ? 0 : oldObj.hashCode());
        result = prime * result + ((newObj == null) ? 0 : newObj.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ActionContainer cont) {
            return state == cont.getState() &&
                    Objects.equals(oldObj, cont.getOldObj()) &&
                    Objects.equals(newObj, cont.getNewObj());
        }
        return false;
    }

    @Override
    public String toString() {
        return state + " " + (oldObj == null ? " " : oldObj.getName());
    }
}