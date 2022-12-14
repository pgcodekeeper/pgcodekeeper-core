package ru.taximaxim.codekeeper.core.schema;

import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;

/**
 * Stores trigger information.
 */
public abstract class AbstractTrigger extends PgStatementWithSearchPath {

    @Override
    public DbObjType getStatementType() {
        return DbObjType.TRIGGER;
    }

    public AbstractTrigger(String name) {
        super(name);
    }

    @Override
    public AbstractTrigger shallowCopy() {
        AbstractTrigger triggerDst = getTriggerCopy();
        copyBaseFields(triggerDst);
        return triggerDst;
    }

    protected abstract AbstractTrigger getTriggerCopy();

    @Override
    public AbstractSchema getContainingSchema() {
        return (AbstractSchema) getParent().getParent();
    }
}
