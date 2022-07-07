package ru.taximaxim.codekeeper.apgdiff.model.difftree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ru.taximaxim.codekeeper.apgdiff.model.difftree.IgnoredObject.AddStatus;

public class IgnoreSchemaList implements IIgnoreList {

    private final List<IgnoredObject> rules = new ArrayList<>();

    // black list (show all, hide some) by default
    private boolean isShow = true;

    public boolean isShow() {
        return isShow;
    }

    @Override
    public void setShow(boolean isShow) {
        this.isShow = isShow;
    }

    @Override
    public List<IgnoredObject> getList() {
        return Collections.unmodifiableList(rules);
    }

    public void clearList() {
        rules.clear();
    }

    @Override
    public void add(IgnoredObject rule) {
        rules.add(rule);
    }

    public boolean getNameStatus(String schema) {
        for (IgnoredObject rule : rules) {
            if (rule.match(schema)) {
                AddStatus newStatus = rule.getAddStatus();
                switch (newStatus) {
                case ADD:
                case ADD_SUBTREE:
                    return true;
                case SKIP:
                case SKIP_SUBTREE:
                    return false;
                }
            }
        }
        return isShow;
    }

    public String getListCode() {
        StringBuilder sb = new StringBuilder();
        sb.append(isShow ? "SHOW ALL\n" : "HIDE ALL\n");
        for (IgnoredObject rule : rules) {
            rule.appendRuleCode(sb).append('\n');
        }
        return sb.toString();
    }
}
