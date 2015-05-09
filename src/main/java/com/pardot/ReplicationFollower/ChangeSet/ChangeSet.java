package com.pardot.ReplicationFollower.ChangeSet;

import com.google.common.collect.Maps;

import java.util.Map;

public class ChangeSet {
    private String tableName;
    private String databaseName;
    private ChangeType changeType;

    private Map<String, FieldChange> changeSet = Maps.newHashMap();

    public ChangeSet(String databaseName, String tableName, ChangeType changeType) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.changeType = changeType;
    }

    public void addFieldChange(FieldChange fieldChange) {
        changeSet.put(fieldChange.getFieldName(), fieldChange);
    }

    public FieldChange getFieldChange(String columnName) {
        return changeSet.get(columnName);
    }

    @Override
    public String toString() {
        return "ChangeSet{" +
                "tableName='" + tableName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", changeType=" + changeType +
                ", changeSet=" + changeSet +
                '}';
    }
}
