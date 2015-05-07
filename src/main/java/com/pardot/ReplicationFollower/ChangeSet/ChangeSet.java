package com.pardot.ReplicationFollower.ChangeSet;

import com.google.common.collect.Maps;

import java.util.Map;

public class ChangeSet {
    private String tableName;
    private String databaseName;

    private Map<String, FieldChange> changeSet = Maps.newHashMap();

    public ChangeSet(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
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
                ", changeSet=" + changeSet +
                '}';
    }
}
