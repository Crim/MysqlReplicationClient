package com.spowis.ReplicationClient.DatabaseSchema;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import java.util.HashMap;
import java.util.Map;

public class DatabaseSchemaDef {
    private Map<String, DatabaseTableDef> tableMap = new HashMap<String, DatabaseTableDef>();
    private Map<Long, String> tableIdMap = new HashMap<Long, String>();
    public DatabaseSchemaDef() {

    }

    public void addTable(DatabaseTableDef tableDef) {
        tableMap.put(tableDef.getTableName(), tableDef);
    }

    public DatabaseTableDef getTable(String tableName) {
        return tableMap.get(tableName);
    }

    public boolean updateTable(TableMapEventData data) {
        DatabaseTableDef myTable = getTable(data.getTable());
        myTable.update(data);
        tableIdMap.put(myTable.getTableId(), myTable.getTableName());
        return true;
    }

    public DatabaseTableDef getTable(Long tableId) {
        String tableName = tableIdMap.get(tableId);
        return tableMap.get(tableName);
    }

    @Override
    public String toString() {
        return "DatabaseSchemaDef{" +
                "tableMap=" + tableMap +
                '}';
    }
}
