package com.pardot.ReplicationFollower.DatabaseSchema;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseTableDef {

    public static final Logger log = Logger.getLogger(DatabaseTableDef.class);

    private String tableName;
    private long tableId;
    private byte[] columnTypes;

    private Map<String, DatabaseColumnDef> columnMap = new HashMap<String, DatabaseColumnDef>();
    private int[] columnMetadata;
    private BitSet columnNullability;

    public DatabaseTableDef(String tableName) {
        this.tableName = tableName;
    }

    public void addColumn(DatabaseColumnDef columnDef) {
        columnMap.put(columnDef.getColumnName(), columnDef);
    }

    public String getTableName() {
        return tableName;
    }

    public long getTableId() { return tableId; }

    public DatabaseColumnDef getColumn(Integer columnId) {
        for (DatabaseColumnDef columnDef: columnMap.values()) {
            if (columnId.equals(columnDef.getColumnId())) {
                return columnDef;
            }
        }
        return null;
    }

    public DatabaseColumnDef getColumn(String columnName) {
        return columnMap.get(columnName);
    }

    public void update(TableMapEventData data) {
        this.tableId = data.getTableId();
        this.columnTypes = data.getColumnTypes();
        this.columnMetadata = data.getColumnMetadata();
        this.columnNullability = data.getColumnNullability();
    }

    public int[] getColumnMetadata() {
        return columnMetadata;
    }

    @Override
    public String toString() {
        return "DatabaseTableDef{" +
                "tableName='" + tableName + '\'' +
                ", columnMap=" + columnMap +
                '}';
    }


}
