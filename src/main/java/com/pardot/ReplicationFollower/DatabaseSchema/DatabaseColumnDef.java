package com.pardot.ReplicationFollower.DatabaseSchema;

import java.sql.Types;

public class DatabaseColumnDef {
    private String columnName;
    private int columnId;

    // Matches up with java.sql.Types
    private int columnType;
    private String columnClass;

    public DatabaseColumnDef(String name, int columnType, int columnId) {
        this.columnName = name;
        this.columnType = columnType;
        this.columnId = columnId;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public int getColumnId() {
        return columnId;
    }

    public int getColumnType() {
        return columnType;
    }

    @Override
    public String toString() {
        return "DatabaseColumnDef{" +
                "columnName='" + columnName + '\'' +
                ", columnId=" + columnId +
                ", columnType=" + columnType +
                '}';
    }

    public String getColumnTypeName() {
        switch (getColumnType()) {
            case Types.INTEGER:
                return "Integer";
            case Types.TIMESTAMP:
                return "Timestamp";
            case Types.BOOLEAN:
                return "Boolean";
            case Types.DATE:
                return "Date";
            case Types.VARCHAR:
                return "String";
            case Types.LONGVARCHAR:
                return "String";
            case Types.BIT:
                return "TinyInteger";
            default:
                return "Unknown ("+getColumnType()+")";
        }
    }
}
