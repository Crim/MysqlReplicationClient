package com.pardot.ReplicationFollower.ChangeSet;

public class FieldChange {
    private String fieldName;
    private String fieldType;
    private Object beforeValue;
    private Object afterValue;

    public FieldChange(String fieldName, String fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public FieldChange setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public String getFieldType() {
        return fieldType;
    }

    public FieldChange setFieldType(String fieldType) {
        this.fieldType = fieldType;
        return this;
    }

    public void setBeforeValue(Object beforeValue) {
        this.beforeValue = beforeValue;
    }

    public void setAfterValue(Object afterValue) {
        this.afterValue = afterValue;
    }

    public boolean wasModified() {
        if (beforeValue == null && afterValue == null) {
            return false;
        } else if (beforeValue == null && afterValue != null) {
            return true;
        };
        return beforeValue.equals(afterValue);
    }

    @Override
    public String toString() {
        return "FieldChange{" +
                "fieldName='" + fieldName + '\'' +
                ", fieldType='" + fieldType + '\'' +
                ", beforeValue=" + beforeValue +
                ", afterValue=" + afterValue +
                ", wasModified=" + wasModified() +
                '}';
    }
}
