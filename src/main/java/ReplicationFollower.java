import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ByteArrayEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.collect.Lists;
import com.pardot.ReplicationFollower.ChangeSet.ChangeSet;
import com.pardot.ReplicationFollower.ChangeSet.ChangeType;
import com.pardot.ReplicationFollower.ChangeSet.FieldChange;
import com.pardot.ReplicationFollower.DatabaseSchema.DatabaseColumnDef;
import com.pardot.ReplicationFollower.DatabaseSchema.DatabaseSchemaDef;
import com.pardot.ReplicationFollower.DatabaseSchema.DatabaseTableDef;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Types;
import java.util.*;

public class ReplicationFollower {
    public static final Logger log = Logger.getLogger(ReplicationFollower.class);

    private String hostname;
    private String username;
    private String password;
    private String database;

    private DatabaseConnection dbConnection = null;
    private DatabaseSchemaDef schemaDef = null;

    private TableMapEventData lastTableMap = null;

    public ReplicationFollower(String hostname, String username, String password, String database) {
        setHostname(hostname);
        setUsername(username);
        setPassword(password);
        setDatabase(database);

        dbConnection = new DatabaseConnection(getHostname(), getUsername(), getPassword(), getDatabase());
    }

    public DatabaseSchemaDef getSchemaDef() {
        return schemaDef;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getHostname() {
        return hostname;
    }

    public ReplicationFollower setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ReplicationFollower setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ReplicationFollower setPassword(String password) {
        this.password = password;
        return this;
    }

    public void loadSchema() {
        if (!dbConnection.open()) {
            log.error("Failed to open connection!");
            return;
        }
        schemaDef = dbConnection.getSchemaDef();
    }

    public void go() throws IOException {

        BinaryLogClient client = new BinaryLogClient(getHostname(), 3306, getUsername(), getPassword());

        client.registerEventListener(new BinaryLogClient.EventListener() {

            public void onEvent(Event event) {
                log.info("Got EventType:" + event.getHeader().getEventType());
                //log.info("Data:"+event.getData());

                if (event.getHeader().getEventType().equals(EventType.EXT_UPDATE_ROWS)) {
                    handleUpdateEvent(event, (UpdateRowsEventData) event.getData());
                } else if (event.getHeader().getEventType().equals(EventType.TABLE_MAP)) {
                    handleTableMapEvent(event, (TableMapEventData) event.getData());
                } else if (event.getHeader().getEventType().equals(EventType.EXT_WRITE_ROWS)) {
                    handleInsertEvent(event, (WriteRowsEventData)event.getData());
                } else if (event.getHeader().getEventType().equals(EventType.EXT_DELETE_ROWS)) {
                    handleDeleteEvent(event, (DeleteRowsEventData) event.getData());
                }

            }
        });
        client.connect();
    }

    private void handleDeleteEvent(Event event, DeleteRowsEventData data) {
        long tableId = data.getTableId();
        DatabaseTableDef tableDef = getSchemaDef().getTable(tableId);
        log.info("Deleting from table " + tableDef.getTableName());

        Iterator i$ = data.getRows().iterator();
        while(i$.hasNext()) {
            Serializable[] row = (Serializable[])i$.next();
            ArrayList<Object> beforeValues = Lists.newArrayList((Object[]) row);

            ChangeSet myChangeSet = new ChangeSet("unknown", tableDef.getTableName(), ChangeType.DELETE);

            // Builder before values
            for (int x=0; x<beforeValues.size(); x++) {
                int columnId = data.getIncludedColumns().nextSetBit(x);
                DatabaseColumnDef columnDef = tableDef.getColumn(columnId);

                Object after = beforeValues.get(x);

                FieldChange fieldChange = new FieldChange(columnDef.getColumnName(), columnDef.getColumnTypeName());
                fieldChange.setBeforeValue(after);
                myChangeSet.addFieldChange(fieldChange);
            }
            log.info("Deleted:"+myChangeSet);
        }
    }

    public void handleQueryEvent(Event event, QueryEventData data) {

    }

    private void handleInsertEvent(Event event, WriteRowsEventData data) {
        long tableId = data.getTableId();
        DatabaseTableDef tableDef = getSchemaDef().getTable(tableId);
        log.info("Inserting table " + tableDef.getTableName());

        Iterator i$ = data.getRows().iterator();
        while(i$.hasNext()) {
            Serializable[] row = (Serializable[])i$.next();
            ArrayList<Object> afterValues = Lists.newArrayList((Object[]) row);

            ChangeSet myChangeSet = new ChangeSet("unknown", tableDef.getTableName(), ChangeType.UPDATE);

            // Builder after values
            for (int x=0; x<afterValues.size(); x++) {
                int columnId = data.getIncludedColumns().nextSetBit(x);
                DatabaseColumnDef columnDef = tableDef.getColumn(columnId);

                Object after = afterValues.get(x);

                FieldChange fieldChange = new FieldChange(columnDef.getColumnName(), columnDef.getColumnTypeName());
                fieldChange.setAfterValue(after);
                myChangeSet.addFieldChange(fieldChange);
            }
            log.info("Inserted:"+myChangeSet);
        }
    }

    public void handleTableMapEvent(Event event, TableMapEventData data) {
        log.info("Updating TableMap: " + data.getTable());
        getSchemaDef().updateTable(data);
    }

    public void handleUpdateEvent(Event event, UpdateRowsEventData data) {
        long tableId = data.getTableId();
        DatabaseTableDef tableDef = getSchemaDef().getTable(tableId);
        log.info("Updated table " + tableDef.getTableName());

        Iterator i$ = data.getRows().iterator();
        while(i$.hasNext()) {
            Map.Entry row = (Map.Entry)i$.next();

            ArrayList<Object> beforeValues = Lists.newArrayList((Object[]) row.getKey());
            ArrayList<Object> afterValues = Lists.newArrayList((Object[]) row.getValue());

            ChangeSet myChangeSet = new ChangeSet("unknown", tableDef.getTableName(), ChangeType.UPDATE);

            // Build before values
            for (int x=0; x<beforeValues.size(); x++) {
                int columnId = data.getIncludedColumnsBeforeUpdate().nextSetBit(x);
                DatabaseColumnDef columnDef = tableDef.getColumn(columnId);

                Object before = beforeValues.get(x);

                FieldChange fieldChange = new FieldChange(columnDef.getColumnName(), columnDef.getColumnTypeName());
                fieldChange.setBeforeValue(before);

                // Add it
                myChangeSet.addFieldChange(fieldChange);
            }

            // Builder after values
            for (int x=0; x<afterValues.size(); x++) {
                int columnId = data.getIncludedColumns().nextSetBit(x);
                DatabaseColumnDef columnDef = tableDef.getColumn(columnId);

                Object after = afterValues.get(x);

                FieldChange fieldChange = myChangeSet.getFieldChange(columnDef.getColumnName());
                if (fieldChange == null) {
                    fieldChange = new FieldChange(columnDef.getColumnName(), columnDef.getColumnTypeName());
                }
                fieldChange.setAfterValue(after);
                myChangeSet.addFieldChange(fieldChange);
            }

            log.info("Updated:"+myChangeSet);
        }
    }
}
