import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ByteArrayEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.collect.Lists;
import com.pardot.ReplicationFollower.ChangeSet.ChangeSet;
import com.pardot.ReplicationFollower.ChangeSet.FieldChange;
import com.pardot.ReplicationFollower.DatabaseSchema.DatabaseColumnDef;
import com.pardot.ReplicationFollower.DatabaseSchema.DatabaseSchemaDef;
import com.pardot.ReplicationFollower.DatabaseSchema.DatabaseTableDef;
import org.apache.log4j.Logger;

import java.io.IOException;
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
                log.info("Data:"+event.getData());

                if (event.getData() instanceof QueryEventData) {
                    handleQueryEvent(event, (QueryEventData)event.getData());
                }
                if (event.getData() instanceof UpdateRowsEventData) {
                    handleQueryEvent(event, (UpdateRowsEventData)event.getData());
                }
                if (event.getData() instanceof TableMapEventData) {
                    handleQueryEvent(event, (TableMapEventData) event.getData());
                }
            }
        });
        log.info("Connecting");
        client.connect();
        log.info("Connected");
    }

    public void handleQueryEvent(Event event, QueryEventData data) {

    }

    public void handleQueryEvent(Event event, TableMapEventData data) {
        log.info("Table:"+data.getTableId()+" - " + data.getTable());
        getSchemaDef().updateTable(data);
    }

    public void handleQueryEvent(Event event, UpdateRowsEventData data) {
        long tableId = data.getTableId();
        DatabaseTableDef tableDef = getSchemaDef().getTable(tableId);
        log.info("Updated table " + tableDef.getTableName());

        log.info("Column Metadata:" + tableDef.getColumnMetadata());


        Iterator i$ = data.getRows().iterator();
        while(i$.hasNext()) {
            Map.Entry row = (Map.Entry)i$.next();

            ArrayList<Object> beforeValues = Lists.newArrayList((Object[]) row.getKey());
            ArrayList<Object> afterValues = Lists.newArrayList((Object[]) row.getValue());

            ChangeSet myChangeSet = new ChangeSet("unknown", tableDef.getTableName());

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

            log.info("Changeset:"+myChangeSet);
        }

    }
}
