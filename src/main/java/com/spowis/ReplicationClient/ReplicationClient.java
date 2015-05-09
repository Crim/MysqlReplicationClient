package com.spowis.ReplicationClient;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.spowis.ReplicationClient.ChangeSet.ChangeSet;
import com.spowis.ReplicationClient.ChangeSet.ChangeType;
import com.spowis.ReplicationClient.ChangeSet.FieldChange;
import com.spowis.ReplicationClient.ChangeSetHandler.IChangeSetHandler;
import com.spowis.ReplicationClient.DatabaseSchema.DatabaseColumnDef;
import com.spowis.ReplicationClient.DatabaseSchema.DatabaseSchemaDef;
import com.spowis.ReplicationClient.DatabaseSchema.DatabaseTableDef;
import com.spowis.ReplicationClient.LogPositionHandler.ILogPositionHandler;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

public class ReplicationClient {
    public static final Logger log = Logger.getLogger(ReplicationClient.class);

    private String hostname;
    private String username;
    private String password;
    private Integer port;
    private String database;

    private DatabaseConnection dbConnection = null;
    private BinaryLogClient client = null;
    private Map<String, DatabaseSchemaDef> schemaDef = Maps.newHashMap();

    private List<IChangeSetHandler> handlers = Lists.newArrayList();
    private List<ILogPositionHandler> logPositionHandlers = Lists.newArrayList();

    public ReplicationClient(String hostname, String username, String password, Integer port) {
        setHostname(hostname);
        setUsername(username);
        setPassword(password);
        setPort(port);
        setDbConnection(new DatabaseConnection(getHostname(), getUsername(), getPassword(), getPort()));

        client = new BinaryLogClient(getHostname(), getPort(), getUsername(), getPassword());
    }

    public void registerChangesetHandler(IChangeSetHandler handler) {
        handlers.add(handler);
    }

    public void registerLogPositionHandler(ILogPositionHandler logPositionHandler) {
        logPositionHandlers.add(logPositionHandler);
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public DatabaseConnection getDbConnection() {
        return dbConnection;
    }

    public void setDbConnection(DatabaseConnection dbConnection) {
        this.dbConnection = dbConnection;
    }

    public List<IChangeSetHandler> getHandlers() {
        return handlers;
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

    public ReplicationClient setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ReplicationClient setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ReplicationClient setPassword(String password) {
        this.password = password;
        return this;
    }

    public DatabaseSchemaDef loadSchema(String database) {
        if (!dbConnection.open()) {
            log.error("Failed to open connection!");
            throw new RuntimeException("Failed to open DB Connection!");
        }
        // If we've haven't already loaded the schema before
        if (!schemaDef.containsKey(database)) {
            // load the def
            try {
                schemaDef.put(database, getDbConnection().getSchemaDef(database));
            } catch (SQLException e) {
                throw new RuntimeException("Failed to load schema for database "+database, e);
            }
        }
        return schemaDef.get(database);
    }

    public void go() throws IOException {
        // Setup binary log client
        client.registerEventListener(new BinaryLogClient.EventListener() {
            public void onEvent(Event event) {
                log.info("Got EventType " + event.getHeader().getEventType().toString());

                // If its a table map event
                if (event.getHeader().getEventType().equals(EventType.TABLE_MAP)) {
                    parseTableMapEvent(event, (TableMapEventData) event.getData());
                    return;
                }  if (event.getHeader().getEventType() == EventType.ROTATE) {
                    EventData eventData = event.getData();
                    RotateEventData rotateEventData;
                    if (eventData instanceof EventDeserializer.EventDataWrapper) {
                        rotateEventData = (RotateEventData) ((EventDeserializer.EventDataWrapper) eventData).getInternal();
                    } else {
                        rotateEventData = (RotateEventData) eventData;
                    }
                    log.info("Binlog file:" + rotateEventData.getBinlogFilename()+ " position:" + rotateEventData.getBinlogPosition());
                    handleLogPosition(rotateEventData.getBinlogFilename(), rotateEventData.getBinlogPosition());
                    //binlogFilename = rotateEventData.getBinlogFilename();
                    //binlogPosition = rotateEventData.getBinlogPosition();
                } else if (event.getHeader() instanceof EventHeaderV4) {
                    EventHeaderV4 trackableEventHeader = (EventHeaderV4) event.getHeader();
                    long nextBinlogPosition = trackableEventHeader.getNextPosition();
                    if (nextBinlogPosition > 0) {
                       // binlogPosition = nextBinlogPosition;
                        log.info("Binlog Position: "+trackableEventHeader.getNextPosition());
                        handleLogPosition(trackableEventHeader.getNextPosition());
                    }
                }

                List<ChangeSet> changeSets = null;
                if (event.getHeader().getEventType().equals(EventType.EXT_UPDATE_ROWS)) {
                    changeSets = parseUpdateEvent(event, (UpdateRowsEventData) event.getData());
                } else if (event.getHeader().getEventType().equals(EventType.EXT_WRITE_ROWS)) {
                    changeSets = parseInsertEvent(event, (WriteRowsEventData)event.getData());
                } else if (event.getHeader().getEventType().equals(EventType.EXT_DELETE_ROWS)) {
                    changeSets = parseDeleteEvent(event, (DeleteRowsEventData) event.getData());
                }

                // If we have any changesets
                if (changeSets != null && changeSets.size() > 0) {
                    // Handle them
                    handleChangeSets(changeSets);
                }
            }
        });
        client.connect();
    }

    private void handleChangeSets(List<ChangeSet> changeSets) {
        for (IChangeSetHandler handler : getHandlers()) {
            for (ChangeSet changeSet: changeSets) {
                handler.handle(changeSet);
            }
        }
    }

    private void handleLogPosition(long position) {
        for (ILogPositionHandler handler: logPositionHandlers) {
            handler.updatePosition(position);
        }
    }

    private void handleLogPosition(String binlogFilename, long position) {
        for (ILogPositionHandler handler: logPositionHandlers) {
            handler.updatePosition(binlogFilename, position);
        }
    }

    private List<ChangeSet> parseDeleteEvent(Event event, DeleteRowsEventData data) {
        long tableId = data.getTableId();
        DatabaseTableDef tableDef = loadSchema(getDatabase()).getTable(tableId);
        log.info("Deleting from table " + tableDef.getTableName());

        List<ChangeSet> changeSets = Lists.newArrayList();

        Iterator i$ = data.getRows().iterator();
        while(i$.hasNext()) {
            Serializable[] row = (Serializable[])i$.next();
            ArrayList<Object> beforeValues = Lists.newArrayList((Object[]) row);

            ChangeSet myChangeSet = new ChangeSet(getDatabase(), tableDef.getTableName(), ChangeType.DELETE);

            // Builder before values
            for (int x=0; x<beforeValues.size(); x++) {
                int columnId = data.getIncludedColumns().nextSetBit(x);
                DatabaseColumnDef columnDef = tableDef.getColumn(columnId);

                Object after = beforeValues.get(x);

                FieldChange fieldChange = new FieldChange(columnDef.getColumnName(), columnDef.getColumnTypeName());
                fieldChange.setBeforeValue(after);
                myChangeSet.addFieldChange(fieldChange);
            }
            changeSets.add(myChangeSet);
        }

        return changeSets;
    }

    public List<ChangeSet> parseQueryEvent(Event event, QueryEventData data) {
        return Lists.newArrayList();
    }

    private List<ChangeSet> parseInsertEvent(Event event, WriteRowsEventData data) {
        long tableId = data.getTableId();
        DatabaseTableDef tableDef = loadSchema(getDatabase()).getTable(tableId);
        log.info("Inserting table " + tableDef.getTableName());
        List<ChangeSet> changeSets = Lists.newArrayList();

        Iterator i$ = data.getRows().iterator();
        while(i$.hasNext()) {
            Serializable[] row = (Serializable[])i$.next();
            ArrayList<Object> afterValues = Lists.newArrayList((Object[]) row);

            ChangeSet myChangeSet = new ChangeSet(getDatabase(), tableDef.getTableName(), ChangeType.UPDATE);

            // Builder after values
            for (int x=0; x<afterValues.size(); x++) {
                int columnId = data.getIncludedColumns().nextSetBit(x);
                DatabaseColumnDef columnDef = tableDef.getColumn(columnId);

                Object after = afterValues.get(x);

                FieldChange fieldChange = new FieldChange(columnDef.getColumnName(), columnDef.getColumnTypeName());
                fieldChange.setAfterValue(after);
                myChangeSet.addFieldChange(fieldChange);
            }
            changeSets.add(myChangeSet);
        }
        return changeSets;
    }

    public void parseTableMapEvent(Event event, TableMapEventData data) {
        log.info("Updating TableMap: " + data.getDatabase() + "." + data.getTable());

        // Load schema for this database
        loadSchema(data.getDatabase()).updateTable(data);
        setDatabase(data.getDatabase());
    }

    public List<ChangeSet> parseUpdateEvent(Event event, UpdateRowsEventData data) {
        long tableId = data.getTableId();
        DatabaseTableDef tableDef = loadSchema(getDatabase()).getTable(tableId);
        log.info("Updated table " + tableDef.getTableName());
        List<ChangeSet> changeSets = Lists.newArrayList();

        Iterator i$ = data.getRows().iterator();
        while(i$.hasNext()) {
            Map.Entry row = (Map.Entry)i$.next();

            ArrayList<Object> beforeValues = Lists.newArrayList((Object[]) row.getKey());
            ArrayList<Object> afterValues = Lists.newArrayList((Object[]) row.getValue());

            ChangeSet myChangeSet = new ChangeSet(getDatabase(), tableDef.getTableName(), ChangeType.UPDATE);

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
            changeSets.add(myChangeSet);
        }
        return changeSets;
    }

    public void setStartPosition(String binlogFile, long position) {
        log.info("Setting start binlog file: "+ binlogFile+ " position: "+position);
        client.setBinlogFilename(binlogFile);
        client.setBinlogPosition(position);
    }
}
