# Mysql Replication Client
A small wrapper around https://github.com/shyiko/mysql-binlog-connector-java that handles parsing out Mysql row based replication events into a more easily digestible format, as well as adding in an easy to/write handler call back for handling these events.

## Example Code
The following code snippet registers a simple trace/log output ChangeSetHandler.  For any Insert/Update/Delete events that come across replication it will log the change.
```
public class Main {
    public static void main(String args[]) throws IOException {
        // Create client supplying connection credentials
        ReplicationClient client = new ReplicationClient("hostname", "user", "password", 3306);
        
        // Register any ChangeSet Handlers, this one just logs events
        client.registerChangesetHandler(new TraceChangeSetHandler());

        // Setup instance of a log position handler, these store the current position in the binlog
        // This one serializes the state to a file, and is really only here as an example.
        FileWriterLogPositionHandler logPositionHandler = new FileWriterLogPositionHandler();
        client.registerLogPositionHandler(logPositionHandler);

        // Make sure we resume reading from the binlog we left off previously
        client.setStartPosition(logPositionHandler.getBinlogFile(), logPositionHandler.getPosition());

        // Make it so
        client.go();
    }
}
```

## What does a ChangeSet look like?
```
 ChangeSet [
    databaseName='name_of_database',
    tableName='name_of_table',
    changeType=update|insert|delete,
    changeSet=[
      'fieldName'= {
        fieldName='field_name',
        fieldType=Integer|Timestamp|Boolean|Date|String|TinyInteger|Uknown,
        beforeValue='value before the change',
        afterValue='value after the change',
        wasModified=true|false,
      },
      ...
    ]
 ]
```

## Writing your own ChangeSetHandlers
ChangeSetHandlers implement a simple interface, so you can easily ETL the changes to whatever system you'd like in whatever format you'd like.
```
public interface IChangeSetHandler {
    void handle(ChangeSet changeSet);
}
```

## Writing your own LogPositionHandlers
Want to store your replication client's state to redis? Or anywhere other than a flat file?  Just implement this simple interface and register your handler.
```
public interface ILogPositionHandler {
    public void updatePosition(String binlogFilename, long position);
    public void updatePosition(long position);
    public long getPosition();
    public String getBinlogFile();
}
```
