# Replication Client
A small wrapper around https://github.com/shyiko/mysql-binlog-connector-java that handles parsing out row based replication events into a more easily digestible format, as well as adding in an easily pluggable handler call back for handling these events.

## Example Code
The following code snippet registers a simple trace/log output ChangeSetHandler.  For any Insert/Update/Delete events that come across replication it will log the change.
```
public class Main {
    public static void main(String args[]) throws IOException {
        ReplicationClient client = new ReplicationClient("hostname", "user", "password", 3306);
        client.registerChangesetHandler(new TraceChangeSetHandler());
        client.go();
    }
}
```

## What does a ChangeSet look like?
> ChangeSet [
>    databaseName='name_of_database',
>    changeType=update|insert|delete,
>    changeSet=[
>      'fieldName'= {
>        fieldName='field_name',
>        fieldType=Integer|Timestamp|Boolean|Date|String|TinyInteger|Uknown,
>        beforeValue='value before the change',
>        afterValue='value after the change',
>        wasModified=true|false,
>      },
>      ...
>    ]
> ]


## Writing your own ChangeSetHandlers
ChangeSetHandlers implement a simple interface, so you can easily ETL the changes to whatever system you'd like in whatever format you'd like.
```
public interface IChangeSetHandler {
    void handle(ChangeSet changeSet);
}
```
