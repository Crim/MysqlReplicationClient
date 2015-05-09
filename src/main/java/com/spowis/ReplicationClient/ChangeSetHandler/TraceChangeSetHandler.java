package com.spowis.ReplicationClient.ChangeSetHandler;

import com.spowis.ReplicationClient.ChangeSet.ChangeSet;
import org.apache.log4j.Logger;

public class TraceChangeSetHandler implements IChangeSetHandler {
    public static final Logger log = Logger.getLogger(TraceChangeSetHandler.class);
    public void handle(ChangeSet changeSet) {
        log.info("ChangeSet:"+changeSet);
    }
}
