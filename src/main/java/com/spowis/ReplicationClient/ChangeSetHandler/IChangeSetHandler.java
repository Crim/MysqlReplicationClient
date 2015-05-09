package com.spowis.ReplicationClient.ChangeSetHandler;

import com.spowis.ReplicationClient.ChangeSet.ChangeSet;

public interface IChangeSetHandler {
    void handle(ChangeSet changeSet);
}
