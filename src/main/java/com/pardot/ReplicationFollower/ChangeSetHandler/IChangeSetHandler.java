package com.pardot.ReplicationFollower.ChangeSetHandler;

import com.pardot.ReplicationFollower.ChangeSet.ChangeSet;

public interface IChangeSetHandler {
    void handle(ChangeSet changeSet);
}
