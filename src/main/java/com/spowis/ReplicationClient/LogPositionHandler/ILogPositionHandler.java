package com.spowis.ReplicationClient.LogPositionHandler;

public interface ILogPositionHandler {
    public void updatePosition(String binlogFilename, long position);
    public void updatePosition(long position);
    public long getPosition();
    public String getBinlogFile();
}
