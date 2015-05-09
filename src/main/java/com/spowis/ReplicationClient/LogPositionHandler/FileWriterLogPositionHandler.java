package com.spowis.ReplicationClient.LogPositionHandler;

import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;

public class FileWriterLogPositionHandler implements ILogPositionHandler {
    public static final Logger log = Logger.getLogger(FileWriterLogPositionHandler.class);

    private String outputFile = "replication.status";
    private Map<String, Object> values = Maps.newHashMap();
    private ObjectOutput output = null;

    public FileWriterLogPositionHandler() {
    }

    public FileWriterLogPositionHandler(String outputFile) {
        setOutputFile(outputFile);
    }

    public void updatePosition(String binlogFilename, long position) {
        values.put("binlog", binlogFilename);
        updatePosition(position);
    }

    public void updatePosition(long position) {
        values.put("pos", position);
        writeToFile();
    }

    public long getPosition() {
        if (values.isEmpty()) {
            if (!readFromFile()) {
                return -1;
            };
        }
        return (Long) values.get("pos");
    }

    public String getBinlogFile() {
        if (values.isEmpty()) {
            if (!readFromFile()) {
                return null;
            };
        }
        return (String) values.get("binlog");
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    private boolean writeToFile() {
        if (!openOutput()) {
            return false;
        }

        try {
            output.writeObject(values);
            output.flush();
            output.close();
            output = null;
        }
        catch(IOException ex){
            log.error("Cannot perform output.", ex);
            output = null;
            return false;
        }
        return true;
    }

    private boolean readFromFile() {
        try {
            InputStream file = new FileInputStream(getOutputFile());
            InputStream buffer = new BufferedInputStream(file);
            ObjectInput input = new ObjectInputStream (buffer);

            //deserialize the List
            values = (Map<String, Object>)input.readObject();
            input.close();
        }
        catch(ClassNotFoundException ex){
            log.error("Cannot perform input. Class not found.", ex);
            return false;
        }
        catch(IOException ex){
            log.error("Cannot perform input.", ex);
            return false;
        }
        return true;
    }

    private boolean openOutput() {
        try {
            if (output == null) {
                OutputStream file = new FileOutputStream(getOutputFile());
                OutputStream buffer = new BufferedOutputStream(file);
                output = new ObjectOutputStream(buffer);
            }
        }
        catch(IOException ex){
            log.error("Cannot open output.", ex);
            output = null;
            return false;
        }
        return true;
    }
}
