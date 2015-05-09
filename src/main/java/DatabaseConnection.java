import com.pardot.ReplicationFollower.DatabaseSchema.DatabaseColumnDef;
import com.pardot.ReplicationFollower.DatabaseSchema.DatabaseSchemaDef;
import com.pardot.ReplicationFollower.DatabaseSchema.DatabaseTableDef;
import org.apache.log4j.Logger;

import java.sql.*;

public class DatabaseConnection {
    private String hostname;
    private String username;
    private String password;
    private Integer port;
    private Connection conn = null;

    public static final Logger log = Logger.getLogger(ReplicationClient.class);

    public DatabaseConnection(String hostname, String username, String password, Integer port) {
        setHostname(hostname);
        setUsername(username);
        setPassword(password);
        setPort(port);
    }

    public String getHostname() {
        return hostname;
    }

    public DatabaseConnection setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public DatabaseConnection setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DatabaseConnection setPassword(String password) {
        this.password = password;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public boolean open() {
        try {

        // Don't reopen
        if (getConn() != null && getConn().isValid(2)) {
            return true;
        }

        if (getConn() != null && !getConn().isClosed() ) {
            getConn().close();
        }

        setConn(
            DriverManager.getConnection("jdbc:mysql://"+getHostname()+":"+getPort()+"/?" +
                "user="+getUsername()+"&password="+getPassword())
            );
        } catch (SQLException e) {
            log.error("Failed to connect", e);
            return false;
        }
        return true;
    }

    public DatabaseSchemaDef getSchemaDef(String database) throws SQLException {
        conn.setCatalog(database);
        DatabaseSchemaDef schemaDef = new DatabaseSchemaDef();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tableResultSet = metaData.getTables(null, "public", null, new String[]{"TABLE"});
            try {
                while (tableResultSet.next()) {
                    String tableName = tableResultSet.getString("TABLE_NAME");
                    ResultSet columnResultSet = metaData.getColumns(null, "public", tableName, null);
                    DatabaseTableDef tableDef = new DatabaseTableDef(tableName);
                    schemaDef.addTable(tableDef);
                    try {
                        while (columnResultSet.next()) {
                            String columnName = columnResultSet.getString("COLUMN_NAME");
                            tableDef.addColumn(new DatabaseColumnDef(columnName, columnResultSet.getInt("DATA_TYPE"), columnResultSet.getInt("ORDINAL_POSITION")-1));
                        }
                    } finally {
                        columnResultSet.close();
                    }
                }
            } finally {
                tableResultSet.close();
            }
        } catch (SQLException e) {
            log.error("Failed to retrieve metadata", e);
        }

        log.info("Schema Def:"+schemaDef);
        return schemaDef;
    }
}
