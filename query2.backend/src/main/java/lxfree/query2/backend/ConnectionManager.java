package lxfree.query2.backend;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ConnectionManager {
	//Mysql configuration
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "twitter_db";
    private static final String URL = "jdbc:mysql://ec2-54-204-170-186.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false";
    private static final String DB_USER = "root";
    private static final String DB_PWD = "CClxfreee";
    private static Connection conn;
    
    //HBase Configuration
    private static String zkAddr = "172.31.22.53";
    private static org.apache.hadoop.hbase.client.Connection hBaseconn;
    private static final Logger LOGGER = Logger.getRootLogger();
    
    /**
     * Initializes database connection.
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
    }
    
    /**
     * Get Mysql connection
     * @return Mysql Connection
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
    	if(conn == null) {
    		initializeConnection();
    	}
    	return conn;
    }
    
    private static void initializeHBaseConnection() throws IOException {
        // Remember to set correct log level to avoid unnecessary output.
        LOGGER.setLevel(Level.ALL);
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("Malformed HBase IP address");
            System.exit(-1);
        }
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        hBaseconn = ConnectionFactory.createConnection(conf);
    }
    
    /**
     * Get Mysql connection
     * @return HBase Connection
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException 
     */
    public static org.apache.hadoop.hbase.client.Connection getHBaseConnection() throws ClassNotFoundException, SQLException, IOException {
    	if(hBaseconn == null) {
    		initializeHBaseConnection();
    	}
    	return hBaseconn;
    }
    

}
