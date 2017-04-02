package lxfree.query2.backend;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ConnectionManager {
	//Mysql configuration
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "q2_db";
    private static final String[] URLs = {"jdbc:mysql://ec2-54-242-245-123.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false",
    		"jdbc:mysql://ec2-54-209-169-13.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false"};
    private static final String DB_USER = "root";
    private static final String DB_PWD = "CClxfreee";
    private static Connection conn;
    
    //HBase Configuration
    private static String zkAddr = "172.31.79.116";
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
        Random rm = new Random();
        int i = rm.nextInt(3);
        conn = DriverManager.getConnection(URLs[i], DB_USER, DB_PWD);
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
        conf.set("zookeeper.session.timeout", "60");
        conf.set("hbase.rpc.timeout", "60");
//        conf.set("hbase.client.retries.number", "10");
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
