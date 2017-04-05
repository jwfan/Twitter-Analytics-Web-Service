package lxfree.query3.backend;

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
    private static final String DB_NAME = "q3_db";
    /* The DNS address of the sub-databases
     * remember to contain '/' at the end of the address! */
    private static final String DNS1_1="";
    private static final String DNS2_1="";
    private static final String DNS1_2="";
    private static final String DNS2_2="";
    private static final String DNS1_3="";
    private static final String DNS2_3="";
    private static final String URL1_1 = "jdbc:mysql://" + DNS1_1 + DB_NAME + "?useSSL=false";
    private static final String URL1_2 = "jdbc:mysql://" + DNS1_2 + DB_NAME + "?useSSL=false";
    private static final String URL2_1 = "jdbc:mysql://" + DNS2_1 + DB_NAME + "?useSSL=false";
    private static final String URL2_2 = "jdbc:mysql://" + DNS2_2 + DB_NAME + "?useSSL=false";
    private static final String URL1_3 = "jdbc:mysql://" + DNS2_3 + DB_NAME + "?useSSL=false";
    private static final String URL2_3 = "jdbc:mysql://" + DNS2_3 + DB_NAME + "?useSSL=false";
    private static String user="";
    private static String pwd = "";
    /* jdbc connection */
	private static Connection conn1_1;
    private static Connection conn2_1;
    private static Connection conn1_2;
    private static Connection conn2_2;
    private static Connection conn1_3;
    private static Connection conn2_3;
    
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
        /* Initialize jdbc connections */
        user=System.getProperty("user");
        pwd=System.getProperty("passowrd");
        conn1_1 = DriverManager.getConnection(URL1_1, user, pwd);
        conn2_1 = DriverManager.getConnection(URL2_1, user, pwd);
        conn1_2 = DriverManager.getConnection(URL1_2, user, pwd);
        conn2_2 = DriverManager.getConnection(URL2_2, user, pwd);
        conn1_3 = DriverManager.getConnection(URL1_3, user, pwd);
        conn2_3 = DriverManager.getConnection(URL2_3, user, pwd);
    }
    
    /**
     * Get Mysql connection
     * @return Mysql Connection
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection(int choose) throws ClassNotFoundException, SQLException {
    	if(conn1_1 == null || conn2_1==null || conn1_2 == null || conn2_2==null || conn1_3 == null || conn2_3==null) {
    		initializeConnection();
    	}
    	switch(choose){
    	case 0:return conn1_1;
    	case 1:return conn2_1;
    	case 2:return conn1_2;
    	case 3:return conn2_2;
    	case 4:return conn1_3;
    	case 5:return conn2_3;
    	default:return conn1_1;
    	}
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
