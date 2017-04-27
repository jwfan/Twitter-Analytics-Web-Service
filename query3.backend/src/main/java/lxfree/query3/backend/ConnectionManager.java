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
    private static final String DNS1="";
    private static final String DNS2="";
    private static final String DNS3="";
    private static final String DNS4="";
    private static final String DNS5="";
    private static final String DNS6="";
    private static final String URL1 = "jdbc:mysql://" + DNS1 + DB_NAME + "?useSSL=false";
    private static final String URL2 = "jdbc:mysql://" + DNS2 + DB_NAME + "?useSSL=false";
    private static final String URL3 = "jdbc:mysql://" + DNS3 + DB_NAME + "?useSSL=false";
    private static final String URL4 = "jdbc:mysql://" + DNS4 + DB_NAME + "?useSSL=false";
    private static final String URL5 = "jdbc:mysql://" + DNS5 + DB_NAME + "?useSSL=false";
    private static final String URL6 = "jdbc:mysql://" + DNS6 + DB_NAME + "?useSSL=false";
    private static String user="";
    private static String pwd = "";
    /* jdbc connection */
	private static Connection conn1;
    private static Connection conn2;
    private static Connection conn3;
    private static Connection conn4;
    private static Connection conn5;
    private static Connection conn6;
    
    //HBase Configuration
    private static String zkAddr = "172.31.38.19";
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
        conn1 = DriverManager.getConnection(URL1, user, pwd);
        conn2 = DriverManager.getConnection(URL2, user, pwd);
        conn3 = DriverManager.getConnection(URL3, user, pwd);
        conn4 = DriverManager.getConnection(URL4, user, pwd);
        conn5 = DriverManager.getConnection(URL5, user, pwd);
        conn6 = DriverManager.getConnection(URL6, user, pwd);
    }
    
    /**
     * 
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getMySqlConnections(int choose) throws ClassNotFoundException, SQLException {
    	if(conn1 == null || conn2==null || conn3==null || conn4 == null || conn5==null || conn6 ==null) {
    		initializeConnection();
    	}
    	switch(choose){
    	case 0: return conn1;
    	case 1: return conn2;
    	//case 2: return conn3;
    	}
    	return conn1;
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
