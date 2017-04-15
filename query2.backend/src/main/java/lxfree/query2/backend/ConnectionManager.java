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
    // for test
    //private static final String DB_NAME = "test_db";
    private static final String DB_NAME = "q2_db";
    /* The DNS address of 3 replica mysql databases
     * remember to contain '/' at the end of the address! */
    private static final String DNS1="ec2-34-204-2-194.compute-1.amazonaws.com/";
    private static final String DNS2="ec2-54-89-228-84.compute-1.amazonaws.com/";
    private static final String DNS3="ec2-184-72-117-91.compute-1.amazonaws.com/";
    private static final String URL1 = "jdbc:mysql://" + DNS1 + DB_NAME + "?useSSL=false";
    private static final String URL2 = "jdbc:mysql://" + DNS2 + DB_NAME + "?useSSL=false";
    private static final String URL3 = "jdbc:mysql://" + DNS3 + DB_NAME + "?useSSL=false";
    private static String user="root";
    private static String pwd="CClxfreee";
    /* Connection to 3 replica mysql databases */
    private static Connection conn1;
    private static Connection conn2;
    private static Connection conn3;
    
    //HBase Configuration
    private static String zkAddr = "172.31.64.199";
    private static org.apache.hadoop.hbase.client.Connection hBaseconn;
    private static final Logger LOGGER = Logger.getRootLogger();
    
    /**
     * Initializes the connection to 3 replica mysql databases.
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
//        user=System.getProperty("user");
//        pwd=System.getProperty("passowrd");
        conn1 = DriverManager.getConnection(URL1, user, pwd);
        conn2 = DriverManager.getConnection(URL2, user, pwd);
        conn3 = DriverManager.getConnection(URL3, user, pwd);
    }
    
    /**
     * Get Mysql connection according to choose
     * @return Mysql Connection
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection(int choose) throws ClassNotFoundException, SQLException {
    	if(conn1 == null || conn2==null || conn3==null) {
    		initializeConnection();
    	}
    	switch(choose){
    	case 0: return conn1;
    	case 1: return conn2;
    	case 2: return conn3;
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
    		System.out.println("Initialize HBaze Connection...");
    	}
    	return hBaseconn;
    }
    

}
