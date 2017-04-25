package query2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager {
	/**Mysql configuration */
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "q2_db";
    /** The DNS address of 3 replica mysql databases
     * remember to contain '/' at the end of the address! */
    private static final String DNS1="ec2-54-234-104-41.compute-1.amazonaws.com/";
    private static final String DNS2="ec2-54-159-56-149.compute-1.amazonaws.com/";
    private static final String URL1 = "jdbc:mysql://" + DNS1 + DB_NAME + "?useSSL=false";
    private static final String URL2 = "jdbc:mysql://" + DNS2 + DB_NAME + "?useSSL=false";
    private static String user="root";
    private static String pwd="CClxfreee";
    /** Connection to 3 replica mysql databases */
    private static Connection conn1;
    private static Connection conn2;
    
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
    }
    
    /**
     * Get Mysql connection according to choose
     * @return Mysql Connection
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection(int choose) throws ClassNotFoundException, SQLException {
    	if(conn1 == null || conn2==null) {
    		initializeConnection();
    	}
    	switch(choose){
    	case 0: return conn1;
    	case 1: return conn2;
    	}
    	return conn1;
    }

}
