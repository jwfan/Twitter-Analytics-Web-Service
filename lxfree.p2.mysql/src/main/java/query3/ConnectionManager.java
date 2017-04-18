package query3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
    public static Connection getMySqlConnections() throws ClassNotFoundException, SQLException {
    	if(conn1 == null || conn2==null || conn3==null || conn4 == null || conn5==null || conn6 ==null) {
    		initializeConnection();
    	}
    	return conn1;
    }
    

}
