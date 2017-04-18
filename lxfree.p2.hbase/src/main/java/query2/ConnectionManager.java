package query2;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ConnectionManager {
    
    //HBase Configuration
    private static String zkAddr = "172.31.40.115";
    private static org.apache.hadoop.hbase.client.Connection hBaseconn;
    private static final Logger LOGGER = Logger.getRootLogger();
    
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
        conf.set("hbase.rpc.timeout", "600000");
        conf.set("hbase.client.scanner.timeout.period", "600000");
        hBaseconn = ConnectionFactory.createConnection(conf);
    }
    
    /**
     * Get Hbase connection
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
