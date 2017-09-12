package fill_view;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import utilities.CLogger;
import utilities.CProperties;

public class COracleDB {
	private static String driverName = "oracle.jdbc.OracleDriver";
	private static Connection connection;
	private static Statement st;
	
	public static boolean connect(){
		try {
	        Class.forName(driverName);
	        connection = DriverManager.getConnection(
	        		String.join("", "jdbc:oracle:thin:@",CProperties.getOracle_host(),":",CProperties.getOracle_port(),":",CProperties.getOracle_database())
	        		, CProperties.getOracle_user(), CProperties.getOracle_password());
	        return !connection.isClosed();
		} catch (Exception e) {
			CLogger.writeFullConsole("Error 1: CHive.class", e);
	    }
	    return false;
	}
	
	
	
	
	public static Connection getConnection(){
		return connection;
	}
	
	public static void close(){
		try{
			connection.close();
		}
		catch(Exception e) { 
			CLogger.writeFullConsole("Error 4: CHive.class", e);
		}
	}
	
	public static void close(Connection conn){
		try {
			conn.close();
		} catch (Exception e) {
			CLogger.writeFullConsole("Error 5: CHive.class", e);
		}
	}
	
	public static ResultSet runQuery(String query){
		ResultSet ret=null;
		try{
			if(!connection.isClosed()){
				st = connection.createStatement();
				ret = st.executeQuery(query);
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 6: CHive.class", e);
		}
		return ret;
	}
	
	public static Integer countQuery(String query){
		Integer ret=null;
		try{
			if(!connection.isClosed()){
				st = connection.createStatement();
				ResultSet result = st.executeQuery(query);
				if(result.next()){
					ret = result.getInt(1);
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 7: CHive.class", e);
		}
		return ret;
	}

}
