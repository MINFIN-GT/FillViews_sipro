package fill_view;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


import utilities.CLogger;
import utilities.CProperties;

public class CMariaDB {
	private static Connection connection;
	private static Connection connection_analytic;
	private static String host;
	private static Integer port; 
	private static String user;
	private static String password;
	private static String schema;
	private static String schemades;
	private static String schema_analytic;
	
	private static Statement st;
	
	static {		
		host = CProperties.getMariadb_host();
		port = CProperties.getMariadb_port();
		user = CProperties.getMariadb_user();
		password = CProperties.getMariadb_password();
		schema = CProperties.getMariadb_schema();
		schemades = CProperties.getMysql_schemades();
		schema_analytic = CProperties.getMariadb_schema_analytic();
	}
	
	public static boolean connect(){
		try{
			Class.forName("org.mariadb.jdbc.Driver").newInstance();
			connection=DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+schema+"?" +
                    "user="+user+"&password="+password);
			if(!connection.isClosed())
				return true;
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1 : CMariaDB.class ", e);
		}
		return false;
	}
	
	public static boolean connectdes(){
		try{
			Class.forName("org.mariadb.jdbc.Driver").newInstance();
			connection=DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+schemades+"?" +
                    "user="+user+"&password="+password);
			if(!connection.isClosed())
				return true;
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 2 : CMariaDB.class ", e);
		}
		return false;
	}
	
	public static Connection getConnection(){
		return connection;
	}
	

	public static void close(){
		try {
			connection.close();
		} catch (SQLException e) {
			CLogger.writeFullConsole("Error 3 : CMariaDB.class ", e);
		}
	}
	
	public static long getNextID(Connection connection,String table){
		long ret = -1;
		try{
			PreparedStatement stm = connection.prepareStatement("SELECT last_id FROM uid WHERE table_name=?");
			stm.setString(1, table);
			ResultSet rs = stm.executeQuery();
			if(rs.next()){
				ret = rs.getLong("last_id");
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 4: CMariaDB.class", e);
		}
		return ret;
	}
	
	
	public static boolean saveLastID(Connection connection, String table, long last_id){
		boolean ret = false;
		try{
			PreparedStatement stm = connection.prepareStatement("UPDATE uid SET last_id=? WHERE table_name=?");
			stm.setLong(1, last_id);
			stm.setString(2, table);
			if(stm.executeUpdate()>0)
				ret = true;
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 5: CMariaDB.class", e);
		}
		return ret;
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
			CLogger.writeFullConsole("Error 6: CMariaDB.class", e);
		}
		return ret;
	}
	
	public static boolean connect_analytic(){
		try{
			Class.forName("org.mariadb.jdbc.Driver").newInstance();
			connection_analytic=DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+schema_analytic+"?" +
                    "user="+user+"&password="+password);
			if(!connection_analytic.isClosed())
				return true;
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 7 : CMariaDB.class ", e);
		}
		return false;
	}
	
	public static Connection getConnection_analytic(){
		return connection_analytic;
	}
	

	public static void close_analytic(){
		try {
			connection_analytic.close();
		} catch (SQLException e) {
			CLogger.writeFullConsole("Error 8 : CMariaDB.class ", e);
		}
	}

}
