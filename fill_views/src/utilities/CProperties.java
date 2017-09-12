package utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class CProperties {
	private static Properties properties;
	private static String memsql_host="";
	private static Integer memsql_port=null;
	private static String memsql_user="";
	private static String memsql_password="";
	private static String memsql_schema=null;
	private static String memsql_schemades=null;
	private static String hive_host="";
	private static String hive_port="";
	private static String hive_user="";
	private static String hive_password="";
	private static String hive_database="";	
	private static String hive_databasedes="";	
	private static String mariadb_host="";
	private static Integer mariadb_port=null;
	private static String mariadb_user="";
	private static String mariadb_password=null;
	private static String mariadb_schema="";
	private static String mysql_schemades="";
	
	private static String oracle_host="";
	private static String oracle_port="";
	private static String oracle_user="";
	private static String oracle_password="";
	private static String oracle_database="";	
	
	

	static{
		InputStream input;
		properties = new Properties();
		try{
			File configfile = new File("data_loader.properties");
			if(configfile.exists()){
				input = new FileInputStream(configfile);
				
				properties.load(input);
				memsql_host = properties.getProperty("memsql_host");
				memsql_port = properties.getProperty("memsql_port") != null ? 
						Integer.parseInt(properties.getProperty("memsql_port")) : null;
				memsql_user = properties.getProperty("memsql_user");
				memsql_password = properties.getProperty("memsql_password");
				memsql_schema = properties.getProperty("memsql_schema");
				memsql_schemades = properties.getProperty("memsql_schemades");
				hive_host = properties.getProperty("hive_host");
				hive_port = properties.getProperty("hive_port");
				hive_user = properties.getProperty("hive_user");
				hive_password = properties.getProperty("hive_password");
				mariadb_host = properties.getProperty("mariadb_host");
				mariadb_port =properties.getProperty("mariadb_port") != null ? 
						Integer.parseInt(properties.getProperty("mariadb_port")) : null;
				mariadb_user=properties.getProperty("mariadb_user");
				mariadb_password=properties.getProperty("mariadb_password");
				mariadb_schema=properties.getProperty("mariadb_schema");
				
				setHive_database(properties.getProperty("hive_database"));
				setHive_databasedes(properties.getProperty("hive_databasedes"));
				
				
				oracle_host=properties.getProperty("oracle_host");;
				oracle_port=properties.getProperty("oracle_port");;
				oracle_user=properties.getProperty("oracle_user");;
				oracle_password= properties.getProperty("oracle_password");;
				oracle_database=properties.getProperty("oracle_database");;
				
				
				
				
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CProperties.class", e);
		}
		finally{
			
		}
	}

	public static Properties getProperties() {
		return properties;
	}

	public static void setProperties(Properties properties) {
		CProperties.properties = properties;
	}

	public static String getmemsql_host() {
		return memsql_host;
	}

	public static void setmemsql_host(String memsql_host) {
		CProperties.memsql_host = memsql_host;
	}

	public static Integer getmemsql_port() {
		return memsql_port;
	}

	public static void setmemsql_port(Integer memsql_port) {
		CProperties.memsql_port = memsql_port;
	}

	public static String getmemsql_user() {
		return memsql_user;
	}

	public static void setmemsql_user(String memsql_user) {
		CProperties.memsql_user = memsql_user;
	}

	public static String getmemsql_password() {
		return memsql_password;
	}

	public static void setmemsql_password(String memsql_password) {
		CProperties.memsql_password = memsql_password;
	}
	
	public static String getmemsql_schema(){
		return memsql_schema;
	}
	
	public void setmemsql_schema(String memsql_schema){
		CProperties.memsql_schema = memsql_schema;
	}
	
	public static String getmemsql_schemades(){
		return memsql_schemades;
	}
	
	public void setmemsql_schemades(String memsql_schemades){
		CProperties.memsql_schemades = memsql_schemades;
	}

	public static String getHive_host() {
		return hive_host;
	}

	public static void setHive_host(String hive_host) {
		CProperties.hive_host = hive_host;
	}

	public static String getHive_port() {
		return hive_port;
	}

	public static void setHive_port(String hive_port) {
		CProperties.hive_port = hive_port;
	}

	public static String getHive_user() {
		return hive_user;
	}

	public static void setHive_user(String hive_user) {
		CProperties.hive_user = hive_user;
	}

	public static String getHive_password() {
		return hive_password;
	}

	public static void setHive_password(String hive_password) {
		CProperties.hive_password = hive_password;
	}

	public static String getHive_database() {
		return hive_database;
	}

	public static void setHive_database(String hive_database) {
		CProperties.hive_database = hive_database;
	}
	
	public static String getHive_databasedes() {
		return hive_databasedes;
	}

	public static String getMariadb_host() {
		return mariadb_host;
	}

	public static Integer getMariadb_port() {
		return mariadb_port;
	}

	public static String getMariadb_user() {
		return mariadb_user;
	}

	public static String getMariadb_password() {
		return mariadb_password;
	}

	public static String getMariadb_schema() {
		return mariadb_schema;
	}

	public static String getMysql_schemades() {
		return mysql_schemades;
	}

	public static void setHive_databasedes(String hive_databasedes) {
		CProperties.hive_databasedes = hive_databasedes;
	}
	
	public static String getOracle_host() {
		return oracle_host;
	}

	public static void setOracle_host(String oracle_host) {
		CProperties.oracle_host = oracle_host;
	}

	public static String getOracle_port() {
		return oracle_port;
	}

	public static void setOracle_port(String oracle_port) {
		CProperties.oracle_port = oracle_port;
	}

	public static String getOracle_user() {
		return oracle_user;
	}

	public static void setOracle_user(String oracle_user) {
		CProperties.oracle_user = oracle_user;
	}

	public static String getOracle_password() {
		return oracle_password;
	}

	public static void setOracle_password(String oracle_password) {
		CProperties.oracle_password = oracle_password;
	}

	public static String getOracle_database() {
		return oracle_database;
	}

	public static void setOracle_database(String oracle_database) {
		CProperties.oracle_database = oracle_database;
	}
	
}
