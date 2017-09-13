package fill_view;

import java.sql.Connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.Seconds;

import utilities.CLogger;

public class CMain {
	private static Options options;
	
	static{
		options = new Options();
		options.addOption("ep", "ej_pre", false, "Ejecucion presupuestara");
		options.addOption("sg", "sigade", false, "Conexión a Sigade");
		options.addOption("eue", "entidades_ues", true, "Catalogo de entidades y ues");
	}
	
	final static  CommandLineParser parser = new DefaultParser();
	
	 public static void main(String[] args) throws Exception {
		 DateTime start = new DateTime();
		 CommandLine cline = parser.parse( options, args );
		 if (CHive.connect()){
			 Connection conn = CHive.getConnection();
			 if(cline.hasOption("ej_pre")){
				 CLogger.writeConsole("Inicio carga ejecución presupuestaria...");
				 if(CEjecucionPresupuestaria.loadEjecucionPresupuestaria(conn, null))
					 CLogger.writeConsole("Cara ejecucion presupuestara con exito");
			 }
			 if(!cline.hasOption("help")){
				 DateTime now = new DateTime();
				 CLogger.writeConsole("Tiempo total: " + Minutes.minutesBetween(start, now).getMinutes() + " minutos " + (Seconds.secondsBetween(start, now).getSeconds() % 60) + " segundos " +
				 (now.getMillis()%10) + " milisegundos ");
			 }
			 if(cline.hasOption("entidades_ues")){
				 CLogger.writeConsole("Inicio carga de catalogo de entidades y unidades_ejecutoras...");
				 Integer ejercicio = cline.getOptionValue("egch")!=null && cline.getOptionValue("eue").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("eue")) : start.getYear();
				 CEntidad.loadEntidad(conn, ejercicio);
			 }
			 CHive.close(conn);
		 }
		 if(cline.hasOption("sigade")){
			 if(COracleDB.connect()){		 Connection conn = COracleDB.getConnection();
				 CLogger.writeConsole("Inicio carga sigade...");
				 if(CSigade.loadDataSigade(conn))
					 CLogger.writeConsole("Cara de sigade con exito");
				 COracleDB.close();
			 }
		 }
	 }			 
}