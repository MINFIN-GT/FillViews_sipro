package fill_view;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CGuatecompras {
	
	public static boolean loadAdquisiciones(Connection conn){
		
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMariaDB.connect_analytic()){
				ret = true;
				PreparedStatement pstm;
				PreparedStatement pstm1;
				ResultSet rs;

				ret = true;
				int rows = 0;
				int rows_total=0;
				
				pstm1 = CMariaDB.getConnection_analytic().prepareStatement("TRUNCATE TABLE sipro_analytic.mv_gc_adquisiciones");
				pstm1.executeUpdate();
				
				boolean bconn =  CMariaDB.connect_analytic();
				CLogger.writeConsole("GC - Cargando datos a mariaDB");
				if(bconn){
					
					CLogger.writeConsole("\tCargando mv_gc_adquisiciones");
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("Insert INTO sipro_analytic.mv_gc_adquisiciones "
							+ "values (?,?,?,?,?,?,?,?) ");
					 
					
					pstm = conn.prepareStatement("select  c.nog_concurso, a.numero_contrato, a.monto, " +
													"c.fecha_doc_respaldo, c.fecha_publicacion, c.fecha_cro, "+
													"a.fecha_adjudicacion, a.fecha_hora_contrato " +
													"from guatecompras.gc_concurso c  " +
													"left outer join guatecompras.gc_adjudicacion a ON  ( c.nog_concurso = a.nog_concurso ) "  ); 
						pstm.setFetchSize(1000);
						rs = pstm.executeQuery();
						
						while(rs!=null && rs.next()){
							pstm1.setInt(1, rs.getInt("nog_concurso"));
							pstm1.setString(2, rs.getString("numero_contrato"));
							pstm1.setDouble(3, rs.getDouble("monto"));
							pstm1.setDate(4, rs.getTimestamp("fecha_doc_respaldo") != null &&  rs.getTimestamp("fecha_doc_respaldo").getTime() > 0 ? new Date(rs.getTimestamp("fecha_doc_respaldo").getTime()): null);
							pstm1.setDate(5, rs.getTimestamp("fecha_publicacion") != null &&  rs.getTimestamp("fecha_publicacion").getTime() > 0  ? new Date(rs.getTimestamp("fecha_publicacion").getTime()): null);
							pstm1.setDate(6, rs.getTimestamp("fecha_cro") != null &&  rs.getTimestamp("fecha_cro").getTime() > 0 &&  rs.getTimestamp("fecha_cro").getTime() < 2510451068L  ?  new Date(rs.getTimestamp("fecha_cro").getTime()): null);
							pstm1.setDate(7, rs.getTimestamp("fecha_adjudicacion") != null &&  rs.getTimestamp("fecha_adjudicacion").getTime() > 0  ?  new Date(rs.getTimestamp("fecha_adjudicacion").getTime()) : null);
							pstm1.setDate(8, rs.getTimestamp("fecha_hora_contrato") != null &&  rs.getTimestamp("fecha_hora_contrato").getTime() > 0  ? new Date(rs.getTimestamp("fecha_hora_contrato").getTime()): null);

							pstm1.addBatch();
							rows++;
							if((rows % 100000) == 0){
								pstm1.executeBatch();
								CLogger.writeConsole("\tRecords escritos: "+rows);
							}
						}
					CLogger.writeConsole("\tRecords escritos: "+rows);
					pstm1.executeBatch();
					rows_total += rows;
					rows=0;
					rs.close();
					pstm.close();
					
					pstm1.close();
					rows = 0;
				
				pstm1.close();
				rows = 0;
				CLogger.writeConsole("Records escritos Totales: "+rows_total);	
				}
			}	
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CGuatecompras.class", e);
			ret = false;
		}
		finally{
			CMariaDB.close_analytic();
		}
		return ret;
	}

}
