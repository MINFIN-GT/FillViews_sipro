package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CSigade {
	public static boolean loadDataSigade(Connection conn){
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMariaDB.connect()){
				ret = true;
				
				PreparedStatement pstm;
				
				
				boolean bconn =  CMariaDB.connect_analytic();
				CLogger.writeConsole("Cargando datos a mariaDB");
				if(bconn){
					ret = true;
					int rows = 0;
					int rows_total=0;
					ResultSet rs;
					
					
					PreparedStatement pstm1 = CMariaDB.getConnection_analytic().prepareStatement("TRUNCATE TABLE sipro_analytic.dtm_avance_fisfinan_det_dti");
					pstm1.executeUpdate();
					
					
					CLogger.writeConsole("Cargando datos a dtm_avance_fisfinan_det_dti");
					
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("Insert INTO sipro_analytic.dtm_avance_fisfinan_det_dti "
							+ "values (?,?,?,?,?,?,?,?,?,?,?) ");
					
					pstm = conn.prepareStatement("select * from DTM_AVANCE_FISFINAN_DET_DTI");
					
					pstm.setFetchSize(1000);
					rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						
						pstm1.setBigDecimal(1, rs.getBigDecimal("ejercicio_fiscal"));
						pstm1.setString(2, rs.getString("mes_desembolso"));
						pstm1.setString(3, rs.getString("codigo_presupuestario"));
						pstm1.setInt(4, rs.getInt("entidad_sicoin"));
						pstm1.setInt(5, rs.getInt("unidad_ejecutora_sicoin"));
						pstm1.setString(6, rs.getString("moneda_desembolso"));
						pstm1.setBigDecimal(7, rs.getBigDecimal("desembolsos_mes_moneda"));
						pstm1.setBigDecimal(8, rs.getBigDecimal("tc_mon_usd"));
						pstm1.setBigDecimal(9, rs.getBigDecimal("desembolsos_mes_usd"));
						pstm1.setBigDecimal(10, rs.getBigDecimal("tc_usd_gtq"));
						pstm1.setBigDecimal(11, rs.getBigDecimal("desembolsos_mes_gtq"));
						
						
						pstm1.addBatch();
						rows++;
						
						if((rows % 1000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole("Records escritos: "+rows);
						}
					}
					CLogger.writeConsole("Records escritos: "+rows);
					pstm1.executeBatch();
					rows_total += rows;
					rows=0;
					
					
					CLogger.writeConsole("Cargando datos a dtm_avance_fisfinan_dti");
					
					
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("TRUNCATE TABLE sipro_analytic.dtm_avance_fisfinan_dti");
					pstm1.executeUpdate();
					
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("Insert INTO sipro_analytic.dtm_avance_fisfinan_dti "
							+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");
					
					pstm = conn.prepareStatement("select * from DTM_AVANCE_FISFINAN_DTI");
					
					pstm.setFetchSize(1000);
					rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						
						pstm1.setDate(1, rs.getDate("fecha_corte"));
						pstm1.setString(2, rs.getString("no_prestamo"));
						pstm1.setString(3, rs.getString("codigo_presupuestario"));
						pstm1.setString(4, rs.getString("nombre_programa"));
						pstm1.setInt(5, rs.getInt("codigo_organismo_finan"));
						pstm1.setString(6, rs.getString("siglas_organismo_finan"));
						pstm1.setString(7, rs.getString("nombre_organismo_finan"));
						pstm1.setString(8, rs.getString("moneda_prestamo"));
						pstm1.setBigDecimal(9, rs.getBigDecimal("monto_contratado"));
						pstm1.setBigDecimal(10, rs.getBigDecimal("monto_contratado_usd"));
						pstm1.setBigDecimal(11, rs.getBigDecimal("monto_contratado_gtq"));
						pstm1.setBigDecimal(12, rs.getBigDecimal("desembolsos"));
						pstm1.setBigDecimal(13, rs.getBigDecimal("desembolsos_usd"));
						pstm1.setBigDecimal(14, rs.getBigDecimal("desembolsos_gtq"));
						pstm1.setDate(15, rs.getDate("fecha_decreto"));
						pstm1.setDate(16, rs.getDate("fecha_suscripcion"));
						pstm1.setDate(17, rs.getDate("fecha_vigencia"));
						pstm1.setBigDecimal(18, rs.getBigDecimal("por_desembolsar"));
						pstm1.setBigDecimal(19, rs.getBigDecimal("por_desembolsar_usd"));
						pstm1.setBigDecimal(20, rs.getBigDecimal("por_desembolsar_gtq"));
						pstm1.setString(21, rs.getString("estado_prestamo"));
						pstm1.setString(22, rs.getString("objetivo"));
						
						
						pstm1.addBatch();
						rows++;
						
						if((rows % 1000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole("Records escritos: "+rows);
						}
					}
					CLogger.writeConsole("Records escritos: "+rows);
					pstm1.executeBatch();
					rows_total += rows;
					rows=0;
					
					
					
					CLogger.writeConsole("Cargando datos a dtm_avance_fisfinan_cmp");
					
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("TRUNCATE TABLE sipro_analytic.dtm_avance_fisfinan_cmp");
					pstm1.executeUpdate();
					
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("Insert INTO sipro_analytic.dtm_avance_fisfinan_cmp "
							+ "values (?,?,?,?,?) ");
					
					pstm = conn.prepareStatement("select * from DTM_AVANCE_FISFINAN_CMP");
					
					pstm.setFetchSize(1000);
					rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						
						pstm1.setString(1, rs.getString("codigo_presupuestario"));
						pstm1.setInt(2, rs.getInt("numero_componente"));
						pstm1.setString(3, rs.getString("nombre_componente"));
						pstm1.setString(4, rs.getString("moneda_componente"));
						pstm1.setBigDecimal(5, rs.getBigDecimal("monto_componente"));
						pstm1.addBatch();
						rows++;
						
						if((rows % 1000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole("Records escritos: "+rows);
						}
					}
					CLogger.writeConsole("Records escritos: "+rows);
					pstm1.executeBatch();
					rows_total += rows;
					rows=0;
					
					CLogger.writeConsole("Cargando datos a dtm_avance_fisfinan_enp");
					
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("TRUNCATE TABLE sipro_analytic.dtm_avance_fisfinan_enp");
					pstm1.executeUpdate();
					
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("Insert INTO sipro_analytic.dtm_avance_fisfinan_enp "
							+ "values (?,?,?,?) ");
					
					pstm = conn.prepareStatement("select * from DTM_AVANCE_FISFINAN_ENP");
					
					pstm.setFetchSize(1000);
					rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						
						pstm1.setString(1, rs.getString("codigo_presupuestario"));
						pstm1.setInt(2, rs.getInt("ejercicio_fiscal"));
						pstm1.setInt(3, rs.getInt("entidad_presupuestaria"));
						pstm1.setInt(4, rs.getInt("unidad_ejecutora"));
						pstm1.addBatch();
						rows++;
						
						if((rows % 1000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole("Records escritos: "+rows);
						}
					}
					CLogger.writeConsole("Records escritos: "+rows);
					pstm1.executeBatch();
					rows_total += rows;
					rows=0;
					

					
					
				
					CLogger.writeConsole("Actualizando componentes ");
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("SELECT * FROM sipro_analytic.dtm_avance_fisfinan_cmp");
					rs = pstm1.executeQuery();
					PreparedStatement pstm2 = CMariaDB.getConnection_analytic().prepareStatement("");
					while(rs!=null && rs.next()){
						String query = "update sipro.componente_sigade " +  
								"set monto_componente =  " + rs.getDouble("monto_componente") + 
								" where codigo_presupuestario = '"+ rs.getString("codigo_presupuestario") + "' " +
								" and numero_componente = " + rs.getInt("numero_componente") + 
								" and estado = 1";
						pstm2 = CMariaDB.getConnection_analytic().prepareStatement(query);
						rows = rows + pstm2.executeUpdate();
					}
					if (pstm2!= null && pstm2.isClosed())
					pstm2.close();
					
					rows_total += rows;
					rows=0;
					
					CLogger.writeConsole("\tRecords editados: "+rows);
				
					
					
					CLogger.writeConsole("Eliminando componentes que ya no existen  ");
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement("select c.* " +
								"from sipro.componente c  " +
								"where c.componente_sigadeid in ( " +
								"select cp.id  " +
								"from sipro.componente_sigade cp " +
								"left outer join sipro_analytic.dtm_avance_fisfinan_cmp cmp " +
								"on (cmp.codigo_presupuestario = cp.codigo_presupuestario " +
									"and cmp.numero_componente = cp.numero_componente) " +
								"Where cmp.codigo_presupuestario is null " +
								"and cmp.numero_componente is null ) and estado = 1 "
							);
					
					rs = pstm1.executeQuery();
					pstm2 = CMariaDB.getConnection_analytic().prepareStatement("");
					while(rs!=null && rs.next()){
						String query = "update sipro.componente " +  
								"set estado =  0 " +  
								" where componente_sigadeid = "+ rs.getString("componente_sigadeid")  ;
						pstm2 = CMariaDB.getConnection_analytic().prepareStatement(query);
						rows = rows + pstm2.executeUpdate();
						
						query = "update sipro.componente_sigade " +  
								"set estado =  0 " +  
								" where id = "+ rs.getString("id")  ;
						pstm2 = CMariaDB.getConnection_analytic().prepareStatement(query);
						rows = rows + pstm2.executeUpdate();
					}
					
					if (pstm2!= null && pstm2.isClosed())
					pstm2.close();
					
					rows_total += rows;
					rows=0;
					
					pstm1.close();
					rs.close();
					//pstm.close();
				CLogger.writeConsole("Records escritos Totales: "+rows_total);
				}
				
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CSigade.class", e);
		}
		finally{
			CMariaDB.close();
		}
		
		return ret;
	}

}
