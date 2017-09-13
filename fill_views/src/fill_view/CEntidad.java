package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CEntidad {
	
public static boolean loadEjecucionPresupuestaria(Connection conn,Integer ejercicio){
		
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMariaDB.connect()){
				ret = true;
				PreparedStatement pstm;
				PreparedStatement pstm1;
				ResultSet rs;

				
				
				boolean bconn =  CMariaDB.connect();
				CLogger.writeConsole("Cargando datos a mariaDB");
				if(bconn){
					ret = true;
					int rows = 0;
					int rows_total=0;
					
					
										
									
				
				
				
				
				
				
				
				
				CLogger.writeConsole("entidades");
				String query = "delete from sipro.entidad where ejercicio = " + ejercicio;
				
				pstm1 = CMariaDB.getConnection().prepareStatement(query);
				pstm1.executeUpdate();
				
				pstm1 = CMariaDB.getConnection().prepareStatement("Insert INTO sipro.entidad "
						+ "values (entidad,nombre,abreviatura,ejercicio) ");
				
				pstm = conn.prepareStatement(" select ejercicio, entidad, nombre, '' abreviatura from sicoinprod.cg_entidades " + 
								"where ejercicio="+ ejercicio + " and unidad_ejecutora = 0 "+
								"and restrictiva = 'N';" ); 

				pstm.setFetchSize(1000);
				rs = pstm.executeQuery();
				while(rs!=null && rs.next()){
					pstm1.setInt(1, rs.getInt("entidad"));
					pstm1.setString(2, rs.getString("nombre"));
					pstm1.setString(3, rs.getString("abreviatura"));
					pstm1.setInt(4, rs.getInt("ejercicio"));
					pstm1.addBatch();
					rows++;
					
					if((rows % 1000) == 0){
						pstm1.executeBatch();
						CLogger.writeConsole("Records escritos: "+rows);
					}
				}
				
				pstm1.close();
				
				rs.close();
				pstm.close();
				
				rows_total += rows;
				rows=0;
				
				
				
				
				CLogger.writeConsole("unidad_ejecutora");
				query = "delete from sipro.unidad_ejecutora where ejercicio = "+ ejercicio ;
				
				pstm1 = CMariaDB.getConnection().prepareStatement(query);
				pstm1.executeUpdate();
				
				pstm1 = CMariaDB.getConnection().prepareStatement("Insert INTO unidad_ejecutora "
						+ "values (unidad_ejecutora,nombre,entidadentidad,ejercicio) ");
				
				pstm = conn.prepareStatement("select ejercicio, entidad, unidad_ejecutora, nombre from sicoinprod.cg_entidades " +
												"where ejercicio="+ ejercicio + " and unidad_ejecutora >0 " +
												"and restrictiva = 'N';" ); 

				pstm.setFetchSize(1000);
				rs = pstm.executeQuery();
				while(rs!=null && rs.next()){
					
					pstm1.setInt(1, rs.getInt("unidad_ejecutora"));
					pstm1.setString(2, rs.getString("nombre"));
					pstm1.setInt(3, rs.getInt("entidadentidad"));
					pstm1.setInt(4, rs.getInt("ejercicio"));
					pstm1.addBatch();
					rows++;
					
					if((rows % 1000) == 0){
						pstm1.executeBatch();
						CLogger.writeConsole("Records escritos: "+rows);
					}
				}
				
				pstm1.close();
				
				rs.close();
				pstm.close();
				
				rows_total += rows;
				rows=0;
				
				
				
				
				
				CLogger.writeConsole("unidad_ejecutora 0");
				
				
				pstm1 = CMariaDB.getConnection().prepareStatement("Insert INTO unidad_ejecutora "
						+ "values (unidad_ejecutora,nombre,entidadentidad,ejercicio) ");
				
				pstm = conn.prepareStatement("select ejercicio, entidad, 0 ,nombre from sicoinprod.cg_entidades " +
								"where ejercicio="+ ejercicio + " and unidad_ejecutora = 0 " +
								"and restrictiva = 'N';" ); 

				pstm.setFetchSize(1000);
				rs = pstm.executeQuery();
				while(rs!=null && rs.next()){
					
					pstm1.setInt(1, rs.getInt("unidad_ejecutora"));
					pstm1.setString(2, rs.getString("nombre"));
					pstm1.setInt(3, rs.getInt("entidadentidad"));
					pstm1.setInt(4, rs.getInt("ejercicio"));
					pstm1.addBatch();
					rows++;
					
					if((rows % 1000) == 0){
						pstm1.executeBatch();
						CLogger.writeConsole("Records escritos: "+rows);
					}
				}
				
				pstm1.close();
				
				rs.close();
				pstm.close();
				
				rows_total += rows;
				rows=0;
				
				
				
				
				
				query = "insert into entidad (entidad,nombre,abreviatura,ejercicio) "+
						"values (0,'SIN ENTIDAD','Sin Entidad',"+ ejercicio + ");";
				
				pstm1 = CMariaDB.getConnection().prepareStatement(query);
				pstm1.executeUpdate();
				
				
				query = "insert into unidad_ejecutora (unidad_ejecutora,nombre,entidadentidad,ejercicio) " +
						"values (0,'SIN UNIDAD EJECUTORA',0,"+ ejercicio + ")";
				
				pstm1 = CMariaDB.getConnection().prepareStatement(query);
				pstm1.executeUpdate();
				
				pstm1.close();
				CLogger.writeConsole("Records escritos Totales: "+rows_total);
					
				}
				
			}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CEjecucionPresupuestaria.class", e);
		}
		finally{
			CMariaDB.close();
		}
		return ret;
	}


}
