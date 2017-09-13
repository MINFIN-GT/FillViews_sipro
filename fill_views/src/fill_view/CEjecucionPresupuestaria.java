package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


import utilities.CLogger;

public class CEjecucionPresupuestaria {
	
	public static boolean loadEjecucionPresupuestaria(Connection conn,Integer ejercicio){
		
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMariaDB.connect()){
				ret = true;
				PreparedStatement pstm;
				PreparedStatement pstm1;
				ResultSet rs;

				pstm1 = CMariaDB.getConnection().prepareStatement("TRUNCATE TABLE sipro.ejecucion_presupuestaria");
				pstm1.executeUpdate();
				
				boolean bconn =  CMariaDB.connect();
				CLogger.writeConsole("Cargando datos a mariaDB");
				if(bconn){
					ret = true;
					int rows = 0;
					int rows_total=0;
					CLogger.writeConsole("Cargando ejecucion_presupuestaria");
					pstm1 = CMariaDB.getConnection().prepareStatement("Insert INTO sipro.ejecucion_presupuestaria "
							+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");
					 
					
					pstm = conn.prepareStatement("select gh.ejercicio ejercicio,month(gh.fec_aprobado) mes, gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma, gd.proyecto, gd.actividad, gd.obra, gd.renglon, r.nombre renglon_nombre, gd.fuente, " +       
									"gd.renglon - (gd.renglon%100) grupo, gg.nombre grupo_nombre, gd.renglon - (gd.renglon%10) subgrupo, sg.nombre subgrupo_nombre,  " +
									"sum(gd.monto_renglon) ejecucion_presupuestaria, gd.organismo, gd.correlativo         " +
									"from sicoinprod.eg_gastos_hoja gh, sicoinprod.eg_gastos_detalle gd, " +        
									"sicoinprod.cp_grupos_gasto gg, sicoinprod.cp_objetos_gasto sg, sicoinprod.cp_objetos_gasto r " +     
									"where gh.ejercicio = gd.ejercicio            " +
									"and gh.entidad = gd.entidad          " +
									"and gh.unidad_ejecutora = gd.unidad_ejecutora " +         
									"and gh.no_cur = gd.no_cur          " +
									"and gh.clase_registro IN ('DEV', 'CYD', 'RDP', 'REG') " +         
									"and gh.estado = 'APROBADO'          " +
									"and gg.ejercicio =  gh.ejercicio  " +
									"and gg.grupo_gasto = (gd.renglon-(gd.renglon%100)) " +     
									"and sg.ejercicio = gh.ejercicio       " +
									"and sg.renglon = (gd.renglon - (gd.renglon%10)) " +          
									"and r.ejercicio = gh.ejercicio      " +
									"and r.renglon = gd.renglon      " +
									"and gd.fuente = 52 " +
									//"and gd.organismo = 402 " +
									//"and gd.correlativo = 104 " +
									(ejercicio!=null ? ("and gd.ejercicio = " + ejercicio) : "") +
									"group by gh.ejercicio, month(gh.fec_aprobado), gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma, " +         
									"gd.proyecto, gd.actividad, gd.obra, gg.nombre, sg.nombre, r.nombre, gd.renglon, gd.fuente, gd.geografico, gd.organismo, gd.correlativo " ); 
						//pstm.setInt(1, i);
						//pstm.setInt(2, i);
						pstm.setFetchSize(1000);
						rs = pstm.executeQuery();
						while(rs!=null && rs.next()){
							
							pstm1.setInt(1, rs.getInt("ejercicio"));
							pstm1.setInt(2, rs.getInt("mes"));
							pstm1.setInt(3, rs.getInt("entidad"));
							pstm1.setInt(4, rs.getInt("unidad_ejecutora"));
							pstm1.setInt(5, rs.getInt("programa"));
							pstm1.setInt(6, rs.getInt("subprograma"));
							pstm1.setInt(7, rs.getInt("proyecto"));
							pstm1.setInt(8, rs.getInt("actividad"));
							pstm1.setInt(9, rs.getInt("obra"));
							pstm1.setInt(10, rs.getInt("renglon"));
							pstm1.setString(11, rs.getString("renglon_nombre"));
							pstm1.setInt(12, rs.getInt("fuente"));
							pstm1.setInt(13, rs.getInt("grupo"));
							pstm1.setString(14, rs.getString("grupo_nombre"));
							pstm1.setInt(15, rs.getInt("subgrupo"));
							pstm1.setString(16, rs.getString("subgrupo_nombre"));
							
							pstm1.setDouble(17, rs.getDouble("ejecucion_presupuestaria"));
							pstm1.setInt(18, rs.getInt("organismo"));
							pstm1.setInt(19, rs.getInt("correlativo"));
							
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
					rs.close();
					pstm.close();
					
					pstm1.close();
					rows = 0;
					
					
					CLogger.writeConsole("mv_ep_prestamo");
					String query = "drop tables if exists sipro.mv_ep_prestamo;";
					pstm1 = CMariaDB.getConnection().prepareStatement(query);
					pstm1.executeUpdate();
					
					query = String.join(" ","",
						"create table sipro.mv_ep_prestamo" ,
							"select t1.ejercicio,t1.fuente,t1.organismo,t1.correlativo,",
						"sum(case",
						  "when t1.mes = 1 then t1.ejecucion",
						  "else null",
						"end) enero,",
						"sum(case",
						  "when t1.mes = 2 then t1.ejecucion",
						  "else null",
						"end) febrero,",
						"sum(case",
						  "when t1.mes = 3 then t1.ejecucion",
						  "else null",
						"end) marzo,",
						"sum(case",
						  "when t1.mes = 4 then t1.ejecucion",
						  "else null",
						"end )abril,",
						"sum(case",
						  "when t1.mes = 5 then t1.ejecucion",
						  "else null",
						"end )mayo,",
						"sum(case",
						  "when t1.mes = 6 then t1.ejecucion",
						  "else null",
						"end )junio,",
						"sum(case",
						  "when t1.mes = 7 then t1.ejecucion",
						  "else null",
						"end )julio,",
						"sum(case",
						  "when t1.mes = 8 then t1.ejecucion",
						  "else null",
						"end )agosto,",
						"sum(case",
						  "when t1.mes = 9 then t1.ejecucion",
						  "else null",
						"end )septiembre,",
						"sum(case",
						  "when t1.mes = 10 then t1.ejecucion",
						  "else null",
						"end )octubre,",
						"sum(case",
						  "when t1.mes = 11 then t1.ejecucion",
						  "else null",
						"end )noviembre,",
						"sum(case",
						  "when t1.mes = 12 then t1.ejecucion",
						  "else null",
						"end ) diciembre",
						"from",
						"(",
						  "select ejercicio, mes, fuente,organismo,correlativo, sum(ejecucion_presupuestaria ) as ejecucion",
						  "from sipro.ejecucion_presupuestaria",
						  "group by ejercicio, mes, fuente,organismo,correlativo",
						") t1",
						"group by ejercicio,fuente,organismo,correlativo;");
					
					
					
					
					pstm1 = CMariaDB.getConnection().prepareStatement(query);
					pstm1.executeUpdate();

					pstm1.close();
					
					CLogger.writeConsole("Records escritos: "+rows);
					
					rows_total += rows;
					rows=0;
					
					
					
					
					CLogger.writeConsole("mv_ep_estructura");
					query = "drop tables if exists sipro.mv_ep_estructura;";
					
					pstm1 = CMariaDB.getConnection().prepareStatement(query);
					pstm1.executeUpdate();
					
					query = String.join(" ", "",
							"create table sipro.mv_ep_estructura" ,
							"select t1.ejercicio,t1.fuente,t1.organismo,t1.correlativo,",
							"t1.programa, t1.subprograma,t1.proyecto,t1.actividad,t1.obra,",
							"sum(case",
							  "when t1.mes = 1 then t1.ejecucion_presupuestaria",
							  "else null",
							"end) enero,",
							"sum(case",
							  "when t1.mes = 2 then t1.ejecucion_presupuestaria",
							  "else null",
							"end) febrero,",
							"sum(case",
							  "when t1.mes = 3 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )marzo,",
							"sum(case",
							  "when t1.mes = 4 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )abril,",
							"sum(case",
							  "when t1.mes = 5 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )mayo,",
							"sum(case",
							  "when t1.mes = 6 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )junio,",
							"sum(case",
							  "when t1.mes = 7 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )julio,",
							"sum(case",
							  "when t1.mes = 8 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )agosto,",
							"sum(case",
							  "when t1.mes = 9 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )septiembre,",
							"sum(case",
							  "when t1.mes = 10 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )octubre,",
							"sum(case",
							  "when t1.mes = 11 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )noviembre,",
							"sum(case",
							  "when t1.mes = 12 then t1.ejecucion_presupuestaria",
							  "else null",
							"end )diciembre",
							"from",
							"(   select ejercicio, mes, fuente,organismo,correlativo,",
								"programa, subprograma,proyecto,actividad,obra,",
								"sum(ejecucion_presupuestaria) as ejecucion_presupuestaria",
								"from sipro.ejecucion_presupuestaria",
							    "group by ejercicio, mes, fuente,organismo,correlativo, programa, subprograma,proyecto,actividad,obra",
							") t1",
							"group by t1.ejercicio,t1.fuente,t1.organismo,t1.correlativo,",
							"t1.programa, t1.subprograma,t1.proyecto,t1.actividad,t1.obra;");
					
				pstm1 = CMariaDB.getConnection().prepareStatement(query);
				rows = pstm1.executeUpdate();

				pstm1.close();
				
				CLogger.writeConsole("Records escritos: "+rows);
				
				rows_total += rows;
				rows=0;
				
				
				
				
				
				
				
				
				
				CLogger.writeConsole("entidades");
				query = "delete from sipro.entidad where ejercicio = 2017;";
				
				pstm1 = CMariaDB.getConnection().prepareStatement(query);
				pstm1.executeUpdate();
				
				pstm1 = CMariaDB.getConnection().prepareStatement("Insert INTO sipro.entidad "
						+ "values (entidad,nombre,abreviatura,ejercicio) ");
				
				pstm = conn.prepareStatement(" select ejercicio, entidad, nombre, '' abreviatura from sicoinprod.cg_entidades " + 
								"where ejercicio=2017 and unidad_ejecutora = 0 "+
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
				query = "delete from sipro.unidad_ejecutora where ejercicio = 2027;";
				
				pstm1 = CMariaDB.getConnection().prepareStatement(query);
				pstm1.executeUpdate();
				
				pstm1 = CMariaDB.getConnection().prepareStatement("Insert INTO unidad_ejecutora "
						+ "values (unidad_ejecutora,nombre,entidadentidad,ejercicio) ");
				
				pstm = conn.prepareStatement("select ejercicio, entidad, unidad_ejecutora, nombre from sicoinprod.cg_entidades " +
												"where ejercicio=2017 and unidad_ejecutora >0 " +
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
								"where ejercicio=2017 and unidad_ejecutora = 0 " +
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
						"values (0,'SIN ENTIDAD','Sin Entidad',2017);";
				
				pstm1 = CMariaDB.getConnection().prepareStatement(query);
				pstm1.executeUpdate();
				
				
				query = "insert into unidad_ejecutora (unidad_ejecutora,nombre,entidadentidad,ejercicio) " +
						"values (0,'SIN UNIDAD EJECUTORA',0,2017";
				
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
