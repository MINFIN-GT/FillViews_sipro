package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


import utilities.CLogger;

public class CEjecucionPresupuestaria {
	
	public static boolean loadEjecucionPresupuestaria(Connection conn,Integer ejercicio){
		
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
				
				pstm1 = CMariaDB.getConnection_analytic().prepareStatement("TRUNCATE TABLE sipro_analytic.mv_ejecucion_presupuestaria");
				pstm1.executeUpdate();
				pstm1.close();
				
				pstm1 = CMariaDB.getConnection_analytic().prepareStatement("delete from sipro_analytic.mv_ep_ejec_asig_vige " 
						+ (ejercicio!=null ? ("where ejercicio  = " + ejercicio) : ""));
																			
				pstm1.executeUpdate();
				pstm1.close();
				
				CLogger.writeConsole("Cargando datos a mariaDB");
					
				CLogger.writeConsole("Cargando mv_ejecucion_presupuestaria");
				pstm1 = CMariaDB.getConnection_analytic().prepareStatement("Insert INTO sipro_analytic.mv_ejecucion_presupuestaria "
						+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");
					 
				pstm = conn.prepareStatement("select gh.ejercicio ejercicio,month(gh.fec_aprobado) mes, gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma, gd.proyecto, gd.actividad, gd.obra, gd.renglon, r.nombre renglon_nombre, gd.fuente, " +       
						"gd.renglon - (gd.renglon%100) grupo, gg.nombre grupo_nombre, gd.renglon - (gd.renglon%10) subgrupo, sg.nombre subgrupo_nombre, gd.geografico, " +
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
						"gd.proyecto, gd.actividad, gd.obra, gg.nombre, sg.nombre, r.nombre, gd.renglon, gd.fuente, gd.geografico, gd.organismo, gd.correlativo "
						 ); 
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
						pstm1.setInt(20, rs.getInt("geografico"));
						
						pstm1.addBatch();
						rows++;
						
						if((rows % 100000) == 0){
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
					
					String query = "truncate table sipro_analytic.mv_ep_prestamo;";
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement(query);
					pstm1.executeUpdate();
					pstm1.close();
					
					query = String.join(" ",
						"Insert into sipro_analytic.mv_ep_prestamo" ,
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
						  "from sipro_analytic.mv_ejecucion_presupuestaria",
						  "group by ejercicio, mes, fuente,organismo,correlativo",
						") t1",
						"group by ejercicio,fuente,organismo,correlativo;");
					
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement(query);
					pstm1.executeUpdate();
					pstm1.close();
					
					CLogger.writeConsole("Records escritos: "+rows);
					
					rows_total += rows;
					rows=0;
					
					query = "Truncate table sipro_analytic.mv_ep_estructura;";
					
					pstm1 = CMariaDB.getConnection_analytic().prepareStatement(query);
					pstm1.executeUpdate();
					pstm1.close();
					
					query = String.join(" ", "",
							"Insert into sipro_analytic.mv_ep_estructura" ,
							"select t1.ejercicio,t1.fuente,t1.organismo,t1.correlativo,",
							"t1.programa, t1.subprograma,t1.proyecto,t1.actividad,t1.obra, t1.renglon,t1.geografico, ",
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
								"programa, subprograma,proyecto,actividad,obra, renglon,geografico, ",
								"sum(ejecucion_presupuestaria) as ejecucion_presupuestaria",
								"from sipro_analytic.mv_ejecucion_presupuestaria",
							    "group by ejercicio, mes, fuente,organismo,correlativo, programa, subprograma,proyecto,actividad,obra,renglon,geografico",
							") t1",
							"group by t1.ejercicio,t1.fuente,t1.organismo,t1.correlativo,",
							"t1.programa, t1.subprograma,t1.proyecto,t1.actividad,t1.obra,t1.renglon,t1.geografico;");
					
				pstm1 = CMariaDB.getConnection_analytic().prepareStatement(query);
				rows = pstm1.executeUpdate();

				pstm1.close();
				
				CLogger.writeConsole("Records escritos: "+rows);
				
				rows_total += rows;
				rows=0;
				
				CLogger.writeConsole("Cargando mv_ep_ejec_asig_vige");
				
				pstm1 = CMariaDB.getConnection_analytic().prepareStatement("Insert INTO sipro_analytic.mv_ep_ejec_asig_vige "
						+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");
				 
				pstm = conn.prepareStatement("select * " +
						"from ( " +
						"select " + 
						"nvl(ga.ejercicio, m.ejercicio) ejercicio, " +
						"nvl(ga.mes, m.mes) mes, " +
						"nvl(ga.entidad,m.entidad)  entidad, " +
						"nvl(ga.unidad_ejecutora,m.unidad_ejecutora) unidad_ejecutora, " +
						"nvl(ga.programa,m.programa) programa , " +
						"nvl(ga.subprograma,m.subprograma) subprograma, " +
						"nvl(ga.proyecto,m.proyecto) proyecto, " +
						"nvl(ga.actividad,m.actividad) actividad, " +
						"nvl(ga.obra,m.obra) obra, " +
						"nvl(ga.renglon,m.renglon) renglon, " +
						"nvl(ga.geografico,m.geografico) geografico , " +
						"nvl(ga.correlativo,m.correlativo) correlativo , " +
						"nvl(ga.organismo,m.organismo) organismo, " +
						"nvl(ga.fuente, m.fuente) fuente, " +
						"ga.ejecutado, ga.asignado, m.modificaciones " +
						"from ( " +
						"select nvl(ejec.ejercicio, asig.ejercicio) ejercicio, ejec.mes, " +
						"nvl(ejec.entidad,asig.entidad)  entidad, " +
						"nvl(ejec.unidad_ejecutora,asig.unidad_ejecutora) unidad_ejecutora, " +
						"nvl(ejec.programa,asig.programa) programa , " +
						"nvl(ejec.subprograma,asig.subprograma) subprograma, " +
						"nvl(ejec.proyecto,asig.proyecto) proyecto, " +
						"nvl(ejec.actividad,asig.actividad) actividad, " +
						"nvl(ejec.obra,asig.obra) obra, " +
						"nvl(ejec.renglon,asig.renglon) renglon, " +
						"nvl(ejec.geografico,asig.geografico) geografico , " +
						"nvl(ejec.correlativo,asig.correlativo) correlativo , " +
						"nvl(ejec.organismo,asig.organismo) organismo, " +
						"nvl(ejec.fuente, asig.fuente) fuente, " +
						"ejec.ejecutado, asig.asignado " +
						"from  " +
						"( " +
						  "SELECT gh.ejercicio,month(gh.fec_aprobado) mes, " +
						"gd.entidad,gd.unidad_ejecutora, " +
						"gd.programa,gd.subprograma,gd.proyecto,gd.actividad,gd.obra,gd.renglon,gd.fuente, " +
						  "gd.geografico,gd.correlativo,gd.organismo,SUM (gd.monto_renglon) ejecutado " +
						  "FROM sicoinprod.eg_gastos_hoja gh, sicoinprod.eg_gastos_detalle gd " +
						  "WHERE gh.ejercicio = gd.ejercicio " +
						  "AND gh.entidad = gd.entidad " +
						  "AND gh.unidad_ejecutora = gd.unidad_ejecutora " +
						  "AND gh.no_cur = gd.no_cur " +
						  "AND gh.clase_registro IN ('DEV', 'CYD', 'RDP','REG') " +
						  "AND gh.estado = 'APROBADO' " +
						  "AND gh.fuente = 52 " +
						  "GROUP BY " +
						"gh.ejercicio,month(gh.fec_aprobado),gd.entidad,gd.unidad_ejecutora,gd.programa,gd.subprograma,gd.proyecto, " +
						"gd.actividad,gd.obra,gd.renglon,gd.fuente,gd.geografico,gd.correlativo,gd.organismo " +
						") as ejec " +
						"full outer join ( " +
						"select p.ejercicio, p.entidad, p.unidad_ejecutora, " +
						    "p.programa, p.subprograma, p.proyecto, p.actividad, p.obra, " +
						"p.renglon, p.geografico, p.fuente, p.correlativo, p.organismo, " +
						    "sum(p.asignado) asignado " +
						    "from sicoinprod.eg_f6_partidas p, " +
						    "(select e.ejercicio, e.entidad, count(*) ues " +
						    "from sicoinprod.cg_entidades e " +
						    "group by e.ejercicio, e.entidad) num_ues " +
						     "WHERE ((p.unidad_ejecutora>0 and num_ues.ues>0) OR  " +
						"(p.unidad_ejecutora=0 and num_ues.ues=1)) " +
						    "and p.entidad=num_ues.entidad  " +
						    "and p.ejercicio = num_ues.ejercicio  " +
						    "and p.fuente = 52  " +
						    "group by p.ejercicio,  p.entidad,  p.unidad_ejecutora, " +
						    "p.programa,  p.subprograma,  p.proyecto,  p.actividad,  p.obra, " +
						"p.renglon, p.geografico, p.fuente,  p.correlativo,  p.organismo " +
						") as asig " +
						"on ( " +
						        "asig.ejercicio = ejec.ejercicio " +
						   "AND asig.entidad = ejec.entidad " +
						   "AND asig.unidad_ejecutora = ejec.unidad_ejecutora " +
						   "AND asig.programa = ejec.programa " +
						   "AND asig.subprograma = ejec.subprograma " +
						   "AND asig.proyecto = ejec.proyecto " +
						   "AND asig.actividad = ejec.actividad " +
						   "AND asig.obra = ejec.obra " +
						   "AND asig.renglon = ejec.renglon " +
						   "AND asig.geografico = ejec.geografico " +
						   "AND asig.fuente = ejec.fuente " +
						   "AND asig.correlativo = ejec.correlativo " +
						   "AND asig.organismo = ejec.organismo " +
						") " +
						"group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16 " +
						") ga full outer join " +
						"( " +
						"select t.ejercicio, t.mes, md.entidad, md.unidad_ejecutora, md.programa, md.subprograma, md.proyecto, md.actividad, md.obra, md.fuente, " +
						"md.renglon, md.geografico, md.correlativo, md.organismo, " +
						"sum(case when md.mes_modificacion<=t.mes then md.monto_aprobado else 0 end) modificaciones " +
						"from sicoinprod.eg_modificaciones_hoja mh left outer join " + 
						"(select ejercicio, entidad, count(*) ues " +
						"from sicoinprod.cg_entidades " +
						"group by ejercicio, entidad) num_ues on (mh.entidad=num_ues.entidad and mh.ejercicio = num_ues.ejercicio), dashboard.tiempo t left outer join sicoinprod.eg_modificaciones_detalle md " + 
						"on (t.ejercicio = md.ejercicio and t.dia=1) " +
						"where md.ejercicio = mh.ejercicio " +
						"and num_ues.ejercicio = mh.ejercicio " +
						"and num_ues.entidad = mh.entidad " +
						"and md.ejercicio = mh.ejercicio " +
						"and md.clase_registro = mh.clase_registro " +
						"and md.no_cur = mh.no_cur " +
						"and md.unidad_ejecutora = mh.unidad_ejecutora " +
						"and md.entidad = mh.entidad " +
						"and mh.APROBADO = 'S' " +
						"and md.fuente = 52 " +
						"and ((mh.unidad_ejecutora>0 and num_ues.ues>0) OR (mh.unidad_ejecutora=0 and num_ues.ues=1)) " +
						"group by t.ejercicio, t.mes, md.entidad,  md.unidad_ejecutora,  md.programa,  md.subprograma,  md.proyecto,  md.actividad,  md.obra, " +  
						"md.renglon,  md.geografico,  md.fuente, md.correlativo, md.organismo " +
						") m " +
						"on ( ga.ejercicio = m.ejercicio " +
						"and ga.mes = m.mes " +
						"and ga.entidad = m.entidad " +
						"and ga.unidad_ejecutora = m.unidad_ejecutora " +
						"and ga.programa = m.programa " +
						"and ga.subprograma = m.subprograma " +
						"and ga.proyecto = m.proyecto " +
						"and ga.actividad = m.actividad " +
						"and ga.fuente = m.fuente " +
						"and ga.renglon = m.renglon " + 
						"and ga.correlativo = m.correlativo " +
						"and ga.organismo = m.organismo) " +
						") as t1 "  
						+ (ejercicio!=null ? ("where t1.ejercicio = " + ejercicio) : "")
					); 

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
						pstm1.setInt(11, rs.getInt("geografico"));
						pstm1.setInt(12, rs.getInt("correlativo"));
						pstm1.setInt(13, rs.getInt("organismo"));
						pstm1.setInt(14, rs.getInt("fuente"));
						pstm1.setDouble(15, rs.getDouble("ejecutado"));
						pstm1.setDouble(16, rs.getDouble("asignado"));
						pstm1.setDouble(17, rs.getDouble("modificaciones"));
						
						pstm1.addBatch();
						rows++;
						
						if((rows % 100000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole("  Records escritos: "+rows);
						}
					}
				CLogger.writeConsole("Ttoal Records escritos de Ejecucion Presupuestaria: "+rows);
				pstm1.executeBatch();
				rows_total += rows;
				rows=0;
				rs.close();
				pstm.close();
				
				pstm1.close();
				rows = 0;
					
				CLogger.writeConsole("Records escritos Totales: "+rows_total);
			}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CEjecucionPresupuestaria.class", e);
		}
		finally{
			CMariaDB.close_analytic();
		}
		return ret;
	}
}
