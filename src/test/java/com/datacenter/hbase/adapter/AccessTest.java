package com.datacenter.hbase.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.fastjson.JSONObject;
import com.datacenter.hbase.MsAccessTest;
import com.datacenter.hbase.Util.Checker;

public class AccessTest {
	

	/**
	 * TODO : 读取文件access
	 * 
	 * @param filePath
	 * @return
	 * @throws ClassNotFoundException
	 */
	public static List<Map<String,String>> readFileACCESS(String filePath,int page) {
		List<Map<String,String>> maplist = new ArrayList<Map<String,String>>(); //一行数据
		Properties prop = new Properties();
		prop.put("charSet", "gb2312"); // 这里是解决中文乱码
		prop.put("user", "");
		prop.put("password", "");
		String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + filePath; // 文件地址
		//jdbc:odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=
		PreparedStatement ps = null;
		Statement stmt = null;
		ResultSet rs = null;
		Connection conn =null ;
		try {
			System.out.println( ">>>>>>>>>>>>>>>>>>>>"+ url );
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			//Connection conn = DriverManager.getConnection(url, prop);
			conn = MsAccessTest.newConnection(url, prop) ;
			stmt = (Statement) conn.createStatement();
			String sql = getSQL("Dm_Mobile", page) ;
			if( null == sql ){
				return maplist;
			}
// 10 -20
			//rs = stmt.executeQuery("select * from (select top 10 id from (select top 20 id from Dm_Mobile order by id) t1 order by id desc) t2 order by id");
			System.out.println( sql );
			rs = stmt.executeQuery( sql ) ;
			ResultSetMetaData data = rs.getMetaData();

			while (rs.next()) {
				Map map = new HashMap();
				for (int i = 1; i <= data.getColumnCount(); i++) {
					String columnName = data.getColumnName(i); // 列名
					String columnValue = rs.getString(i);
					//Checker.print(columnName, columnValue);
					map.put(columnName, columnValue);
				}
				System.out.println( map.get("ID")+">>>>>>"+JSONObject.toJSONString(map));
				//maplist.add(map) ;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			Checker.gracefullyClose(rs);
			Checker.gracefullyClose(stmt);
			Checker.gracefullyClose(conn);
			
		}
		return maplist ;
	}
	
	/**
	 * ms access do not support this
	 * @param filePath
	 * @throws SQLException
	 */
	 @Deprecated
	 public static void getPrimaryKeysInfo(String filePath) throws SQLException {  
			Properties prop = new Properties();
			prop.put("charSet", "gb2312"); // 这里是解决中文乱码
			prop.put("user", "");
			prop.put("password", "");
			String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + filePath; // 文件地址
			//jdbc:odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=
			PreparedStatement ps = null;
			Statement stmt = null;
			ResultSet rs = null;
			try {
				System.out.println( ">>>>>>>>>>>>>>>>>>>>"+ url );
				Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
				Connection conn = DriverManager.getConnection(url, prop);
	        	DatabaseMetaData dbmd = conn.getMetaData();
	        	/**
	        	 * 获取对给定表的主键列的描述
	        	 * 方法原型:ResultSet getPrimaryKeys(String catalog,String schema,String table);
	        	 * catalog - 表所在的类别名称;""表示获取没有类别的列,null表示获取所有类别的列。
				 * schema - 表所在的模式名称(oracle中对应于Tablespace);""表示获取没有模式的列,null标识获取所有模式的列; 可包含单字符通配符("_"),或多字符通配符("%");
				 * table - 表名称;可包含单字符通配符("_"),或多字符通配符("%");
	        	 */
	            //rs = dbmd.getPrimaryKeys(null, null, "CUST_INTER_TF_SERVICE_REQ");  
	            rs = dbmd. getPrimaryKeys(null, null,"Dm_Mobile") ;
	            
	            while (rs.next()){  
	            	String tableCat = rs.getString("TABLE_CAT");  //表类别(可为null) 
					String tableSchemaName = rs.getString("TABLE_SCHEM");//表模式（可能为空）,在oracle中获取的是命名空间,其它数据库未知     
					String tableName = rs.getString("TABLE_NAME");  //表名  
					String columnName = rs.getString("COLUMN_NAME");//列名  
	                short keySeq = rs.getShort("KEY_SEQ");//序列号(主键内值1表示第一列的主键，值2代表主键内的第二列)  
	                String pkName = rs.getString("PK_NAME"); //主键名称    
	                
	                System.out.println(tableCat + " - " + tableSchemaName + " - " + tableName + " - " + columnName + " - "
	                       + keySeq + " - " + pkName);     
	            }  
	            conn.close();
	        }catch ( Exception e){  
	            e.printStackTrace();  
	        }finally{
				rs.close();
			}
	    }  
	
	 public static List<Map> clear(String filePath,String st,String ed) {
			List<Map> maplist = new ArrayList();
			Properties prop = new Properties();
			prop.put("charSet", "gb2312"); // 这里是解决中文乱码
			prop.put("user", "");
			prop.put("password", "");
			String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + filePath; // 文件地址
			//jdbc:odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=
			PreparedStatement ps = null;
			Statement stmt = null;
			ResultSet rs = null;
			try {
				System.out.println( ">>>>>>>>>>>>>>>>>>>>"+ url );
				Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
				Connection conn = DriverManager.getConnection(url, prop);
				stmt = (Statement) conn.createStatement();
				conn.commit();
				System.out.println("rs:"+ stmt.execute(  "delete from Dm_Mobile where id > "+st+" and id < "+ed+" order by id" ));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return maplist;
		}
	 
	public static List<Map> readFileACCESS2(String filePath) {
		List<Map> maplist = new ArrayList();
		Properties prop = new Properties();
		prop.put("charSet", "gb2312"); // 这里是解决中文乱码
		prop.put("user", "");
		prop.put("password", "");
		String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + filePath; // 文件地址
		//jdbc:odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=
		PreparedStatement ps = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			System.out.println( ">>>>>>>>>>>>>>>>>>>>"+ url );
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			Connection conn = DriverManager.getConnection(url, prop);
			stmt = (Statement) conn.createStatement();

			rs = stmt.executeQuery("select rownum from Dm_Mobile");
			ResultSetMetaData data = rs.getMetaData();

			while (rs.next()) {
				Map map = new HashMap();
				for (int i = 1; i <= data.getColumnCount(); i++) {
					String columnName = data.getColumnName(i); // 列名
					String columnValue = rs.getString(i);
					Checker.print(columnName, columnValue);
					map.put(columnName, columnValue);
				}
				maplist.add(map);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return maplist;
	}
	
	
	private static String getSQL(String tableName,int currPage){
		if( currPage == 0 ){
			return null ;
		}
		String pk = "" ;
		int range = (getOffset()*(currPage-1)) ;
		String subSql = "" ;
		String id = "id" ;
		if( null != pk && !"".equals( pk ) ){
			id = pk ;
		}
		
		if( range == 0 ){
			subSql = "";
		}else{
			subSql = " where "+id+" not in (select top "+range+" "+id+" from "+tableName+" order by "+id+" ) " ;
		}
		return "select top "+getOffset()+" * from "+tableName+subSql+" order by "+id+" " ;
	}
	public static int getOffset(){
		return 100 ;
	}
}
