package com.datacenter.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.Checker;
import com.datacenter.hbase.Util.HbaseUtil;
import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.adapter.HbaseClient;
import com.datacenter.hbase.db.hbasehandler.IHbandler;
import com.datacenter.hbase.db.hbasehandler.MsAccessHandler;
import com.datacenter.hbase.db.htable.intf.HtableFactory;
import com.datacenter.hbase.db.htable.intf.ITable;

public class MsAccessTest {
	private static Logger logger = Logger.getLogger( MsAccessTest.class ) ;
	
	//读取access文件
	public static void main(String[] args) throws InterruptedException {
		String filePath = "F:\\excel\\sjgsd.mdb" ;
		//String filePath = "F:\\projectProfiles\\sjgsd.mdb" ;
		String tableName = "Dm_Mobile" ;
		Set<String> needed = new HashSet<String>() ;
		needed.add( tableName ) ;
		long size ;//size =  getRows(filePath, tableName) ;
		long maxId = getMaxId(filePath, tableName) ;
		size = maxId ;
		HbaseClient hClient = HbaseClient.build() ;
		IHbandler handler = new MsAccessHandler(hClient) ;
		ITable tableInfor = HtableFactory.getItable(tableName) ;
		handler.setHtable(tableInfor);
		try{
		// 1.创建表
			handler.createHTable(tableName, tableInfor.getFamily());
		} catch (Exception e) {
			logger.error("can't create table",e);
			System.exit(0);
		}
		/**
		 * 2.循环遍历表数据,插入数据
		 */
		int offset = 1000 ;
		long devide = size/1000 ;
		long loop = (size%1000==0? devide :devide+1) ;
		
		System.out.println( "总记录数："+size );
		int st = 0 ;
		int end = offset ;
		for(int i = 1 ;i< loop;i++){
			List<Map<String,String>> rows = readFileACCESS(tableName, filePath, st, end); //i>1
			if( null == rows ) {
				continue ;
			}
			st = end ;
			end = st+offset ;
			for(Map<String,String> row : rows){
				try {
					insertOneRow(handler,tableInfor, row);
				} catch (Exception e) {
					logger.error("row inserting failed",e);
				}
			}
		}
		System.err.println("import over");
	}
	 
	 private static void insertOneRow(IHbandler handler,ITable tableInfor,Map<String,String> row) throws Exception{
			List<HbaseCell> columns = new ArrayList<HbaseCell>() ;
			String rowkey = row.get( tableInfor.getIndex() )==null?row.get( tableInfor.getIndex().toUpperCase() ):"" ; //数据库主键作为rowkey
			for(String key : row.keySet() ){
				//row,family,qualifier,value
				columns.add( HbaseUtil.getACell( rowkey , tableInfor.getFamily(), key, row.get(key) ) ) ;
			}
			//DEBUG
			//System.out.println("handler.handleRow:"+ JSONObject.toJSONString( columns.get(0).getRowName() ) );
			handler.handleRow(tableInfor.getTablenName(), tableInfor.getFamily(), columns);
		}
	 
	
	
	public static Connection newConnection(String url,Properties prop){
		try {
			return  DriverManager.getConnection(url, prop);
		} catch (SQLException e) {
			 logger.error("can't create connection to"+url,e);
		}
		return null ;
	}
	
	/**
	 * TODO : 读取文件access
	 * @param filePath
	 * @return
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes", "unused" })
	public static List<Map<String,String>> readFileACCESS(String table,String filePath,int st,int end) {
		List<Map<String,String>> maplist = new ArrayList<Map<String,String>>(); //一行数据
		String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + filePath; // 文件地址
		PreparedStatement ps = null;
		Statement stmt = null;
		ResultSet rs = null;
		Connection conn =null ;
		try {
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			conn = newConnection(url, getProperties() ) ;
			stmt = (Statement) conn.createStatement();
			String sql =  "select * from "+table+" where id>"+st+" and id <"+end  ;
			if( null == sql ){
				return maplist;
			}
			logger.info( sql );
			rs = stmt.executeQuery( sql ) ;
			ResultSetMetaData data = rs.getMetaData();

			while (rs.next()) {
				Map map = new HashMap();
				for (int i = 1; i <= data.getColumnCount(); i++) {
					String columnName = data.getColumnName(i); // 列名
					String columnValue = rs.getString(i);
					map.put(columnName, columnValue);
				}
				//System.out.println( map.get("ID")+">>>>>>"+JSONObject.toJSONString(map));
				maplist.add(map) ;
			}
		} catch (Exception e) {
			logger.error("data read failed",e);
		}finally {
			Checker.gracefullyClose(rs);
			Checker.gracefullyClose(stmt);
			Checker.gracefullyClose(conn);
			
		}
		return maplist ;
	}
	
	public static Properties getProperties(){
		Properties prop = new Properties();
		prop.put("charSet", "gb2312"); // 这里是解决中文乱码
		prop.put("user", "");
		prop.put("password", "");
		return prop ;
	}
	
	public static long getMaxId(String filePath ,String table){
		Connection conn = null ;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + filePath; // 文件地址
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			conn = newConnection(url, getProperties() ) ;
			stmt = (Statement) conn.createStatement();
			rs = stmt.executeQuery("select max(id) from "+table);//first line
			
			while (rs.next()) {
				return rs.getLong(1) ;
			}
		} catch (Exception e) {
			logger.error("getMaxId failed",e);
		}finally{
			Checker.gracefullyClose(rs);
			Checker.gracefullyClose(stmt);
			Checker.gracefullyClose(conn);
		}
	     return 0 ;
	}
	
	public static long getRows(String filePath ,String table){
		Connection conn = null ;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + filePath; // 文件地址
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			conn = newConnection(url, getProperties() ) ;
			stmt = (Statement) conn.createStatement();
			rs = stmt.executeQuery("select count(*) from "+table);//first line
			while (rs.next()) {
				return rs.getLong(1) ;
			}
		} catch (Exception e) {
			logger.error("getRows failed",e);
		}finally{
			Checker.gracefullyClose(rs);
			Checker.gracefullyClose(stmt);
			Checker.gracefullyClose(conn);
		}
	     return 0 ;
	}
}
