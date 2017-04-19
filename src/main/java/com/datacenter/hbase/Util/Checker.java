package com.datacenter.hbase.Util;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class Checker {
	private Checker() { }
	
	public static void main(String[] args) {
	
		System.out.println(""+ defaultVal( null, null));
		
	}
	
	public static boolean isExcel(String file){
		return endWith(file,".xlsx") ;
	}
	
	public static boolean isAccess(String file){
		return endWith(file,".mdb") || endWith(file,".accdb") ;
	}
	
	private static boolean endWith(String file,String suffix){
		return file!=null && file.endsWith( suffix ) ;
	}
	
	public static void gracefullyClose(Connection conn){
		if( null != conn ){
			try {
				conn.close();
			} catch (SQLException e) {
				 
			}
		}
	}
	
	public static void gracefullyClose(PreparedStatement ps){
		if( null != ps ){
			try {
				ps.close();
			} catch (SQLException e) {
				 
			}
		}
	}
	
	public static void gracefullyClose(ResultSet rs){
		if( null != rs ){
			try {
				rs.close();
			} catch (SQLException e) {
				 
			}
		}
	}
	
	public static boolean folderExists(String path){
		File dir = new File(path) ;
		return dir.exists() && dir.isDirectory() ;
	}
	
	public static <T> T defaultVal(T val,T df){
		try {
			checkNull(val);
			return val ;
		} catch (NullPointerException e) {
			return df ;
		}
	}
	
	public static void checkNull(Object val) throws NullPointerException{
		 checkNull(null, val);
	}
	
	public static void checkNull(String key ,Object val) throws NullPointerException{
		String msg = "" ;
		if( null != key ){
			msg = key+" can't be null" ;
		}
		if( null == val ) throw new NullPointerException(msg) ;
		if( val instanceof String){
			String value = (String) val;
			if( "".equals(value.trim() )) throw new NullPointerException(msg) ;
		}
	}

	public static void gracefullyClose(Statement stmt) {
		if( null != stmt ){
			try {
				stmt.close();
			} catch (SQLException e) {
				 
			}
		}
	}
	
	public static void print(String key,String value){
		System.out.println(key+":"+value);
	}
}
