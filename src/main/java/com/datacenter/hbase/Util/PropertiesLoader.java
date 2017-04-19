package com.datacenter.hbase.Util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesLoader {
	//configuration file
	private final static String file = "hbase_conf.properties" ;//apk_parse_conf.properties
	private static Properties pro ;
	
	static{
		pro = load() ;
	}
	
	public static void main(String[] args) {
		System.out.println( load() );
	}
	
	public static Properties load(){
		if( null != pro ){
			return pro ;
		}
		Properties pro = new Properties() ;
		try {
			pro.load( PropertiesLoader.class.getClassLoader().getResourceAsStream(file) );
			return pro ;
		} catch (Exception e) {
			e.printStackTrace();
			try {
				pro.load( new FileInputStream(file) );
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return null ;
	}
	
	public static String get(String key,String defaulVal){
		if( null != pro ){
			return pro.getProperty(key, defaulVal) ;
		}
		return defaulVal ;
	}
	
	public static int getInt(String key,int defaulVal){
		if( null != pro ){
			try {
				return Integer.parseInt( pro.getProperty(key) ) ;
			} catch (Exception e) {
				 
			}
		}
		return defaulVal ;
	}
	
	public static boolean getBool(String key,boolean defaulVal){
		if( null != pro ){
			try {
				return Boolean.valueOf( pro.getProperty(key) ) ;
			} catch (Exception e) {
				 
			}
		}
		return defaulVal ;
	}
}
