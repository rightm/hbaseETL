package com.datacenter.hbase.Util;

import org.apache.log4j.Logger;

import com.datacenter.hbase.comm.ICallBack;

public class RefectUtil {
	private static Logger log = Logger.getLogger( RefectUtil.class ) ;
	
	public static void main(String[] args) {
		String clazz = "com.datacenter.hbase.Util.Hello" ;
		ICallBack cal = (ICallBack) invoke(clazz) ;
		cal.callback("hello world");
	}
	/**
	 * 反射生成实例
	 * @param clazz
	 * @return
	 */
	public static Object invoke(String clazz){
		if( null != clazz ){
			@SuppressWarnings("rawtypes")
			Class classType = null ;
			try {
				classType = Class.forName( clazz );
				if( classType.isInterface()   ){
					return null ;
				}
				return classType.newInstance();  
			} catch (Exception e) {
				log.error("can't get object from "+clazz,e);
			}  
		}
		return null ;
	}
}
