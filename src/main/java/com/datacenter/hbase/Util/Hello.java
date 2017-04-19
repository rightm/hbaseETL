package com.datacenter.hbase.Util;

import java.io.File;

import com.datacenter.hbase.comm.ICallBack;

public class Hello implements ICallBack {

	public void callback(Object arg) {
		 System.out.println( arg);
	}
	
	public static void listFile(File file){
		for( String fil : file.list() ){
			System.out.println( ">>>>"+fil );
		}
	}
	
}
