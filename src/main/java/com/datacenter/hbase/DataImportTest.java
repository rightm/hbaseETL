package com.datacenter.hbase;

public class DataImportTest {
	/**
	 * 多线程读取Db数据文件，并且将数据存入hbase中,适合读取excel文件
	 * @param args
	 */
	public static void main(String[] args) {
		DataImporter imp = new DataImporter() ;
		try {
			 imp.importData();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
