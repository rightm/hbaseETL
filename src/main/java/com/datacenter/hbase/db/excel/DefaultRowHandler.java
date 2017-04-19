package com.datacenter.hbase.db.excel;

import java.util.List;

public class DefaultRowHandler implements IRowHandler{
	private ExcelFileHandler excelHandler ;
	private int startSheet ;
	private int stopLine ; //for test ,stop when meet this line
	
	public DefaultRowHandler(int index) {
		this.startSheet = index ;
	}
	
	public DefaultRowHandler(int index,int stopLine ) {
		this.startSheet = index ;
		this.stopLine = stopLine ;
	}
	
	public void setExcelHandler(ExcelFileHandler excelHandler) {
		 this.excelHandler = excelHandler ;
	}
	
	public boolean matchSheet(int index) {
		return startSheet == index ;
	}
	
	public boolean matchSheet(String sheetName) {
		return true;
	}
	
	public void collectOneRow(int rowNum,List<Object> rowList) throws Exception {
		if( rowNum == 0 ){
			return ; //skip the column names
		}
		if( rowNum == this.stopLine ){
			throw new Exception("--stopLine--") ;
		}
		//list.add(cellList);
		//handler a row 
		//TODO 
		System.out.println( rowNum +"#"+rowList);
	}

	public ExcelFileHandler getExcelHandler() {
		return excelHandler;
	}
}
