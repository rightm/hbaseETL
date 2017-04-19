package com.datacenter.hbase.db.excel;

import java.util.List;

public class FirstRowHandler implements IRowHandler{
	private List<Object> rowList ;
	private ExcelFileHandler excelHandler ;
	private String sheetname  ;
	private int startSheet ;
	
	public FirstRowHandler(String sheetname) {
		 this.sheetname = sheetname;
	}
	
	public FirstRowHandler(int index ) {
		 this.startSheet = index ;
	}
	
	public void collectOneRow(int rowNum,List<Object> rowList)throws Exception {
		 setRowList(rowList);
		// System.out.println("extract first row"+rowList );
		 getExcelFileHandler().setAbort( true );
	}

	public List<Object> getRowList() {
		return rowList;
	}

	public void setRowList(List<Object> rowList) {
		this.rowList = rowList;
	}

	public ExcelFileHandler getExcelFileHandler(){
		return this.excelHandler ;
	}
	
	public void setExcelHandler(ExcelFileHandler excelHandler) {
		this.excelHandler = excelHandler ;
	}

	public boolean matchSheet(String sheetName) {
		return  true;
	}

	public boolean matchSheet(int index) {
		return  startSheet  == index ;
	}

}
