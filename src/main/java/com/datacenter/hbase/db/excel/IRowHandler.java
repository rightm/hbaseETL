package com.datacenter.hbase.db.excel;

import java.util.List;

public interface IRowHandler {
	public void collectOneRow(int rowNum,List<Object> rowList) throws Exception ;
	public void setExcelHandler(ExcelFileHandler excelHandler) ;
	public boolean matchSheet(String sheetName) ;
	public boolean matchSheet(int index ) ;
}
