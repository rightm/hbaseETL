//package com.datacenter.hbase.db.htable;
//
//import java.util.List;
//import java.util.Map;
//
//import com.datacenter.hbase.db.htable.intf.IExcelTable;
//
///**
// * POS银联黑名单.xlsx
// * @author Administrator
// */
//public class PosYinLinaBlackList implements IExcelTable{
//	List<Object> firstRow ;
//	
//	public PosYinLinaBlackList() {
//	}
//	
//	public String getIndex() {
//		return "POS名称";
//	}
//
//	public String getTablenName() {
//		return "POSYINLINABLACKLIST";
//	}
//
//	public String getFamily() {
//		return "cf";
//	}
//
//	public String getWhere(String... args) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	public List<Object> getExcelColumns() {
//		return this.firstRow;
//	}
//
//	public String getExcelFileName() {
//		return "POS银联黑名单.xlsx";
//	}
//
//	public List<Object> getFirstRow() {
//		return firstRow;
//	}
//
//	public void setFirstRow(List<Object> firstRow) {
//		this.firstRow = firstRow;
//	}
//
//	public void setExcelColumns(List<Object> cols) {
//		setFirstRow(cols);
//	}
//
//	public int getRowKeyIndex() {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	public boolean ifFormatCellValue() {
//		// TODO Auto-generated method stub
//		return false;
//	}
//
//	public Map<String, String> getFamilyNameMap() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	public String getAuxiliaryKey() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//}
