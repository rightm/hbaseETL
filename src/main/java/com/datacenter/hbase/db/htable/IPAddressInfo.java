package com.datacenter.hbase.db.htable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.datacenter.hbase.db.htable.intf.IExcelTable;

public class IPAddressInfo  implements IExcelTable {
	private List<Object> firstRow ;
	private Map<String, String> familyNameMap ;
	
	public String getIndex() {
		return "StartNum";
	}

	public String getTablenName() {
		return "IPADDRESSINFO";
	}

	public String getFamily() {
		// TODO Auto-generated method stub
		return "CF";
	}

	public String getWhere(String... args) {
		// TODO Auto-generated method stub
		return null;
	}

	public String getAuxiliaryKey() {
		return "Id";
	}

	public List<Object> getExcelColumns() {
		  return this.firstRow; 
	}

	public String getExcelFileName() {
		return "IPAddressInfo.xlsx";
	}

	public void setExcelColumns(List<Object> cols) {
		 this.firstRow = cols ;
	}
	
	public int getRowKeyIndex() {
		return 5-1;
	}

	public boolean ifFormatCellValue() {
		return false;
	}

	public Map<String, String> getFamilyNameMap() {
		// TODO Auto-generated method stub
		return null;
	}

	public Map<String, Object> getColumnTypeMap() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isIgnored(String key) {
		HashSet<String> ignoredColumns = new HashSet<String>() ;
		//需要忽略的字段是
		ignoredColumns.add("rowid") ;
		ignoredColumns.add("Id") ;
		return false;
	}

}
