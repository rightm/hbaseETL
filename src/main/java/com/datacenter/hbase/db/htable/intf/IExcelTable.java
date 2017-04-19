package com.datacenter.hbase.db.htable.intf;

import java.util.List;
import java.util.Map;

public interface IExcelTable extends ITable {
	/**
	 * 获得excel中的列名
	 */
	public List<Object> getExcelColumns();
	
	public String getExcelFileName();
	
	public void setExcelColumns(List<Object> cols);
	/**
	 * 那一列作为索引键,在excel中第几列 (0开始)
	 * @return
	 */
	public int getRowKeyIndex();
	
	/**
	 * 是否需要格式化数据
	 * @return
	 */
	public boolean ifFormatCellValue();
	/**
	 * hbase 只能支持数字或者字母作为列名称，因此只能转为字母,如果可以将excel列直接作为列名的就不需要做映射了
	 * Illegal character code:-24, <￨> at 0. User-space table qualifiers can only contain 'alphanumeric characters': i.e. [a-zA-Z_0-9-.]
	 * @return
	 */
	public Map<String, String> getFamilyNameMap() ;
}
