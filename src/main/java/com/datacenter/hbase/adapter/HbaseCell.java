package com.datacenter.hbase.adapter;

import java.io.Serializable;
/**
 * represent a column
 * @author Administrator
 *
 */
public class HbaseCell implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String tableName ;
	private String rowName;
	private String family;
	private String qualifier;
	private String value;
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getRowName() {
		return rowName;
	}
	public String getFamily() {
		return family;
	}
	public String getQualifier() {
		return qualifier;
	}
	public String getValue() {
		return value;
	}
	public void setRowName(String rowName) {
		this.rowName = rowName;
	}
	public void setFamily(String family) {
		this.family = family;
	}
	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
}
