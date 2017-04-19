package com.datacenter.hbase.db.htable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datacenter.hbase.db.htable.intf.IExcelTable;

/**
 * 基站数据
 * @author Administrator
 * 
 */
public class BaseSite_UNI implements IExcelTable{
	private List<Object> firstRow ;
	private Map<String, String> familyNameMap ;
	
	public BaseSite_UNI( ) {
 		familyNameMap = new HashMap<String, String>();
		familyNameMap.put("ID", "id") ;
		familyNameMap.put("SITENAME", "name") ;
		familyNameMap.put("SITETYPE", "type") ;
		familyNameMap.put("SITELAC", "lac") ;//大基站号
		familyNameMap.put("SITELACHEX", "lachex") ;//大基站号（16进制表示）
		familyNameMap.put("SITECELL", "cellid") ;//小区号
		familyNameMap.put("SITECELLHEX", "cellidhex") ; //小区号（16进制表示）
		familyNameMap.put("SITELON", "longitude") ; //经度
		familyNameMap.put("SITELAT", "latitude") ;//纬度
		familyNameMap.put("SITEDIRECTION", "direction") ;//方向
		familyNameMap.put("SITEACCURACY", "accuracy") ;//精度
		//familyNameMap.put("", "province") ;//省
		familyNameMap.put("SITECITY", "city") ;//市
		familyNameMap.put("SITECOUNTRY", "county") ;//区
		familyNameMap.put("SITESTREET", "street") ;//街道
		//familyNameMap.put("", "isofficial") ;//是否为官方数据
		familyNameMap.put("PC", "tag") ;//导入批次
		//familyNameMap.put("", "source") ;//数据来源
		familyNameMap.put("USERID", "userid") ;//用户id
		familyNameMap.put("CREATEDATE", "createddate") ;//创建时间//(datetime ('now', 'localtime'))
		//familyNameMap.put("", "createdby") ;//创建人
		familyNameMap.put("", "modifieddate") ;//修改时间
		familyNameMap.put("", "modifiedby") ;//修改人
		familyNameMap.put("", "syncstatus") ;//同步状态
		familyNameMap.put("", "syncversion") ;//同步版本
	}
	
	//没办法直接将基站名字作为rowkey，做好做一些转换
	//比如 ： 慈溪-跨海大桥2
	public String getIndex() {
		return "SITENAME";
	}

	public String getTablenName() {
		return "BASESITE";
	}

	public String getFamily() {
		return "CF";
	}

	public String getWhere(String... args) {
		return null;
	}

	public String getAuxiliaryKey() {
		return "name"; //基站名称
	}

	public List<Object> getExcelColumns() {
		return getFirstRow();
	}

	public String getExcelFileName() {
		return "SITE_INFO_UNI.xlsx";
	}

	public void setExcelColumns(List<Object> cols) {
		setFirstRow(cols);	
	}

	public int getRowKeyIndex() {
		return 2-1;
	}

	public boolean ifFormatCellValue() {
		// TODO Auto-generated method stub
		return false;
	}

	public Map<String, String> getFamilyNameMap() {
		return this.familyNameMap;
	}

	public List<Object> getFirstRow() {
		return firstRow;
	}

	public void setFirstRow(List<Object> firstRow) {
		this.firstRow = firstRow;
	}

	public Map<String, Object> getColumnTypeMap() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isIgnored(String key) {
		// TODO Auto-generated method stub
		return false;
	}

}
