package com.datacenter.hbase.db.htable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datacenter.hbase.db.htable.intf.IExcelTable;
import com.datacenter.hbase.db.htable.intf.ITable;

public class PosBankCardBINData implements IExcelTable{
	private List<Object> firstRow ;
	private Map<String, String> familyNameMap ;
	
	public PosBankCardBINData() {
		familyNameMap = new HashMap<String, String>();
		// 发卡行名称及机构代码	|卡 名	|卡号长度	|银行卡全号段样式		|请输入银行卡前6位	  |卡种（银行卡还是信用卡）
		familyNameMap.put("发卡行名称及机构代码", "card_bank") ;
		familyNameMap.put("卡名", "card_name") ;
		familyNameMap.put("卡 名", "card_name") ;
		familyNameMap.put("卡号长度", "card_num_len") ;
		familyNameMap.put("银行卡全号段样式", "card_num_style") ;
		familyNameMap.put("请输入银行卡前6位", "card_six_bits") ;
		familyNameMap.put("卡种（银行卡还是信用卡）", "card_type") ;
	}
	
	public String getIndex() {
		return "请输入银行卡前6位";
	}

	public String getTablenName() {
		return "POSBANKCARDBIN";
	}

	public String getFamily() {
		return "CF";
	}

	public String getWhere(String... args) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<Object> getExcelColumns() {
		return this.firstRow;
	}

	public String getExcelFileName() {
		return "POS银行卡BIN数据.xlsx";//"posbankbin.xlsx";
	}

	public List<Object> getFirstRow() {
		return firstRow;
	}

	public void setFirstRow(List<Object> firstRow) {
		this.firstRow = firstRow;
	}

	public void setExcelColumns(List<Object> cols) {
		setFirstRow(cols);
	}

	public int getRowKeyIndex() {
		return 4;//5-1  start from 0
	}

	public boolean ifFormatCellValue() {
		return false;
	}

	public Map<String, String> getFamilyNameMap() {
		return familyNameMap;
	}

	public String getAuxiliaryKey() {
		//("发卡行名称及机构代码", "card_bank") ;
		return "card_bank";
	}

	public Map<String, Object> getColumnTypeMap() {
		Map<String, Object> typeMap = new HashMap<String, Object>();
		typeMap.put("card_bank", ITable.TYPE_STRING ) ;
		typeMap.put("card_name", ITable.TYPE_STRING  ) ;
		typeMap.put("card_num_len", ITable.TYPE_INT ) ;
		typeMap.put("card_num_style", ITable.TYPE_STRING  ) ;
		typeMap.put("card_six_bits", ITable.TYPE_INT  ) ; //主要用于区分数字还是字符
		typeMap.put("card_type", ITable.TYPE_STRING  ) ;
		return typeMap;
	}

	public boolean isIgnored(String key) {
		// TODO Auto-generated method stub
		return false;
	}
}
