package com.datacenter.hbase.db.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.alibaba.fastjson.JSONObject;
import com.datacenter.hbase.Util.HbaseUtil;
import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.adapter.HbaseClient;
import com.datacenter.hbase.db.AccessorOpFactory;
import com.datacenter.hbase.db.AccessorOperation;
import com.datacenter.hbase.db.DBConstants;
import com.datacenter.hbase.db.DbAccessor;
import com.datacenter.hbase.db.hbasehandler.IHbandler;
import com.datacenter.hbase.db.hbasehandler.MsAccessHandler;
import com.datacenter.hbase.db.htable.intf.HtableFactory;
import com.datacenter.hbase.db.htable.intf.IExcelTable;

public class MsExcel implements DbAccessor {
	private File file ;
	private HbaseClient client ;
	
	public MsExcel( File file,HbaseClient client) {
		this.file = file ;
		this.client = client ;
	}
	
	//name of columns
	private List<String> columnNames = new ArrayList<String>() ;
	
	public Connection openConn() throws Exception {
		// TODO Auto-generated method stub
		 return null ;
	}

	public void closeConn() {
		// TODO Auto-generated method stub
		
	}

	public void access(AccessorOpFactory fac) throws Exception {
		AccessorOperation op = fac.getOperation(this,"") ;
		IHbandler handler = new MsAccessHandler( getHbaseClient() ) ;
		Map<String, Object> param = new HashMap<String, Object>() ;
		param.put( "hclient" ,  handler) ;
		param.put(DbAccessor.CONN_KEY, null ) ;
		op.acccess(this,param) ;	
	}

	public static class DefaultMsExcelOperation implements AccessorOperation{
		private  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 字符
		private  DecimalFormat df = new DecimalFormat("0");// 格式化 number
		private  DecimalFormat nf = new DecimalFormat("0.00");// 格式化数字
		private Logger logger = Logger.getLogger(getClass()) ;
		
		public <T> List<T> acccess(DbAccessor acc, Map<String, Object> parameter) throws Exception {
			IHbandler handler = (IHbandler) parameter.get( "hclient" ) ;
			List<Object> firstRow= getFirstRow( acc.getDbFile(), 0); //第一行，有列名称
			IExcelTable tabInfo = (IExcelTable) HtableFactory.getItable( acc.getDbFile().getName() ) ;
			if( null == tabInfo ){
				logger.error("No such excel file"+acc.getDbFile().getName() );
				return null ;
			}
			tabInfo.setExcelColumns(firstRow);
			handler.setHtable(tabInfo);
			// 1.创建表
			try {
				handler.createHTable( tabInfo.getTablenName(), tabInfo.getFamily() );
			} catch (Exception e) {
				throw new Exception(e) ;
			}
			// 2.循环遍历表数据,插入数据
			readExcel( acc.getDbFile() , 0, 0,handler,tabInfo) ;
			//TODO 
			return null;
		}
		
		/**
		 * 读取excel 文件
		 * 
		 * @param file
		 * @param startSheet  sheet num 0
		 * @param startRow   start row 0
		 * @return
		 */
		public  void readExcel(File file, int startSheet, int startRow,IHbandler handler,IExcelTable tableInfor) {
			//List<List<Object>> list = new LinkedList<List<Object>>();
			Workbook wb = null;
			try {
				wb = getWorkbook(file);
				Sheet sheet = wb.getSheetAt(startSheet);
				Row row = null;
				Iterator<Row> rows = sheet.rowIterator();
				int cols = tableInfor.getExcelColumns().size() ; //列数目
				while (rows.hasNext()) {
					row = (Row) rows.next();
					List<Object> cellList = HbaseUtil.getAList(cols);
					cellList = readLine(row, startRow,cols ,tableInfor.ifFormatCellValue()) ;
					if( row.getRowNum() == 0 ){
						continue ; //skip the column names
					}
					//list.add(cellList);
					//handler a row 
					List<HbaseCell> columns = HbaseUtil.getACell(tableInfor, cellList ) ;
					//System.out.println( "handler.handleRow:"+ HbaseUtil.formatStr( columns ) );
					handler.handleRow(tableInfor.getTablenName(), tableInfor.getFamily(), columns);;
				}
			} catch (Exception e) {
				logger.error("can't parse excel",e);
				e.printStackTrace();
			} finally {
				if( null != pkg ){
					try {
						pkg.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
				}
			}
		}
		/**
		 * 获取第一行 ，列信息
		 */
		public  List<Object> getFirstRow(File file, int startSheet) {
			int startRow = 0 ;
			Workbook wb = null;
			List<Object> cellList = new LinkedList<Object>();
			
			try {
				wb = getWorkbook(file);
				Sheet sheet = wb.getSheetAt(startSheet);
				Iterator<Row> rows = sheet.rowIterator();
				Row row = null;
				while (rows.hasNext()) {
					row = (Row) rows.next();
					cellList = readLine(row, startRow,-1,false) ;
					break ;
				}
			} catch (Exception e) {
				logger.error("can't read excel "+file.getAbsolutePath(),e);
			}finally{
				 if( null != pkg ){
					 try {
						pkg.close();
					} catch (IOException e) {
						e.printStackTrace();
					} 
				 }
			}
			return cellList;
		}
		
		private List<Object> readLine(Row row, int startRow,int columns,boolean format){
			Object value = null;
			Cell cell = null;
			List<Object> cellList ;
			if( -1 == columns ){
				cellList =   new ArrayList<Object>() ;
				//List<Object> cellList = new LinkedList<Object>();
			}else{
				cellList = HbaseUtil.getAList(columns);
			}
			if (row.getRowNum() >= startRow) {
				Iterator<Cell> cells = row.cellIterator();
				while (cells.hasNext()) {
					cell = (Cell) cells.next();
					int index = cell.getColumnIndex() ;
					value = getCellValue(cell,format) ;
					if( columns == -1 ){
						cellList.add(value);
					}else{
						cellList.set(index, value);
					}
				}
			}
			return cellList ;
		}
		
		private  Object getCellValue(Cell cell,boolean format) {
			Object value = null;
			CellStyle cs = null;
			String csStr = null;
			Double numval = null;
			
			if( format ){
				switch (cell.getCellType()) {
				case Cell.CELL_TYPE_NUMERIC:
					cs = cell.getCellStyle();
					csStr = cs.getDataFormatString();
					numval = cell.getNumericCellValue();
					if ("@".equals(csStr)) {
						value = df.format(numval);
					} else if ("General".equals(csStr)) {
						value = nf.format(numval);
					} else {
						value = sdf.format(HSSFDateUtil.getJavaDate(numval));
					}
					break;
				case Cell.CELL_TYPE_STRING:
					value = cell.getStringCellValue();
					break;
				case Cell.CELL_TYPE_FORMULA:
					if (!cell.getStringCellValue().equals("")) {
						value = cell.getStringCellValue();
					} else {
						value = cell.getNumericCellValue() + "";
					}
					break;
				case Cell.CELL_TYPE_BLANK:
					value = "";
					break;
				case Cell.CELL_TYPE_BOOLEAN:
					value = cell.getBooleanCellValue();
					break;
				default:
					value = cell.toString();
				}
			}else{
				if( cell.getCellType() == Cell.CELL_TYPE_NUMERIC ){
					numval = cell.getNumericCellValue();
					value = df.format(numval);
				}else{
					value = cell.toString().replace(" ","").trim();
				}
			}
				
			return value ;
		} 
		
		private SXSSFWorkbook xsfs(XSSFWorkbook xss){
			return new SXSSFWorkbook(xss ) ;
		}
		
		private OPCPackage pkg ;
		
		@SuppressWarnings("resource")
		public  Workbook getWorkbook(File file) throws Exception {
			String fileName = file.getName();
			String extension = fileName.lastIndexOf(".") == -1 ? "" : fileName.substring(fileName.lastIndexOf(".") + 1);
			FileInputStream fis = new FileInputStream(file);
			// 根据不同的文件名返回不同类型的WorkBook
			if (extension.equals("xls")) {
				return new HSSFWorkbook(fis);
			} else if (extension.equals("xlsx")) {
				//XSSFWorkbook 会比较吃内存，需要通过SXSSFWorkbook 的方式类访问excel文件
				OPCPackage a ;
				return xsfs( new XSSFWorkbook(fis) );
			} else {
				throw new Exception("不支持该格式的文件！");
			}
			
		}
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

	public String getDbType() {
		return DBConstants.MS_EXCEL ;
	}

	public File getDbFile() {
		return this.file ;
	}

	public void setHbaseClient(HbaseClient client) {
		 this.client = client ;
	}

	public HbaseClient getHbaseClient() {
		return this.client ;
	}

	public String getFilePath() {
		// TODO Auto-generated method stub
		if( null != file ){
			return this.file.getAbsolutePath();
		}
		return null ;
	}

}
