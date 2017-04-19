package com.datacenter.hbase.db.excel;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.alibaba.fastjson.JSONObject;
import com.datacenter.hbase.Util.HbaseUtil;
import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.db.AccessorOperation;
import com.datacenter.hbase.db.DbAccessor;
import com.datacenter.hbase.db.hbasehandler.IHbandler;
import com.datacenter.hbase.db.htable.intf.HtableFactory;
import com.datacenter.hbase.db.htable.intf.IExcelTable;

public class DefaultMsExcelXMLOperation implements AccessorOperation{
		private Logger logger = Logger.getLogger(getClass()) ;
		 
		public <T> List<T> acccess(DbAccessor acc, Map<String, Object> parameter) throws Exception {
			IHbandler handler = (IHbandler) parameter.get( "hclient" ) ;
			List<Object> firstRow= getFirstRow( acc.getDbFile(), 0); //第一行，有列名称
			logger.info("columns:"+firstRow.size() );
			logger.info("columns>"+JSONObject.toJSONString( firstRow ));
			IExcelTable tabInfo = (IExcelTable) HtableFactory.getItable( acc.getDbFile().getName() ) ;
			if( null == tabInfo ){
				logger.error("No such excel file "+acc.getDbFile().getName() );
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
		public  void readExcel(File file, final int startSheet, final int startRow,final IHbandler handler,final IExcelTable tableInfor) {
			
			
			ExcelFileHandler example = new ExcelFileHandler( new IRowHandler() {
				private ExcelFileHandler excelHandler ;
				
				public void setExcelHandler(ExcelFileHandler excelHandler) {
					 this.excelHandler = excelHandler ;
				}
				
				public boolean matchSheet(int index) {
					//logger.debug( startSheet+" matchSheet "+ ( startSheet  == index ) +index );
					return  startSheet  == index ;
				}
				
				public boolean matchSheet(String sheetName) {
					return true;
				}
				
				public void collectOneRow(int rowNum,List<Object> rowList) throws Exception {
					if( rowNum == 0 ){
						return ; //skip the column names
					}
					//list.add(cellList);
					//handler a row 
					String tableName = tableInfor.getTablenName() ;
					List<HbaseCell> columns = HbaseUtil.getACell(tableInfor, rowList ) ;
					//logger.info( "handler.handleRow:"+ HbaseUtil.formatStr( columns ) );
					try {
						handler.handleRow( tableName , tableInfor.getFamily(), columns);
					} catch (Exception e) {
						String rowname = columns.get(0).getRowName() ;
						String value =  columns.get(0).getValue() ;
						StringBuilder s = new StringBuilder();
						s.append(",table:").append(tableName)
						 .append(",row:").append(rowname)
						 .append(",qf:").append( columns.get(0).getQualifier() )
						 .append(",value:").append(value) ;
						logger.error("one row inserting failed in "+s.toString() ,e);
					}
				}
			}  );
			//example.processOneSheet( file , "rId1");
			try {
				example.processAllSheets( file.getAbsolutePath() );
			} catch (Exception e) {
				logger.error("can't parse excel",e);
			}
			 
		}
		/**
		 * 获取第一行 ，列信息
		 */
		public  List<Object> getFirstRow(File file, int startSheet) {
			FirstRowHandler firstRow = new FirstRowHandler( startSheet ) ;
			ExcelFileHandler example = new ExcelFileHandler( firstRow  );
			//example.processOneSheet( file , "rId1");
			try {
				example.processAllSheets( file.getAbsolutePath() );
			} catch (Exception e) {
				String message = e.getMessage()  ;
				if( e instanceof SAXException && ExcelFileHandler.ABORT_PARSE.equals( message) ){
					logger.info("abort parsing");
				}else{
					logger.error("getFirstRow exception",e);
				}
			}
			return firstRow.getRowList() ;
		}
}
