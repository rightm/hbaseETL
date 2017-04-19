package com.datacenter.hbase.db.excel;
 
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFReader.SheetIterator;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import com.datacenter.hbase.Util.HbaseUtil;
/**
 * 
 * @author Administrator
 * refer http://www.bbsmax.com/A/MyJxWYr2zn/
 */
public class ExcelFileHandler {
	private IRowHandler rowHandler ;
	private volatile boolean abort = false ;
	public static String ABORT_PARSE = "abort" ;
	private Logger logger = Logger.getLogger(getClass()) ;
	
	public ExcelFileHandler( IRowHandler rowHandler  ) {
		this.rowHandler = rowHandler ;
		rowHandler.setExcelHandler( this );
	}
	public IRowHandler getRowHandler() {
		return rowHandler;
	}
	public void setRowHandler(IRowHandler rowHandler) {
		this.rowHandler = rowHandler;
	}
	public boolean isAbort() {
		return abort;
	}
	public void setAbort(boolean abort) {
		this.abort = abort;
	}
	/**
	 * 解析一个sheet 
	 * @param filename
	 * @throws Exception
	 */
	public void processOneSheet(String filename,String sheetName) throws Exception {
		OPCPackage pkg = OPCPackage.open(filename);
		XSSFReader r = new XSSFReader( pkg );
		SharedStringsTable sst = r.getSharedStringsTable(); //打开文件获取文件句柄
		XMLReader parser = fetchSheetParser(sst);
		// To look up the Sheet Name / Sheet Order / rID,
		//  you need to process the core Workbook stream.
		// Normally it's of the form rId# or rSheet#
		InputStream sheet2 = r.getSheet( sheetName );
		processSheet(sheet2, parser);
	}
	
	/**
	 * 解析多个sheet
	 * @param filename
	 * @throws Exception
	 */
	public void processAllSheets(String filename) throws Exception {
		OPCPackage pkg = OPCPackage.open(filename);
		XSSFReader r = new XSSFReader( pkg );
		SharedStringsTable sst = r.getSharedStringsTable();
		//sheet 的总数目
		XMLReader parser = fetchSheetParser(sst);
		XSSFReader.SheetIterator sheets = (SheetIterator) r.getSheetsData();
		//Iterator<InputStream> sheets =  r.getSheetsData();
		int index = 0 ;
		while(sheets.hasNext() && !isAbort() ) {
			InputStream sheet = sheets.next();
			//当前sheet的名称
			//System.out.println( "sheetname:"+sheets.getSheetName() );
			if( null != getRowHandler()){
				if( !getRowHandler().matchSheet( sheets.getSheetName()  ) ){
					continue ; //skip the not match sheet
				}
				if( !getRowHandler().matchSheet(index) ){
					continue ;
				}
			}
			if( logger.isDebugEnabled() ){
				logger.debug( "Processing new sheet:"+sheets.getSheetName() );
			}
			try {
				processSheet(sheet, parser);
			} catch (Exception e) {
				String message = e.getMessage()  ;
				if( e instanceof SAXException && ExcelFileHandler.ABORT_PARSE.equals( message) ){
					//logger.info("abort parsing");
				}else{
					logger.error("can't parse file",e);
				}
			}
			index++ ;
		}
	}


	private void processSheet( InputStream sheet, XMLReader parser ) throws Exception{
		InputSource sheetSource = new InputSource(sheet);
		parser.parse(sheetSource);
		sheet.close();
	}
	public XMLReader fetchSheetParser(SharedStringsTable sst) throws SAXException {
		XMLReader parser =
			XMLReaderFactory.createXMLReader(
					"org.apache.xerces.parsers.SAXParser"
			);
		//自定义处理
		ContentHandler handler = new SheetHandler(sst,this);
		parser.setContentHandler(handler);
		return parser;
	}

	/** 
	 * See org.xml.sax.helpers.DefaultHandler javadocs 
	 * 将excel作为xml来处理 ,类似如下格式
	 * <row>
	 * 	 <cell/>
	 * 	 <cell/>
	 *	 <cell/>
	 *	 <cell/>
	 * </row>
	 * 
	 */
	private static class SheetHandler extends DefaultHandler {
		private SharedStringsTable sst;
		private String lastContents;
		private boolean nextIsString;
		private AtomicInteger counter = new AtomicInteger( 0 ) ;
		 // 定义当前读到的列与上一次读到的列中是否有空值（即该单元格什么也没有输入，连空格都不存在）默认为false
        private boolean flag = false ;
		private List<Object> rowList = new ArrayList<Object>() ;
		private ExcelFileHandler fileHandler ;
		private Logger logger = Logger.getLogger(getClass()) ;
		 // 定义当前读到的列数，实际读取时会按照从0开始...
        private int thisColumn = -1;
        // 定义上一次读到的列序号
        private int lastColumnNumber = -1;
		
		private SheetHandler(SharedStringsTable sst,ExcelFileHandler fileHandler) {
			this.sst = sst;
			this.fileHandler = fileHandler ;
		}
		
		public void exit(){
			if( counter .get() == 10 ){
				System.out.println( " 只读取10行，");
				System.exit( 0 );
			}
		}
		
		@Override
		public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
			logger.info("ignoring>"+new String(ch, start, length));
		}
		
		@Override
		public void skippedEntity(String name) throws SAXException {
			logger.info("skiping>"+ name ); 
		}
		
		/**
		 * 处理<cell>
		 */
		public void startElement(String uri, String localName, String name,
				Attributes attributes) throws SAXException {
			if( this.fileHandler.isAbort() ){
				throw new SAXException( ExcelFileHandler.ABORT_PARSE ) ;
				//return ;//skip
			}
			// c => cell
			if(name.equals("c")) {
				//String rowNum = attributes.getValue("r") ;
				String r = attributes.getValue("r");
				 int firstDigit = -1;
	                for (int c = 0; c < r.length(); ++c) {
	                    if (Character.isDigit(r.charAt(c))) {
	                        firstDigit = c;
	                        break;
	                    }
	                }
                thisColumn = nameToColumn(r.substring(0, firstDigit));//获取当前读取的列数
				// Print the cell reference
//				if( rowNum.startsWith("A") ){
//					//一行开始
//					//exist() ;
//					//System.out.print( rowNum + " - "); //坐标
//				}
				// Figure out if the value is an index in the SST
				// cellType == null ,if there is empty 
				String cellType = attributes.getValue("t"); 
				if(cellType != null && cellType.equals("s")) {
					//下面是否是字符串
					nextIsString = true;
				} else {
					nextIsString = false;
				}
			} 
			// Clear contents cache
			lastContents = "";
		}
		/**
		 * 处理结尾的 </cell>
		 */
		public void endElement(String uri, String localName, String name)
				throws SAXException {
			if( this.fileHandler.isAbort() ){
				return ;//skip
			}
			// Process the last contents as required.
			// Do now, as characters() may be called more than once
			if(nextIsString) {
				int idx = Integer.parseInt(lastContents);
				//获取单元格数据
				lastContents = new XSSFRichTextString(sst.getEntryAt(idx)).toString();
				nextIsString = false;
			}
			
			// v => contents of a cell
			// Output after we've seen the string contents
			if( name.equals("v") ) {
				// Output after we've seen the string contents
                // Emit commas for any fields that were missing on this row
                /*if (lastColumnNumber == -1) {
                    lastColumnNumber = 0;
                }*/
                // 以下是核心算法，在同一行内，若后一次比前一次读取的列序号相差大于1，证明中间没有读到值
                // 按照.xlsx底层是xml描述文件原理，此时对应xml中"空值"情况
                if(thisColumn - lastColumnNumber > 1){
                    flag = true ;
                }
                for (int i = lastColumnNumber; i < thisColumn; ++i){
                    if(flag && i > lastColumnNumber){
                    	rowList.add(i, "");
                    }
                }

                // Might be the empty string.
              //处理每个单元格数据
				//rowList.add( lastContents ) ;
                rowList.add(thisColumn, lastContents.trim());

                // Update column
                if (thisColumn > -1){
                    lastColumnNumber = thisColumn;
                }
				
			} 
			
			if( "row".equals( name ) ){ //一行结束
				try {
					newRow() ;
				} catch (Exception e) {
					//can't consume this line
					 throw new SAXException(e) ;
				}
               
                flag = false ;
                lastColumnNumber = -1;
			}
		}
		
		private void newRow() throws Exception{
			//collect the row data and reset .
			if( null != fileHandler ){
				fileHandler.getRowHandler().collectOneRow(counter.incrementAndGet()-1,rowList);
			}
			rowList = new ArrayList<Object>() ;//reset the row 
		}

		public void characters(char[] ch, int start, int length)
				throws SAXException {
			lastContents += new String(ch, start, length);
		}
		/**
         * Converts an Excel column name like "C" to a zero-based index.
         *
         * @param name
         * @return Index corresponding to the specified name
         */
        private int nameToColumn(String name) {
            int column = -1;
            for (int i = 0; i < name.length(); ++i) {
                int c = name.charAt(i);
                column = (column + 1) * 26 + c - 'A';
            }
            return column;
        }
		 
	}
	
	public static void main0(String[] args) throws Exception {
		String file = "F:\\excel\\IPAddressInfo.xlsx" ;
		FirstRowHandler firstRow = new FirstRowHandler("IPAddressInfo") ;
		ExcelFileHandler example = new ExcelFileHandler( firstRow  );
		//example.processOneSheet( file , "rId1");
		example.processAllSheets( file );
	    System.out.println(">>>>>>>>>result:"+ firstRow.getRowList()  );
	}
	
	public static void main(String[] args) {
		
		String filePath = "F:\\excel\\IPAddressInfo.xlsx" ;
		File file = new File(filePath) ;
		final int startSheet = 0 ;
		DefaultRowHandler row = new DefaultRowHandler( startSheet ,10 ) ;
		ExcelFileHandler example = new ExcelFileHandler(  row );
		//example.processOneSheet( file , "rId1");
		try {
			example.processAllSheets( file.getAbsolutePath() );
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
