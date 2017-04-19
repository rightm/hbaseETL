package com.datacenter.hbase.adapter;

import java.io.File;  
import java.io.FileInputStream;  
import java.text.DecimalFormat;  
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;  
import java.util.LinkedList;  
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;  
import org.apache.poi.hssf.usermodel.HSSFWorkbook;  
import org.apache.poi.ss.usermodel.Cell;  
import org.apache.poi.ss.usermodel.CellStyle;  
import org.apache.poi.ss.usermodel.Row;  
import org.apache.poi.ss.usermodel.Sheet;  
import org.apache.poi.ss.usermodel.Workbook;  
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.datacenter.hbase.Util.HbaseUtil;  
  
/** 
 *  
 * @author jynine 
 *  
 */  
public class ExcelUtil {  
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 字符  
    private static DecimalFormat df = new DecimalFormat("0");// 格式化 number String  
    private static DecimalFormat nf = new DecimalFormat("0.00");// 格式化数字  
    private static Logger logger = Logger.getLogger( ExcelUtil.class ) ;
    
    public static Workbook getWorkbook(File file) throws Exception {  
        String fileName = file.getName();  
        String extension = fileName.lastIndexOf(".") == -1 ? "" : fileName  
                .substring(fileName.lastIndexOf(".") + 1);  
        FileInputStream fis = new FileInputStream(file);  
        // 根据不同的文件名返回不同类型的WorkBook  
        if (extension.equals("xls")) {  
            return new HSSFWorkbook(fis);  
        } else if (extension.equals("xlsx")) {  
            return new XSSFWorkbook(fis);  
        } else {  
            throw new Exception("不支持该格式的文件！");  
        }  
    }  
    
    /**
	 * 获取第一行 ，列信息
	 */
	public static List<Object> getFirstRow(File file, int startSheet) {
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
				cellList = readLine(row, startRow) ;
				break ;
			}
		} catch (Exception e) {
			logger.error("can't read excel "+file.getAbsolutePath(),e);
		}finally{
			
		}
		return cellList;
	}
	
	private static List<Object> readLine(Row row, int startRow){
		Object value = null;
		Cell cell = null;
		CellStyle cs = null;
		String csStr = null;
		Double numval = null;
		List<Object> cellList = new LinkedList<Object>();
		if (row.getRowNum() >= startRow) {
			Iterator<Cell> cells = row.cellIterator();
			while (cells.hasNext()) {
				cell = (Cell) cells.next();
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
				cellList.add(value);
			}
		}
		return cellList ;
	}
    
    /** 
     * 读取excel 文件 
     * @param file 
     * @param startSheet 
     * @param startRow 
     * @return 
     */  
    public static List<List<Object>> readExcel(File file,int startSheet,int startRow,int columns) {  
        List<List<Object>> list = new LinkedList<List<Object>>();  
        Workbook wb = null;  
        try {  
            wb = getWorkbook(file);  
            Sheet sheet = wb.getSheetAt(startSheet);  
            Object value = null;  
            Row row = null;  
            Cell cell = null;  
            CellStyle cs = null;  
            String csStr = null;  
            Double numval = null;  
            Iterator<Row> rows = sheet.rowIterator();  
            while (rows.hasNext()) {  
                row = (Row) rows.next();  
                if(row.getRowNum() >= startRow){  
                    List<Object> cellList = HbaseUtil.getAList(columns);
                    
                    Iterator<Cell> cells = row.cellIterator();  
                    while (cells.hasNext()) {  
                        cell = (Cell) cells.next();  
                        int index = cell.getColumnIndex() ;
                        System.out.println( index );
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
                        //cellList.add(value);  
                        cellList.set(index, value);
                    }  
                    list.add(cellList);  
                }  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
        return list;  
    }  
    
    public static void readLine( File file){
    	List<List<Object>> list = readExcel(file, 0, 0,3);  
        for (int i = 0; i < list.size(); i++) {  
            List<Object> objects = list.get(i);  
            for (int j = 0; j < objects.size(); j++) {  
                System.out.print(objects.get(j)+"=====");  
            }  
            System.out.println();  
        }  
    }
    
    public static void firstLine(File file){
    	List<Object> firstRow = getFirstRow(file, 0) ;
    	for(Object row:firstRow ){
    		String rr = (String) row;
    		System.out.println( rr );
    	}
    }
    
    public static void main(String[] args) {  
        File file = new File("F:\\projectProfiles\\hbase（数据资源库）\\数据资源库\\POS银联黑名单.xlsx");  
        File file2 = new File("F:\\projectProfiles\\excel\\POS银行卡BIN数据.xlsx") ;
        
        readLine(file2);
        //readLine(file);
        // firstLine(file);
        //System.out.println( file.getName() );
    }  
}  