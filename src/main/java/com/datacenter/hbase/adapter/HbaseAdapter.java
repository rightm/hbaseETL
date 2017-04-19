package com.datacenter.hbase.adapter;

import static com.datacenter.hbase.Util.HbaseUtil.bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete ;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result ;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.Checker;
import com.datacenter.hbase.Util.HbaseUtil;   

/**
 * CRUD
 * @author Administrator
 *
 */
public class HbaseAdapter {
	public static final String HBASE_PORT = "hbase.zookeeper.property.clientPort";
	public static final String HABSE_QUORUM="hbase.zookeeper.quorum";
	private HbaseClient hbase ;
	private Logger logger = Logger.getLogger(getClass()) ;
	
	public HbaseAdapter(HbaseClient hbase) {
		this.hbase = hbase ;
	}
	
	/**
	 * create a table in HBASE
	 */
	public void createTable(String tablenm,String colFamliy,boolean overide) throws Exception {
		Checker.checkNull(tablenm);
		Checker.checkNull(colFamliy);
		Connection conn = getHbase().openConnection() ;
		try {
			Admin admin = conn.getAdmin() ;
			Checker.checkNull(admin);
			
			TableName tableName = TableName.valueOf(tablenm);
			if (admin.tableExists(tableName) ) {
				if( !overide ){
					return ;  //不会覆盖原来表
				}
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			 HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			 tableDesc.addFamily(new HColumnDescriptor(colFamliy));
			 admin.createTable(tableDesc);
			 if(logger.isDebugEnabled()){
				 logger.debug("table created!"+tablenm);
			 }
		} catch (Exception e) {
			throw new Exception("table creating failed "+tablenm,e) ;
		}finally {
			getHbase().releaseConnection(conn);
		}
	}
	
	/**
	 * insert data into table
	 */
	private void insertTO(Table table,List<Put> puts,boolean close) throws  Exception {
		Checker.checkNull(table);
		Checker.checkNull(puts);
		//TODO modify 
		table.put(puts);
		if(close){
			table.close();
		}
	}
	
	public boolean existRow(String tableName,String rowkey) throws Exception{
		Connection conn = getHbase().openConnection() ;
		Table table = null ;
		try {
			table = HbaseUtil.getHTable(conn, tableName)  ;
			Get get = new Get( bytes(rowkey)) ;
			return table.exists(get) ;
		} catch (Exception e) {
			throw new Exception("table inserting failed "+tableName+",rowkey="+rowkey,e) ;
		}finally {
			HbaseUtil.closeHTable(table) ;
			getHbase().releaseConnection(conn);
		}
	}
	
	public void insertTO(String tableName,List<Put> puts) throws  Exception {
		Connection conn = getHbase().openConnection() ;
		Table table = null ;
		try {
			if ( logger.isDebugEnabled() ) {
				//logger.debug("intsert into "+tableName);
			}
			table = HbaseUtil.getHTable(conn, tableName)  ;
			insertTO(table, puts,false);
		} catch (Exception e) {
			throw new Exception("table inserting failed "+tableName,e) ;
		}finally {
			HbaseUtil.closeHTable(table) ;
			getHbase().releaseConnection(conn);
		}
	}
	/**
	 * add one column in a row
	 */
	public void addOneColumn(String tableName,HbaseCell cell) throws  Exception{
		Checker.checkNull(cell);
		Connection conn = getHbase().openConnection() ;
		Table table = null ;
		try {
			table = HbaseUtil.getHTable(conn, tableName)  ;
			Append append= new Append( bytes(cell.getRowName()) ) ;
			append.add( bytes(cell.getFamily()),bytes( cell.getQualifier()), bytes(cell.getValue()) ) ;
			table.append(append) ;
		} catch (Exception e) {
			throw new Exception("table addOneColumn failed "+tableName,e) ;
		}finally {
			HbaseUtil.closeHTable(table) ;
			getHbase().releaseConnection(conn);
		}
	}
	
	public List<Result> scaneByPrefixFilter(String tableName, String rowPrifix) throws Exception {
	   Connection conn = getHbase().openConnection() ;
	   Table table = null ;
	   List<Result> result = new ArrayList<Result>() ;
	   try {
		   table = HbaseUtil.getHTable(conn, tableName)  ;
	       Scan s = new Scan();
	       s.setFilter(new PrefixFilter(rowPrifix.getBytes()));
	       ResultScanner rs = table.getScanner(s);
	       for (Result r : rs) {
	    	  result.add( r ) ;
	      }
	    } catch (IOException e) {
		   throw new Exception("table scaneByPrefixFilter failed "+tableName,e) ;
		}finally {
			HbaseUtil.closeHTable(table) ;
			getHbase().releaseConnection(conn);
		}
	   return result ;
	 }
	
	 public Result selectRowKeyFamilyColumn(String tablename, String rowKey, String family, String column)  
	            throws Exception    {  
			Connection conn = getHbase().openConnection() ;
			Table table = null ;
		 	
			Result rs = null;
			try {
				table = HbaseUtil.getHTable(conn, tablename)  ;
				Get g = new Get(rowKey.getBytes());  
				g.addColumn(family.getBytes(), column.getBytes());  
  
				rs = table.get(g);
			} catch (Exception e) {
				throw new Exception("table selectRowKeyFamilyColumn failed "+tablename,e) ;
			}finally {
				HbaseUtil.closeHTable(table) ;
				getHbase().releaseConnection(conn);
			}
	        return rs ;
	    } 
	/**
	 * get result by row name,result contains :row,cell ininfo etc
	 */
	public List<Result> getRows(String tableName,List<Get> list ) throws  Exception{
		Checker.checkNull(list);
		Connection conn =null ;
		Table table = null ;
		try {
			conn = getHbase().openConnection() ;
			table = HbaseUtil.getHTable(conn, tableName) ;
			Result[] res = table.get(list) ;
			
			if( null != res ){
				 return Arrays.asList(res) ;
			}
		} catch (Exception e) {
			logger.error("can't get datas",e);
			throw new Exception(e) ;
		}finally {
			HbaseUtil.closeHTable( table) ;
			getHbase().releaseConnection(conn);
		}
		return null ;
	}
	
	/**
	 * delete one row or a family or a column
	 * @param table
	 * @param cell
	 * @throws Exception
	 */
	public void deleteOneRow(String tableName,HbaseCell cell) throws Exception{
		Checker.checkNull(tableName);
		Checker.checkNull(cell);
		Connection conn =null ;
		Table table = null ;
		try {
			conn = getHbase().openConnection() ;
			table = HbaseUtil.getHTable(conn, tableName) ;
			Delete del = new Delete( bytes(cell.getRowName())) ;
			if( null != cell.getFamily() ){
				if( null != cell.getQualifier() ){
					del.addColumn(  bytes( cell.getFamily()),bytes( cell.getQualifier())) ;
				}else{
					del.addFamily( bytes( cell.getFamily())) ;
				}
			}
			table.delete(del);
		} catch (Exception e) {
			throw new Exception(e) ;
		}finally {
			HbaseUtil.closeHTable(table) ;
			getHbase().releaseConnection(conn);
		}
	}
	
	/**
	 * 针对rowKey索引键做模糊匹配
	 */
	public void likeQueryByRowkey(String tableName,String regular) throws Exception{  
		Connection conn =null ;
		Table table = null ;
		
        Scan scan = new Scan();    
        RegexStringComparator comp = new RegexStringComparator( regular); //"(2014-09)"
        RowFilter filter = new RowFilter(CompareOp.EQUAL, comp);    
        scan.setFilter(filter);    
        scan.setCaching(200);    
        scan.setCacheBlocks(false);    
        
        conn = getHbase().openConnection() ;
		table = HbaseUtil.getHTable(conn, tableName) ;
       // HTable hTable = new HTable(configuration, "access_page_ip_basis_hour");    
       // ResultScanner scanner = hTable.getScanner(scan);    
		 ResultScanner scanner = table.getScanner(scan) ;
        
        byte[] fbytes = Bytes.toBytes("columnFamily1");  
        byte[] cbytes = Bytes.toBytes("h_time");   
        for (Result r : scanner) {   
        	//TODO 
//            AccessPageIpBasisHour o = new AccessPageIpBasisHour();  
//            for (KeyValue kv : r.raw()) {  
//                if("pageNum".equals(new String(kv.getQualifier()))){  
//                    o.setPageNum(Integer.parseInt(new String(kv.getValue())));  
//                    String hTime = new String(kv.getRow());  
//                    o.sethTime(hTime);  
//                }else if("ipNum".equals(new String(kv.getQualifier()))){  
//                    o.setIpNum(Integer.parseInt(new String(kv.getValue())));  
//                }else if("accessNum".equals(new String(kv.getQualifier()))){  
//                    o.setAccessNum(Integer.parseInt(new String(kv.getValue())));  
//                }  
//            }  
//            System.out.println(o.getAccessNum());  
        }    
	}  
	
	/**
	 * 针对列值做模糊匹配
	 */
	public void likeQueryByColumn(String tableName,String regular,byte[] cfBytes,byte[] qfBytes) throws Exception{  
		Connection conn =null ;
		Table table = null ;
        Scan scan = new Scan();    
        RegexStringComparator comp = new RegexStringComparator(regular);    //"(##2014-09)"
        //byte[] bytes = Bytes.toBytes("h_time");    
        //
        Filter filter = new SingleColumnValueFilter(cfBytes, qfBytes, CompareOp.EQUAL, comp);    
        scan.setFilter(filter);    
        scan.setCaching(200);    
        scan.setCacheBlocks(false);    
        conn = getHbase().openConnection() ;
		table = HbaseUtil.getHTable(conn, tableName) ;
        //HTable hTable = new HTable(configuration, "access_page_ip_basis_hour");    
        ResultScanner scanner = table.getScanner(scan);    
        for (Result r : scanner) {  
        	//TODO 
//            AccessPageIpBasisHour o = new AccessPageIpBasisHour();  
//            for (KeyValue kv : r.raw()) {  
//                if("pageNum".equals(new String(kv.getQualifier()))){  
//                    o.setPageNum(Integer.parseInt(new String(kv.getValue())));  
//                    String hTime = new String(kv.getRow());  
//                    o.sethTime(hTime);  
//                }else if("ipNum".equals(new String(kv.getQualifier()))){  
//                    o.setIpNum(Integer.parseInt(new String(kv.getValue())));  
//                }else if("accessNum".equals(new String(kv.getQualifier()))){  
//                    o.setAccessNum(Integer.parseInt(new String(kv.getValue())));  
//                }  
//            }  
//            System.out.println(o.gethTime());  
        }    
    }  
	

	public HbaseClient getHbase() {
		return hbase;
	}

	public void setHbase(HbaseClient hbase) {
		this.hbase = hbase;
	}
}