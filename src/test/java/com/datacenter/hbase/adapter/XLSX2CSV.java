package com.datacenter.hbase.adapter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * A rudimentary XLSX -> CSV processor modeled on the
 * POI sample program XLS2CSVmra by Nick Burch from the
 * package org.apache.poi.hssf.eventusermodel.examples.
 * Unlike the HSSF version, this one completely ignores
 * missing rows.
 * <p/>
 * Data sheets are read using a SAX parser to keep the
 * memory footprint relatively small, so this should be
 * able to read enormous workbooks.  The styles table and
 * the shared-string table must be kept in memory.  The
 * standard POI styles table class is used, but a custom
 * (read-only) class is used for the shared string table
 * because the standard POI SharedStringsTable grows very
 * quickly with the number of unique strings.
 * <p/>
 * Thanks to Eric Smith for a patch that fixes a problem
 * triggered by cells with multiple "t" elements, which is
 * how Excel represents different formats (e.g., one word
 * plain and one word bold).
 *
 * @author Chris Lott
 */
public class XLSX2CSV {

    /**
     * The type of the data value is indicated by an attribute on the cell.
     * The value is usually in a "v" element within the cell.
     */
    enum xssfDataType {
        BOOL,
        ERROR,
        FORMULA,
        INLINESTR,
        SSTINDEX,
        NUMBER,
    }

    /**
     * Derived from http://poi.apache.org/spreadsheet/how-to.html#xssf_sax_api
     * <p/>
     * Also see Standard ECMA-376, 1st edition, part 4, pages 1928ff, at
     * http://www.ecma-international.org/publications/standards/Ecma-376.htm
     * <p/>
     * A web-friendly version is http://openiso.org/Ecma/376/Part4
     */
    class MyXSSFSheetHandler extends DefaultHandler {
        /**
         * Table with styles
         */
        private StylesTable stylesTable;
        /**
         * Table with unique strings
         */
        private ReadOnlySharedStringsTable sharedStringsTable;
        /**
         * Destination for data
         */
        private final PrintStream output;
        /**
         * Number of columns to read starting with leftmost
         */
        private final int minColumnCount;
        // Set when V start element is seen
        private boolean vIsOpen;
        // Set when cell start element is seen;
        // used when cell close element is seen.
        private xssfDataType nextDataType;
        // Used to format numeric cell values.
        private short formatIndex;
        private String formatString;
        private final DataFormatter formatter;
        // 定义当前读到的列数，实际读取时会按照从0开始...
        private int thisColumn = -1;
        // 定义上一次读到的列序号
        private int lastColumnNumber = -1;
        // Gathers characters as they are seen.
        private StringBuffer value;
        // 定义存储每行内容的list
        private List<String> rowlist = new ArrayList<String>();
        // 定义当前读到的列与上一次读到的列中是否有空值（即该单元格什么也没有输入，连空格都不存在）默认为false
        private boolean flag = false ;

        /**
         * Accepts objects needed while parsing.
         *
         * @param styles  Table of styles
         * @param strings Table of shared strings
         * @param cols    Minimum number of columns to show
         * @param target  Sink for output
         */
        public MyXSSFSheetHandler(
                StylesTable styles,
                ReadOnlySharedStringsTable strings,
                int cols,
                PrintStream target) {
            this.stylesTable = styles;
            this.sharedStringsTable = strings;
            this.minColumnCount = cols;
            this.output = target;
            this.value = new StringBuffer();
            this.nextDataType = xssfDataType.NUMBER;
            this.formatter = new DataFormatter();
        }

        /*
           * (non-Javadoc)
           * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
           */
        public void startElement(String uri, String localName, String name,
                                 Attributes attributes) throws SAXException {

            if ("inlineStr".equals(name) || "v".equals(name)) {
                vIsOpen = true;
                // Clear contents cache
                value.setLength(0);
            }
            // c => cell
            else if ("c".equals(name)) {
                // Get the cell reference
                String r = attributes.getValue("r");
                int firstDigit = -1;
                for (int c = 0; c < r.length(); ++c) {
                    if (Character.isDigit(r.charAt(c))) {
                        firstDigit = c;
                        break;
                    }
                }
                thisColumn = nameToColumn(r.substring(0, firstDigit));//获取当前读取的列数

                // Set up defaults.
                this.nextDataType = xssfDataType.NUMBER;
                this.formatIndex = -1;
                this.formatString = null;
                String cellType = attributes.getValue("t");
                String cellStyleStr = attributes.getValue("s");
                if ("b".equals(cellType)){
                    nextDataType = xssfDataType.BOOL;
                }else if ("e".equals(cellType)){
                    nextDataType = xssfDataType.ERROR;
                }else if ("inlineStr".equals(cellType)){
                    nextDataType = xssfDataType.INLINESTR;
                }else if ("s".equals(cellType)){
                    nextDataType = xssfDataType.SSTINDEX;
                }else if ("str".equals(cellType)){
                    nextDataType = xssfDataType.FORMULA;
                }else if (cellStyleStr != null) {
                    // It's a number, but almost certainly one
                    //  with a special style or format
                    int styleIndex = Integer.parseInt(cellStyleStr);
                    XSSFCellStyle style = stylesTable.getStyleAt(styleIndex);
                    this.formatIndex = style.getDataFormat();
                    this.formatString = style.getDataFormatString();
                    if (this.formatString == null){
                        this.formatString = BuiltinFormats.getBuiltinFormat(this.formatIndex);
                    }
                }
            }
        }

        /*
           * (non-Javadoc)
           * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
           */
        public void endElement(String uri, String localName, String name)
                throws SAXException {

            String thisStr = null;

            // v => contents of a cell
            if ("v".equals(name)) {
                // Process the value contents as required.
                // Do now, as characters() may be called more than once
                switch (nextDataType) {
                    case BOOL:
                        char first = value.charAt(0);
                        thisStr = first == '0' ? "FALSE" : "TRUE";
                        break;
                    case ERROR:
                        thisStr = "\"ERROR:" + value.toString() + '"';
                        break;
                    case FORMULA:
                        // A formula could result in a string value,
                        // so always add double-quote characters.
                        thisStr = '"' + value.toString() + '"';
                        break;
                    case INLINESTR:
                        // TODO: have seen an example of this, so it's untested.
                        XSSFRichTextString rtsi = new XSSFRichTextString(value.toString());
                        thisStr = '"' + rtsi.toString() + '"';
                        break;
                    case SSTINDEX:
                        String sstIndex = value.toString();
                        try {
                            int idx = Integer.parseInt(sstIndex);
                            XSSFRichTextString rtss = new XSSFRichTextString(sharedStringsTable.getEntryAt(idx));
//                            thisStr = '"' + rtss.toString() + '"';
                            thisStr = rtss.toString();
                        }
                        catch (NumberFormatException ex) {
                            output.println("Failed to parse SST index '" + sstIndex + "': " + ex.toString());
                        }
                        break;
                    case NUMBER:
                        String n = value.toString();
                        if (this.formatString != null){
                            thisStr = formatter.formatRawCellContents(Double.parseDouble(n), this.formatIndex, this.formatString);
                        }else{
                            thisStr = n;
                        }
                        break;
                    default:
                        thisStr = "(TODO: Unexpected type: " + nextDataType + ")";
                        break;
                }

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
                        rowlist.add(i, "");
                    }
                }

                // Might be the empty string.
                rowlist.add(thisColumn, thisStr.trim());

                // Update column
                if (thisColumn > -1){
                    lastColumnNumber = thisColumn;
                }

            } else if ("row".equals(name)) {//读到一行末尾

                // Print out any missing commas if needed
                if (minColumns > 0) {
                    // Columns are 0 based
                    if (lastColumnNumber == -1) {
                        lastColumnNumber = 0;
                    }
                    for (int i = lastColumnNumber; i < (this.minColumnCount); i++) {
                        output.print("");
                    }
                }
               // rowReader.getRows(sheetIndex, curRow, rowlist);
                rowlist.clear();
                curRow++;
                flag = false ;
                // We're onto a new row
                output.println();
                lastColumnNumber = -1;
            }
        }

        /**
         * Captures characters only if a suitable element is open.
         * Originally was just "v"; extended for inlineStr also.
         */
        public void characters(char[] ch, int start, int length)
                throws SAXException {
            if (vIsOpen){
                value.append(ch, start, length);
            }
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

        public List<String> getRowlist() {
            return rowlist;
        }

        public void setRowlist(List<String> rowlist) {
            this.rowlist = rowlist;
        }

    }

    ///////////////////////////////////////

    private OPCPackage xlsxPackage;
    private int minColumns;
    private PrintStream output;

    // 当前行
     private int curRow = 0;

     private int sheetIndex = 0;

//     private IRowReader rowReader;
//
//     public void setRowReader(IRowReader rowReader) {
//         this.rowReader = rowReader;
//     }

    /**
     * Creates a new XLSX -> CSV converter
     *
     * @param pkg        The XLSX package to process
     * @param output     The PrintStream to output the CSV to
     * @param minColumns The minimum number of columns to output, or -1 for no minimum
     * @param rowReader
     */
    public XLSX2CSV(OPCPackage pkg, PrintStream output, int minColumns/*,IRowReader rowReader*/) {
        this.xlsxPackage = pkg;
        this.output = output;
        this.minColumns = minColumns;
      //  this.rowReader = rowReader;
    }

    /**
     * Parses and shows the content of one sheet
     * using the specified styles and shared-strings tables.
     *
     * @param styles
     * @param strings
     * @param sheetInputStream
     */
    public void processSheet(
            StylesTable styles,
            ReadOnlySharedStringsTable strings,
            InputStream sheetInputStream)
            throws IOException, ParserConfigurationException, SAXException {

        InputSource sheetSource = new InputSource(sheetInputStream);
        SAXParserFactory saxFactory = SAXParserFactory.newInstance();
        SAXParser saxParser = saxFactory.newSAXParser();
        XMLReader sheetParser = saxParser.getXMLReader();
        ContentHandler handler = new MyXSSFSheetHandler(styles, strings, this.minColumns, this.output);
        sheetParser.setContentHandler(handler);
        sheetParser.parse(sheetSource);
    }

    /**
     * Initiates the processing of the XLS workbook file to CSV.
     *
     * @throws IOException
     * @throws OpenXML4JException
     * @throws ParserConfigurationException
     * @throws SAXException
     */
    public void process() throws Exception {
        InputStream stream = null ;
        try {
            ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(this.xlsxPackage);
            XSSFReader xssfReader = new XSSFReader(this.xlsxPackage);
            StylesTable styles = xssfReader.getStylesTable();
            XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) xssfReader.getSheetsData();
            while (iter.hasNext()) {
                curRow = 0;
                sheetIndex++;
                stream = iter.next();
                processSheet(styles, strings, stream);
                stream.close();
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("读取文件失败，此文件可能已损坏，请参照模板重新上传！");
        }finally{
            if(null != stream){
                stream.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        File xlsxFile = new File("C:\\Users\\liuyue\\Desktop\\test1.xlsx");
        if (!xlsxFile.exists()) {
            System.err.println("Not found or not a file: " + xlsxFile.getPath());
            return;
        }
        int minColumns = 6 ;
        // The package open is instantaneous, as it should be.
        OPCPackage p = OPCPackage.open(xlsxFile.getPath(), PackageAccess.READ);
       // IRowReader reader = new RowReaderTest();
        XLSX2CSV xlsx2csv = new XLSX2CSV(p, System.out, minColumns /*,reader*/);
        xlsx2csv.process();
    }

}