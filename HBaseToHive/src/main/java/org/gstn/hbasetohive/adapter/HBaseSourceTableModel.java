/*******************************************************************************
 * Copyright 2018 Goods And Services Tax Network
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package org.gstn.hbasetohive.adapter;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.gstn.hbasetohive.util.FormatDateUtil;
import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.entity.DynamicColumnType;
import org.gstn.schemaexplorer.entity.Tuple;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseColumn;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.gstn.schemaexplorer.hbase.RowkeyField;
import org.gstn.schemaexplorer.util.DataTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an abstract class that contains different components of a source 
 * hbase table like static columns, dynamic columns etc.   
 *
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class HBaseSourceTableModel implements Serializable {

	/**
	 * Object holding information from the source hbase table schema
	 */
	private HBaseTableExplorer hBaseExplorer;
	
	
	/**
	 * Name of the source schema name
	 */
	private String sourceSchema;
	
//	private static boolean staticInitialized = false;
	
	public HBaseSourceTableModel(String sourceSchema, HBaseTableExplorer hBaseExplorer) {
		this.hBaseExplorer = hBaseExplorer;
		this.sourceSchema = sourceSchema;
		initializeStatic();

	}
	
	/**
	 * Map of column family to Map of static columns in hbase table and data 
	 * type of it's value
	 */
	public Map<String, Map<String, Class>> staticColumns = null;

	/**
	 * Map of column family to map of static prefixes of dynamic columns and
	 * data type of it's value
	 */
	public Map<String, Map<String, Class>> dynamicColumnsPrefixes = null;

	/**
	 * Map of column family to map of static suffixes of dynamic columns and
	 * data type of it's value
	 */
	public Map<String, Map<String, Class>> dynamicColumnsSuffixes = null;

	/**
	 * If dyanmic part within column names has multiple components, this field 
	 * stores separator used between them 
	 * ex. for <X> = S|1017|I0001 the separator is "|"
	 */
	private  String dynamicColumnsSeparator = null;
	
	
	/**
	 * If row key has multiple components, this field stores separator used 
	 * between them 
	 */
	private  String rowKeySeparator = null;
	
	
	/**
	 * List of component names within dynamic part of column name
	 */
	private  List<String> dynamicPartNames = null;

	private Logger log = LoggerFactory.getLogger(this.getClass().getCanonicalName());
	
	/**
	 * @return 
	 * 		staticColumns map
	 * @see #staticColumns
	 */
	protected Map<String, Map<String, Class>> defineStaticColumns() {
		return hBaseExplorer.getStaticColumns(sourceSchema);
	}

	/**
	 * @return
	 * 		dynamicColumnsPrefixes map
	 * @see #dynamicColumnsPrefixes
	 */
	protected Map<String, Map<String, Class>> defineDynamicColumnsPrefixes() {
		return hBaseExplorer.getDynamicColumnPrefixes(sourceSchema);
	}

	/**
	 * @return
	 * 		dynamicColumnsSuffixes map
	 * @see #dynamicColumnsSuffixes
	 */
	protected Map<String, Map<String, Class>> defineDynamicColumnsSuffixes() {
		return hBaseExplorer.getDynamicColumnSuffixes(sourceSchema);
	}

	/**
	 * @return
	 * 		dynamicColumnsSeparator 
	 * @see #dynamicColumnsSeparator
	 */
	protected String defineDynamicColumnsSeparator() {
		return hBaseExplorer.getDynamicColumnsSeparator(sourceSchema);
	}
	
	
	/**
	 * @return
	 * 		rowKeySeparator
	 * @see #rowKeySeparator
	 */
	protected String defineRowKeySeparator() {
		return hBaseExplorer.getRowKeySeparator(sourceSchema);
	}

	/**
	 * @return
	 * 		define the names for componenets in dynamic column names i.e. in &lt;X&gt;.
	 * 		Ex. for <X> = S|1017|I0001 it will be {"provider","fy","inum"}.
	 * 		The components that we want to ignore can be given value "SKIP"
	 * @see #dynamicPartNames
	 */
	protected List<String> defineDynamicPartNames() {
		return hBaseExplorer.getDynamicPartNames(sourceSchema);
	}

	/**
	 * Parse the rowkey and returns the list of columns names and values
	 * corresponding to it along with salt byte[] if any.
	 * If this returns no columns, the entire row will be ignored
	 * @param 
	 * 		rowKey byte[] row key
	 * @return
	 * 		DataRecord object containing columns from parsed row key in the form of tuples
	 * @throws InvalidSchemaException 
	 */
	protected DataRecord parseRowKey(byte[] rowKeyBytes) throws InvalidSchemaException {
		
		List<Tuple> rowkeyColumns = new ArrayList<>();

        String rowKey = Bytes.toString(rowKeyBytes);
		
		byte[] salt;
		byte[] rowKeyBytes1;

		boolean isHashed = hBaseExplorer.isRowKeyHashed(sourceSchema);
		int saltSize;
		String[] rowKeyParts = null;
		// if row has hashed field
		if(isHashed) {
			//salt[0] = rowKeyBytes[0];
            saltSize = hBaseExplorer.getRowKeyHashSizeBytes(sourceSchema);
			salt = new byte[saltSize];
			System.arraycopy(rowKeyBytes, 0, salt, 0, saltSize);
			
			// getting a byte array after hash field for splitting
			int rowKeyBytes1Length = rowKeyBytes.length - saltSize;
			if(rowKeyBytes1Length > 0)
			{
				rowKeyBytes1 = new byte[rowKeyBytes1Length];
				System.arraycopy(rowKeyBytes, saltSize, rowKeyBytes1, 0, rowKeyBytes1Length);
				String rowKey1 = Bytes.toString(rowKeyBytes1);
				// after separating hash 
				// rowkey1 = |fy|invoice-type|rtin|stin 
				// so after splitting length = 5 
				if(hBaseExplorer.getRowkeyFieldNames(sourceSchema).size() > 1) {
					rowKeyParts = rowKey1.split(this.getRowKeySeparator());
				} else {
					rowKeyParts = new String[] {rowKey1};
				}
			}	
		} else {
			// if row has no hashed field
			if(hBaseExplorer.getRowkeyFieldNames(sourceSchema).size() > 1) {
				rowKeyParts = rowKey.split(this.getRowKeySeparator());
			} else {
				rowKeyParts = new String[] {rowKey};
			}
		}
		
		List<RowkeyField> rowKeyFields = null;
		try {
			//rowKeyFields = schemaInfo.get_rowkey_field_names(schemaName);
			rowKeyFields = hBaseExplorer.getRowkeyFields(sourceSchema);
		} catch (InvalidSchemaException e) {
			throw e;
		}

		if(!hBaseExplorer.isAValidRowKey(sourceSchema, rowKeyParts)) {
            return null;
        }

        /*
		//System.out.println("Length   --> "+rowKeyParts.length + "  " + rowKeyFields.size());
		if(rowKeyParts.length!=rowKeyFields.size() ){
			return null;
		}
		//System.out.println("Row key --> "+rowKey);
		int j=0;
		// Added to filter INVOICE_TYPES
		RowKey rowkey = schemaInfo.getSchema_rowkey(schemaName);
		for(RowKeyField rkfield : rowkey.getRowkey_columns()) {
			j++;
			if(rkfield.is_literal()) {
				if(!rkfield.get_literal_value().equals(rowKeyParts[j-1])) {
					//System.out.println("rkfield  --> "+rkfield.get_literal_value()+" "+rowKeyParts[j-1]);
					return null;
				}
			}
		}
		*/
		
		for(int i=0; i<rowKeyFields.size(); i++) {
			if(!rowKeyFields.get(i).isHashed()){
				rowkeyColumns.add(Tuple.rowkeyColumn(rowKeyFields.get(i).getName(), rowKeyParts[i]));
			}
		}
		
		return new DataRecord(rowkeyColumns, Collections.emptyList());
	}

	/**
	 * @return
	 * 		returns escaped dynamicColumnsSeparator
	 * @see #dynamicColumnsSeparator
	 */
	public  String getDynamicColumnsSeparator() {
		if (dynamicColumnsSeparator != null)
			return "\\" + dynamicColumnsSeparator;
		else
			return dynamicColumnsSeparator;
	}

	/**
	 * @return
	 * 		escaped rowKeySeparator
	 * @see #rowKeySeparator
	 */
	public  String getRowKeySeparator() {
		if (rowKeySeparator != null)
			return "\\" + rowKeySeparator;
		else
			return rowKeySeparator;
	}
	
	/**
	 * @return
	 * 		dynamicPartNames
	 * @see #dynamicPartNames
	 */
	public  List<String> getDynamicPartNames() {
		return dynamicPartNames;
	}

	
	/**
	 * Initializes all the static members of the class
	 */
	public void initializeStatic() {
		staticColumns = defineStaticColumns();
		dynamicColumnsPrefixes = defineDynamicColumnsPrefixes();
		dynamicColumnsSuffixes = defineDynamicColumnsSuffixes();
		dynamicColumnsSeparator = defineDynamicColumnsSeparator();
		rowKeySeparator = defineRowKeySeparator();
		dynamicPartNames = defineDynamicPartNames();
	}

	/**
	 * @return
	 * 		boolean indicating whether this table has dynamic columns or not
	 */
	public boolean hasDynamicColumns() {
		return ((dynamicColumnsPrefixes != null && !dynamicColumnsPrefixes.isEmpty())
				|| (dynamicColumnsSuffixes != null && !dynamicColumnsSuffixes.isEmpty()));
	}

	/**
	 * @param columnFamily 	
	 * 			hbase table columnFamily
	 * @param columnName	
	 * 			hbase table columnName
	 * @return
	 * 			boolean value indicating whether provided column is static or not
	 */
	public boolean isStaticColumn(String columnFamily, String columnName) {
		boolean present;

		if (!staticColumns.containsKey(columnFamily)) {
			present = false;
		} else if (staticColumns.get(columnFamily).containsKey(columnName)) {
			present = true;
		} else {
			present = false;
		}

		return present;
	}

	/**
	 * Wrapper for storing information about parsed dynamic column, like 
	 * staticPart, dynamic part of the column name and data type of it's value
	 *
	 */
	private  class DynamicColumnComponents {
		final String staticPart, dynamicPart;
		final Class dataType;
		final DynamicColumnType dynamicColType;

		DynamicColumnComponents(String staticPart, String dynamicPart, DynamicColumnType dynamicColType, Class dataType) {
			this.staticPart = staticPart;
			this.dynamicPart = dynamicPart;
			this.dataType = dataType;
			this.dynamicColType = dynamicColType;
		}

		@Override
		public String toString() {
			return "staticPart: " + staticPart + " dynamic part: " + dynamicPart + " dynamicColType: " + dynamicColType+ " dataType: " + dataType;
		}
	}

	/**
	 * Parses the provided dynamic column
	 * Assumption is that columnNameFromHBase is a dynamic column
	 * 
	 * @param columnFamily 
	 * 			hbase table columnFamily
	 * @param columnNameFromHBase
	 * 			hbase table columnName
	 * @return
	 * 			DynamicColumnComponents containing information after parsing 
	 * 			the provided dynamic column
	 */
	public DynamicColumnComponents getDynamicColumnComponents(String columnFamily, String columnNameFromHBase) {
		
		Map<String, Class> dynamicColumnsPrefixesMap = dynamicColumnsPrefixes.getOrDefault(columnFamily,
				Collections.emptyMap());

		int candidatePatternLength = 0;
		Class candidateDatatype = null;
		String staticPart = null;
		String dynamicPart = null;
		DynamicColumnType dynamicColumnType=null;
		
		for (Map.Entry<String, Class> entry : dynamicColumnsPrefixesMap.entrySet()) {
			String prefix = entry.getKey();

			// we are allowing blank matches in prefixes only, for handling
			// columns having no prefix/suffix i.e. <X>
			if (columnNameFromHBase.startsWith(prefix)
					&& (candidatePatternLength == 0 || prefix.length() > candidatePatternLength)) {
				candidateDatatype = entry.getValue();
				candidatePatternLength = prefix.length();
				dynamicPart = columnNameFromHBase.substring(prefix.length());
				staticPart = prefix;
				dynamicColumnType = DynamicColumnType.STATIC_PREFIX;
			}
		}

		Map<String, Class> dynamicColumnsSuffixesMap = dynamicColumnsSuffixes.getOrDefault(columnFamily,
				Collections.emptyMap());

		for (Map.Entry<String, Class> entry : dynamicColumnsSuffixesMap.entrySet()) {
			String suffix = entry.getKey();

			if (columnNameFromHBase.endsWith(suffix) && suffix.length() > candidatePatternLength) {
				candidateDatatype = entry.getValue();
				candidatePatternLength = suffix.length();
				dynamicPart = columnNameFromHBase.substring(0, columnNameFromHBase.length() - suffix.length());
				staticPart = suffix;
				dynamicColumnType = DynamicColumnType.STATIC_SUFFIX;
			}

		}

		return new DynamicColumnComponents(staticPart, dynamicPart, dynamicColumnType, candidateDatatype);
	}

	/**
	 * Parses the provided dynamic column name,value and returns Tuple corresponding to it.
	 * Assumption is that columnNameFromHBase is a dynamic column.
	 * 
	 * @param columnFamily 
	 * 			hbase table columnFamily
	 * @param columnNameFromHBase
	 * 			hbase table columnName
	 * @param valueBytes
	 * 			byte[] of value stored in this hbase column
	 * @return
	 * 			Tuple containing information after parsing the provided 
	 * 			dynamic column,value. If this column is to be filtered out returns null
	 * @throws IOException
	 * @throws ParseException
	 */
	protected Tuple parseDynamicColumn(String columnFamily, String columnNameFromHBase, byte[] valueBytes)
			throws ParseException {
		DynamicColumnComponents components = getDynamicColumnComponents(columnFamily, columnNameFromHBase);

		String columnValue = DataTypeUtil.parseValue(valueBytes, components.dataType);

		String dataFormat = hBaseExplorer.getColumnDataFormat(sourceSchema, components.staticPart);
		if (dataFormat != null) {
			columnValue = FormatDateUtil.FormatDate(dataFormat, columnValue);
		}

		if (null == components.staticPart && null == components.dynamicPart && null == columnValue) {
			log.error("****** Found a column not present in source schema cf: " + columnFamily + " cn: "
					+ columnNameFromHBase);
			return null;
		} else {
			return Tuple.dynamicColumn(columnFamily, components.staticPart, columnValue, components.dynamicPart,
					components.dynamicColType, components.dataType);
		}
	}

	/**
	 * Converts the provided static column's value from byte[] to string. 
	 * @param columnFamily
	 * 			hbase table columnFamily
	 * @param columnName
	 * 			hbase table static columnName
	 * @param columnValue
	 * 			byte[] of the columnValue 
	 * @return
	 * 			columnValue of the string type
	 * @throws IOException
	 * @throws ParseException
	 */
	public String getStaticColumnValue(String columnFamily, String columnName, byte[] columnValue)
			throws ParseException {
		Class valueClass = staticColumns.get(columnFamily).get(columnName);
		String dataFormat = hBaseExplorer.getColumnDataFormat(sourceSchema, columnName);
		// log.info("******dataFormat " + dataFormat);
		if (dataFormat == null)
			return DataTypeUtil.parseValue(columnValue, valueClass);
		else {
			String value = DataTypeUtil.parseValue(columnValue, valueClass);
			return FormatDateUtil.FormatDate(dataFormat, value);
		}
	}

	/**
	 * @param columnFamily
	 * 			hbase table columnFamily
	 * @param columnName
	 * 			hbase table static columnName
	 * @return
	 * 			Class representing data type of the provided column name
	 * @throws IOException
	 */
	public Class getStaticColumnDataType(String columnFamily, String columnName) {
		Class valueClass = staticColumns.get(columnFamily).get(columnName);
		return valueClass;
	}

	/**
	 * @return
	 * 		Set of Column family names of the static columns defined in the source schema.
	 */
	public Set<String> getStaticColumnFamilies() {
		return staticColumns.keySet();
	}

	/**
	 * @param columnFamily
	 * 			hbase table columnFamily
	 * @return
	 * 			Set of all static column names defined in the source schema under provided columnFamily.
	 */
	public Set<String> getStaticColumns(String columnFamily) {
		return staticColumns.get(columnFamily).keySet();
	}

	/**
	 * @return
	 * 		Name of the source schema. 
	 */
	public String getSchemaName() {
		return sourceSchema;
	}
	
	public String getTableName() {
		return hBaseExplorer.getTableName(sourceSchema);
	}
	
	public List<HBaseColumn> getAllColumns(){
		return hBaseExplorer.getAllColumns(sourceSchema);
	}
}
