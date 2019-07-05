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
package org.gstn.schemaexplorer.hbase;

import static org.gstn.schemaexplorer.target.Constants.PARENT_PATH_SEPARATOR;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.gstn.schemaexplorer.exception.InvalidColumnException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.util.DataTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import com.google.gson.JsonElement;

/**
 * This class stores information about static and dynamic columns, and also
 * dynamic part name and separator between them. It also stores parent path
 * information for JSON attributes, which could be individual columns in target
 * HBase table.
 *
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class HBaseColumnList implements Serializable {

	private Logger logger;
	private Map<String, List<HBaseColumn>> columnList;
	private String dynamicPartSeparator;
	private ArrayList<String> dynamicPartNames;
	private Map<String, List<String>> parentPathColumnsMap;
	private List<HBaseColumn> allColumns;

	public HBaseColumnList() {
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
		columnList = new HashMap<>();
		dynamicPartNames = new ArrayList<>();
	}

	public void addColumn(String parentPath, HBaseColumn f) {
		if (columnList.get(parentPath) == null) {
			columnList.put(parentPath, new ArrayList<>());
		}
		columnList.get(parentPath).add(f);
	}

	public void addDynamicPartName(String name) {
		dynamicPartNames.add(name);
	}

	public String getDynamicPartSeparator() {
		return dynamicPartSeparator;
	}

	public void setDynamicPartSeparator(String dynamicPartSeparator) {
		this.dynamicPartSeparator = dynamicPartSeparator;
	}

	/**
	 * This method returns map of column family to map of column qualifier to
	 * its data type for all static columns
	 * 
	 * @return map of column family to map of column qualifier to its data type
	 */
	Map<String, Map<String, Class>> getStaticColumns() {
		Map<String, Map<String, Class>> staticColumns = new LinkedHashMap<>();
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.isStaticColumn()) {
					Class<?> cl = hBaseColumn.getDataTypeClass();
					String cf = hBaseColumn.getColumnFamily();
					String cn = hBaseColumn.getColumnName();

					if (!staticColumns.containsKey(cf)) {
						staticColumns.put(cf, new LinkedHashMap<>());
					}
					staticColumns.get(cf).put(cn, cl);
				}
			}
		}
		return staticColumns;
	}

	/**
	 * This method returns map of column family to map of column qualifier
	 * prefix to its data type for all dynamic columns
	 * 
	 * @return map of column family to map of column qualifier prefix to its
	 *         data type
	 */
	Map<String, Map<String, Class>> getDynamicColumnPrefixes() {
		Map<String, Map<String, Class>> columnPrefixes = new LinkedHashMap<>();

		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				// prefix not empty or prefix and suffix both empty (for
				// handling columns having no prefix/suffix i.e. <X>)
				if (hBaseColumn.isDynamicColumn() && hBaseColumn.getDynamicPrefix() != null
						&& ((!hBaseColumn.getDynamicPrefix().equals(StringUtils.EMPTY))
								|| (hBaseColumn.getDynamicPrefix().equals(StringUtils.EMPTY)
										&& hBaseColumn.getDynamicSuffix().equals(StringUtils.EMPTY)))) {
					Map<String, Class> column;
					if (columnPrefixes.containsKey(hBaseColumn.getColumnFamily())) {
						column = columnPrefixes.get(hBaseColumn.getColumnFamily());
					} else {
						column = new LinkedHashMap<>();
						columnPrefixes.put(hBaseColumn.getColumnFamily(), column);
					}
					column.put(hBaseColumn.getDynamicPrefix(), hBaseColumn.getDataTypeClass());

				}
			}
		}
		return columnPrefixes;
	}

	/**
	 * This method returns map of column family to map of column qualifier
	 * suffix to its data type for all dynamic columns
	 * 
	 * @return map of column family to map of column qualifier suffix to its
	 *         data type
	 */
	Map<String, Map<String, Class>> getDynamicColumnSuffixes() {
		Map<String, Map<String, Class>> columnSuffixes = new LinkedHashMap<>();

		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.isDynamicColumn() && hBaseColumn.getDynamicSuffix() != null
						&& (!hBaseColumn.getDynamicSuffix().equals(StringUtils.EMPTY))) {
					Map<String, Class> column;
					if (columnSuffixes.containsKey(hBaseColumn.getColumnFamily())) {
						column = columnSuffixes.get(hBaseColumn.getColumnFamily());
					} else {
						column = new LinkedHashMap<>();
						columnSuffixes.put(hBaseColumn.getColumnFamily(), column);
					}
					column.put(hBaseColumn.getDynamicSuffix(), hBaseColumn.getDataTypeClass());
				}
			}
		}
		return columnSuffixes;
	}

	/**
	 * Checks if the column is present
	 * 
	 * @param columnFamily
	 *            - column family of the column
	 * @param columnQualifier
	 *            - column qualifier of the column
	 * @return true if column is present, false otherwise
	 */
	public boolean isColumnPresent(String columnFamily, String columnQualifier) {
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.getColumnName().equals(columnQualifier)
						&& hBaseColumn.getColumnFamily().equals(columnFamily)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * This method searches through all the columns and returns data type of the
	 * column matching input parameters
	 * 
	 * @param columnFamily
	 *            - column family of the column
	 * @param columnQualifier
	 *            - column qualifier of the column
	 * @return data type of the column
	 * @throws InvalidSchemaException
	 *             if the column is not present
	 */
	public String getColumnDataType(String columnFamily, String columnQualifier) throws InvalidSchemaException {
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.getColumnName().equals(columnQualifier)
						&& hBaseColumn.getColumnFamily().equals(columnFamily)) {
					return hBaseColumn.getColumnDataType();
				}
			}
		}
		throw new InvalidSchemaException("Unable to determine data type of " + columnQualifier);
	}

	/**
	 * This method searches through all the columns and returns data type of the
	 * column matching input parameters
	 * 
	 * @param columnFamily
	 *            - column family of the column
	 * @param columnQualifier
	 *            - column qualifier of the column
	 * @return class of data type of the column
	 * @throws InvalidSchemaException
	 *             if the column is not present
	 */
	public Class<?> getColumnDataTypeClass(String columnFamily, String columnQualifier) throws InvalidSchemaException {
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.getColumnName().equals(columnQualifier)
						&& hBaseColumn.getColumnFamily().equals(columnFamily)) {
					return hBaseColumn.getDataTypeClass();
				}
			}
		}
		throw new InvalidSchemaException("Unable to determine data type of " + columnQualifier);
	}

	public boolean isDynamicColumn(String columnFamily, String column) throws InvalidSchemaException {
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.getColumnName().equals(column) && hBaseColumn.getColumnFamily().equals(columnFamily)) {
					return hBaseColumn.isDynamicColumn();
				}
			}
		}
		throw new InvalidSchemaException("Internal Error: Unable to determine if the column is static: " + column);
	}

	public boolean isStaticColumn(String columnFamily, String column) throws InvalidSchemaException {
		return (!isDynamicColumn(columnFamily, column));
	}

	/**
	 * This method checks if the input string is the dynamic part
	 * 
	 * @param dynamicPart
	 *            - string to be checked
	 * @return true if the input string is dynamic part, false otherwise
	 */
	public boolean isDynamicComponent(String dynamicPart) {
		logger.debug("isDynamicComponent: checking for = " + dynamicPart);
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.isDynamicColumn()
						&& hBaseColumn.getDynamicComponent().equals("<" + dynamicPart + ">")) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * This method checks if the input string is part of dynamic component
	 * 
	 * @param dynamicPartName
	 *            - component of the dynamic part to be checked
	 * @return true if input string is part of dynamic component, false
	 *         otherwise
	 */
	public boolean isPartOfDynamicComponent(String dynamicPartName) {
		logger.debug("isPartOfDynamicComponent: checking for dynamic part= " + dynamicPartName);

		for (String dynamicPart : dynamicPartNames) {
			if (dynamicPart.equals(dynamicPartName)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * This method returns all the column names which match input parameters
	 * 
	 * @param columnFamilyPattern
	 *            - column family pattern to be matched
	 * @param columnNamePattern
	 *            - column name pattern to be matched
	 * @return set of list of all the column names under a column family
	 */
	public Set<List<String>> getAllColumnNames(String columnFamilyPattern, String columnNamePattern) {

		Set<List<String>> allColumnNamesList = new HashSet<>();
		Pattern columnFamilyRegex = Pattern.compile(columnFamilyPattern);
		Pattern columnNameRegex = Pattern.compile(columnNamePattern);

		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				List<String> columnData = new ArrayList<>();
				columnData.add(hBaseColumn.getColumnFamily());
				columnData.add(hBaseColumn.getColumnName());

				if (columnFamilyRegex.matcher(hBaseColumn.getColumnFamily()).matches()
						&& columnNameRegex.matcher(hBaseColumn.getColumnName()).matches()) {
					logger.debug("getAllColumnNames: Column " + columnData.toString() + " matches pattern "
							+ columnFamilyPattern + ":" + columnNamePattern);
					allColumnNamesList.add(columnData);
				}
			}
		}
		return allColumnNamesList;
	}

	/**
	 * This method returns all the JSON columns
	 * 
	 * @param columnFamilyPattern
	 *            - column family pattern to be matched
	 * @param columnNamePattern
	 *            - column name pattern to be matched
	 * @return set of JSON column objects
	 */
	public Set<HBaseColumn> getJsonColumnNames(String columnFamilyPattern, String columnNamePattern) {

		HashSet<HBaseColumn> matchingColumns = new HashSet<>();
		Pattern columnFamilyRegex = Pattern.compile(columnFamilyPattern);
		Pattern columnNameRegex = Pattern.compile(columnNamePattern);

		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				ArrayList<String> columnData = new ArrayList<>();
				columnData.add(hBaseColumn.getColumnFamily());
				columnData.add(hBaseColumn.getColumnName());

				if (columnFamilyRegex.matcher(hBaseColumn.getColumnFamily()).matches()
						&& columnNameRegex.matcher(hBaseColumn.getColumnName()).matches()
						&& hBaseColumn.getDataType().equalsIgnoreCase("json")) {
					logger.debug("getJsonColumnName: Column " + columnData.toString() + " matches pattern "
							+ columnFamilyPattern + ":" + columnNamePattern);
					matchingColumns.add(hBaseColumn);
				}
			}
		}
		return matchingColumns;
	}

	public String getCompressionType(String columnFamilyPattern, String columnNamePattern) {
		Pattern columnFamilyRegex = Pattern.compile(columnFamilyPattern);
		Pattern columnNameRegex = Pattern.compile(columnNamePattern);

		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				ArrayList<String> columnData = new ArrayList<>();
				columnData.add(hBaseColumn.getColumnFamily());
				columnData.add(hBaseColumn.getColumnName());

				if (columnFamilyRegex.matcher(hBaseColumn.getColumnFamily()).matches()
						&& columnNameRegex.matcher(hBaseColumn.getColumnName()).matches())
					return hBaseColumn.getCompressionType();
			}
		}
		return "";
	}

	/**
	 * This method converts byte data of a column to its corresponding data type
	 * 
	 * @param columnFamily
	 *            - column family of the column
	 * @param columnName
	 *            - column name of the column
	 * @param columnValueBa
	 *            - values as byte array
	 * @param columnDataType
	 *            - data type of the column
	 * @return converted column value as string
	 * @throws InvalidColumnException
	 *             if column is not found
	 */
	public String getColumnValueAsString(String columnFamily, String columnName, byte[] columnValueBa,
			String columnDataType) throws InvalidColumnException {
		// logger.debug("getColumnValueAsAtring: getting column value for -
		// "+columnFamily+":"+columnName+" of data type="+columnDataType);
		String columnValue = null;

		if (null == columnValueBa) {
			logger.debug("getColumnValueAsString: input columnValueBa is null !!!");
		}

		try {
			if (getCompressionType(columnFamily, columnName).equals("snappy"))
				columnValueBa = Snappy.uncompress(columnValueBa);
		} catch (IOException e) {
			throw new InvalidColumnException("Unable to uncompress data value of " + columnFamily + ":" + columnName);
		}

		if (columnDataType.equals("string"))
			columnValue = Bytes.toString(columnValueBa);
		else if (columnDataType.equals("integer"))
			columnValue = Integer.toString(Bytes.toInt(columnValueBa));
		else if (columnDataType.equals("double"))
			columnValue = Double.toString(Bytes.toDouble(columnValueBa));
		else if (columnDataType.equals("bigdecimal"))
			columnValue = Bytes.toBigDecimal(columnValueBa).toString();
		else if (columnDataType.equals("biginteger"))
			columnValue = (new BigInteger(columnValueBa)).toString();
		else if (columnDataType.equals("json"))
			columnValue = Bytes.toString(columnValueBa);

		return columnValue;
	}

	public List<String> getDynamicPartNames() {
		List<String> copy = new ArrayList<>();
		copy.addAll(dynamicPartNames);
		return copy;
	}

	/**
	 * This method return list of names of all the columns
	 * 
	 * @return list of names of all the columns as string
	 */
	public List<String> getColumnNames() {
		List<String> result = new ArrayList<String>();
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.isDynamicColumn())
					result.add(hBaseColumn.getDynamicPrefix() + hBaseColumn.getDynamicSuffix());
				else
					result.add(hBaseColumn.getColumnName());
			}
		}
		return result;
	}
	
	/**
	 * This method return list of names of all the columns grouped by column families
	 * 
	 * @return list of names of all the columns as string
	 */
	public List<HBaseColumn> getAllColumns() {
		if(allColumns==null){
			allColumns = new ArrayList<>();
			for (String parent : columnList.keySet()) {
				allColumns.addAll(columnList.get(parent));
			}
		}
		return allColumns;
	}

	/**
	 * This method return list of names of all the columns by prefixing all JSON
	 * attribute columns with "JSON."
	 * 
	 * @return list of names of all the columns as string
	 */
	public List<String> getColumnNamesWithJson() {
		List<String> result = new ArrayList<String>();
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.isDynamicColumn())
					result.add(hBaseColumn.getDynamicPrefix() + hBaseColumn.getDynamicSuffix());
				else {
					if (hBaseColumn.isJsonField())
						result.add("JSON." + hBaseColumn.getColumnName());
					else
						result.add(hBaseColumn.getColumnName());
				}

			}
		}
		return result;
	}
	
	
	public Map<String,Class> getSchemaJsonColumns() {
		Map<String,Class> columnNames = new HashMap<>();
		for (String parentPath : columnList.keySet()) {
			for (HBaseColumn column : columnList.get(parentPath)) {
				if (column.isJsonField())
					columnNames.put(column.getColumnName(),DataTypeUtil.getDataTypeClassForHiveDataType(column.getColumnDataType()));
			}
		}
		return columnNames;
	}

	public Class<?> getColumnDataType(String columnName) {
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.getColumnName().equals(columnName)) {
					return hBaseColumn.getDataTypeClass();
				}
			}
		}
		return null;
	}

	public String getColumnDefaultValue(String columnName) {
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.getColumnName().equals(columnName)) {
					return hBaseColumn.getColumnDefaultValue();
				}
			}
		}
		return null;
	}

	/**
	 * This method returns a map of parent path to list of all JSON attribute
	 * columns(without the parent path prefixed) having the key as parent path
	 * in target table
	 * 
	 * @return map of parent path to list of JSON attribute column names(without
	 *         the parent path prefixed)
	 */
	public Map<String, List<String>> getParentPathColumnsMap() {

		if (parentPathColumnsMap == null) {
			parentPathColumnsMap = new HashMap<>();
			for (String parentPath : columnList.keySet()) {
				List<String> columns = new ArrayList<>();
				for (HBaseColumn hBaseColumn : columnList.get(parentPath)) {
					if (hBaseColumn.isJsonField()) {
						String colNameWithoutParentPath;
						if (parentPath == null) {
							colNameWithoutParentPath = hBaseColumn.getColumnName();
						} else {
							colNameWithoutParentPath = hBaseColumn.getColumnName()
									.substring(parentPath.length() + PARENT_PATH_SEPARATOR.length());
						}

						columns.add(colNameWithoutParentPath);
					}
				}
				if (!columns.isEmpty()) {
					parentPathColumnsMap.put(parentPath, columns);
				}
			}
		}
		return parentPathColumnsMap;
	}

	public String getColumnDataFormat(String columnName) {
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.getColumnNameWithoutDynamicComponent().equals(columnName)) {
					return hBaseColumn.getDataFormat();
				}
			}
		}
		return null;
	}

	public boolean containsJsonColumn() {
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.getDataType().equalsIgnoreCase("json")) {
					return true;
				}
			}
		}
		return false;
	}

	public boolean isTallTable() {
		for (String parent : columnList.keySet()) {
			for (HBaseColumn hBaseColumn : columnList.get(parent)) {
				if (hBaseColumn.isDynamicColumn()) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * @return 
	 * 		boolean value indicating whether the input key is to be considered 
	 * 		for flattening or not 
	 */
	public boolean checkKey(String parentPath, String currentPath, String key, JsonElement value) {
		
		boolean considerPrefixCase = false;
		if (value.isJsonObject() || (value.isJsonArray() && value.toString().contains(":"))) {
			considerPrefixCase = true;
		}

		//Check if column having name as key is present under parentPath
		List<String> columns = parentPathColumnsMap.get(parentPath);
		if (columns != null) {
			for (String column : columns) {
				if (key.equals(column)) {
					return true;
				}
			}

		}

		if (considerPrefixCase) {
			// Check if currentPath is present as key or currentPath_<key>_ is 
			// present as prefix of key in parentPathColumnsMap keyset
			for (String parent : parentPathColumnsMap.keySet()) {
				if (parent!=null && (parent.equals(currentPath) || parent.startsWith(currentPath + PARENT_PATH_SEPARATOR))) {
					return true;
				}
			}
		}
		return false;
	}
	
}
