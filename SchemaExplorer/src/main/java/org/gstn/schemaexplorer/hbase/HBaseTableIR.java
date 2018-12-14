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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.sql.SqlBean;

import com.google.gson.JsonElement;

/**
 * This class stores complete schema wise information about all the table
 * mentioned in the schema file
 * 
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class HBaseTableIR implements Serializable {

	private HashMap<String, Rowkey> schemaRowkey;
	private HashMap<String, HBaseColumnList> schemaColumnFields;
	private HashMap<String, String> schemaDdls;
	private HashMap<String, String> schemaTableName;

	public HBaseTableIR() {
		schemaRowkey = new HashMap<>();
		schemaColumnFields = new HashMap<>();
		schemaDdls = new HashMap<>();
		schemaTableName = new HashMap<>();
	}

	/*
	 * This function return whether the schema is Tall Table or not
	 */
	public boolean isTallTable(String schema) {
		return schemaColumnFields.get(schema).isTallTable();
	}

	public boolean containsJsonColumn(String schema) {
		return schemaColumnFields.get(schema).containsJsonColumn();
	}

	public boolean isRowKeyHashed(String schema) {
		return schemaRowkey.get(schema).isHashed();
	}

	public void addTablename(String schema, String table) {
		schemaTableName.put(schema, table);
	}

	public String getTableName(String schema) {
		return schemaTableName.get(schema);
	}

	public Set<String> getSchemaNames() {
		return schemaTableName.keySet();
	}

	public void addRowkeyField(String schema, RowkeyField f) {

		// initialize the RowKey() object, if 'f' is the very first component of
		// the schema's row key.
		if (!schemaRowkey.containsKey(schema)) {
			schemaRowkey.put(schema, new Rowkey());
		}
		schemaRowkey.get(schema).addRowkeyField(f);
	}

	public void addColumn(String schema, String parentPath, HBaseColumn column) throws InvalidSchemaException {
		if (!schemaColumnFields.containsKey(schema)) {
			schemaColumnFields.put(schema, new HBaseColumnList());
		}

		// check if the column field is also present in the row key.
		// we do not allow this case.
		if (schemaRowkey.get(schema).isColumnPresent(column.getColumnName()))
			throw new InvalidSchemaException("In schema " + schema + ", column " + column.getColumnName()
					+ " is also present in rowkey. Please fix and retry");

		schemaColumnFields.get(schema).addColumn(parentPath, column);
	}

	public void addDdl(String schema, String ddl) throws InvalidSchemaException {
		if (schemaDdls.containsKey(schema))
			throw new InvalidSchemaException(
					"Attempt to provide duplicate schema : " + schema + ". Please fix schema and re-try");

		schemaDdls.put(schema, ddl);
	}

	public String getDdl(String schema) throws HQLException {
		if (schemaDdls.containsKey(schema))
			return schemaDdls.get(schema);
		else
			throw new HQLException(schema + " not present");
	}

	/**
	 * This method generates the HBase row key taking into account the actual
	 * row key fields for which value has been provided in condition clause
	 * 
	 * @param schema
	 *            - name of the schema
	 * @param query
	 *            - SqlBean object which stores information about input query
	 * @return byte array of generated row key
	 * @throws HQLException
	 *             if hash size specified in row key is more that 1 byte
	 */
	public byte[] getHbaseRowkey(String schema, SqlBean query) throws HQLException {
		if (schemaDdls.containsKey(schema))
			return schemaRowkey.get(schema).getHbaseRowkey(query);
		else
			throw new HQLException("getHbaseRowkey: " + schema + " not present");
	}

	public boolean isColumnPresentInRowkey(String schema, String column) {
		return schemaRowkey.get(schema).isColumnPresent(column);
	}

	public boolean isColumnPresentInData(String schema, String columnFamily, String column) {
		return schemaColumnFields.get(schema).isColumnPresent(columnFamily, column);
	}

	public String getColumnDataType(String schema, String columnFamily, String column) throws InvalidSchemaException {
		return schemaColumnFields.get(schema).getColumnDataType(columnFamily, column);
	}

	public Class<?> getColumnDataTypeClass(String schema, String columnFamily, String column)
			throws InvalidSchemaException {
		return schemaColumnFields.get(schema).getColumnDataTypeClass(columnFamily, column);
	}

	public boolean isStaticColumn(String schema, String columnFamily, String column) throws InvalidSchemaException {
		return schemaColumnFields.get(schema).isStaticColumn(columnFamily, column);
	}

	public boolean isDynamicColumn(String schema, String columnFamily, String column) throws InvalidSchemaException {
		return (!isStaticColumn(schema, columnFamily, column));
	}

	// Check if input string forms a dynamic component in any of the dynamic
	// fields of given table, column family
	public boolean isDynamicComponent(String schema, String component) throws HQLException {
		return schemaColumnFields.get(schema).isDynamicComponent(component);
	}

	/**
	 * This method checks if input string is present as one of the components in
	 * dynamic part
	 * 
	 * @param schema
	 *            - name of the target schema
	 * @param component
	 *            - input string to be checked
	 * @return true if input string is a component of dynamic part, false
	 *         otherwise
	 */
	public boolean isColumnPresentInDynamicParts(String schema, String component) {
		return schemaColumnFields.get(schema).isPartOfDynamicComponent(component);
	}

	/**
	 * This method returns all the column names which match input column family
	 * and column name parameters for given schema
	 * 
	 * @param schema
	 *            - name of the schema
	 * @param columnFamilyPattern
	 *            - column family pattern to be matched
	 * @param columnNamePattern
	 *            - column name pattern to be matched
	 * @return set of list of all the column names under a column family
	 * @throws HQLException
	 *             if input schema is invalid
	 */
	public Set<List<String>> getAllColumnNames(String schema, String columnFamilyPattern, String columnNamePattern)
			throws HQLException {
		if (!schemaColumnFields.containsKey(schema))
			throw new HQLException("Unable to fetch column names for " + schema);
		return schemaColumnFields.get(schema).getAllColumnNames(columnFamilyPattern, columnNamePattern);
	}

	/**
	 * This method returns all the JSON columns, for a input schema
	 * 
	 * @param schema
	 *            - name of the target schema
	 * @param columnFamilyPattern
	 *            - column family pattern to be matched
	 * @param columnNamePattern
	 *            - column name pattern to be matched
	 * @return set of JSON column objects
	 */
	public Set<HBaseColumn> getJsonColumnName(String schema, String columnFamilyPattern, String columnNamePattern) {
		return schemaColumnFields.get(schema).getJsonColumnNames(columnFamilyPattern, columnNamePattern);
	}

	/**
	 * This method returns a list of row key component names, for a input schema
	 * 
	 * @param schema
	 *            - name of the schema
	 * @return list of all row key field names
	 * @throws InvalidSchemaException
	 *             if schema is invalid
	 */
	public List<String> getRowkeyFieldNames(String schema) throws InvalidSchemaException {
		if (!schemaRowkey.containsKey(schema)) {
			throw new InvalidSchemaException("Unable to fetch row key details. Schema not present - " + schema);
		}
		return schemaRowkey.get(schema).getRowkeyFieldNames();
	}
	
	public List<RowkeyField> getRowkeyFields(String schema) throws InvalidSchemaException {
		if (!schemaRowkey.containsKey(schema)) {
			throw new InvalidSchemaException("Unable to fetch row key details. Schema not present - " + schema);
		}
		return schemaRowkey.get(schema).getRowkeyColumns();
	}
	
	/**
	 * This method returns a list of non hash row key component names, for a input schema
	 * 
	 * @param schema
	 *            - name of the schema
	 * @return list of all row key field names
	 * @throws InvalidSchemaException
	 *             if schema is invalid
	 */
	public List<String> getNonHashRowkeyFieldNames(String schema) throws InvalidSchemaException {
		if (!schemaRowkey.containsKey(schema)) {
			throw new InvalidSchemaException("Unable to fetch row key details. Schema not present - " + schema);
		}
		return schemaRowkey.get(schema).getNonHashRowkeyFieldNames();
	}

	public Rowkey getSchemaRowkey(String schema) {
		return schemaRowkey.get(schema);
	}

	/**
	 * This method return a set of names of all columns, rows and dynamic part
	 * components for input schema
	 * 
	 * @param schema
	 *            - name of the schema
	 * @return set of strings of all columns, rows and dynamic part components
	 */
	public List<String> getAllFieldNames(String schema) {
		List<String> result = schemaRowkey.get(schema).getRowkeyFieldNames();
		result.addAll(schemaColumnFields.get(schema).getDynamicPartNames());
		result.addAll(schemaColumnFields.get(schema).getColumnNamesWithJson());
		return result;
	}
	
	public List<HBaseColumn> getAllColumns(String schema){
		return schemaColumnFields.get(schema).getAllColumns();
	}
	
	public Map<String,Class> getSchemaJsonColumns(String schema){
		return schemaColumnFields.get(schema).getSchemaJsonColumns();
	}
	
	public void setDynamicPartSeparator(String schema, String dynamicPartSeparator) {
		schemaColumnFields.get(schema).setDynamicPartSeparator(dynamicPartSeparator);
	}

	public void addDynamicPartName(String schema, String dynamicPartName) {
		schemaColumnFields.get(schema).addDynamicPartName(dynamicPartName);
	}

	/**
	 * This method returns map of column family to map of column qualifier to
	 * its data type for all static columns, for input schema
	 * 
	 * @param schema
	 *            - name of the schema
	 * @return map of column family to map of column qualifier to its data type
	 */
	public Map<String, Map<String, Class>> getStaticColumns(String schema) {
		return schemaColumnFields.get(schema).getStaticColumns();
	}

	/**
	 * This method returns map of column family to map of column qualifier
	 * prefix to its data type for all dynamic columns, for input schema
	 * 
	 * @param schema
	 *            - name of the schema
	 * @return map of column family to map of column qualifier prefix to its
	 *         data type
	 */
	public Map<String, Map<String, Class>> getDynamicColumnPrefixes(String schema) {
		return schemaColumnFields.get(schema).getDynamicColumnPrefixes();
	}

	/**
	 * This method returns map of column family to map of column qualifier
	 * suffix to its data type for all dynamic columns, for input schema
	 * 
	 * @param schema
	 *            - name of the schema
	 * @return map of column family to map of column qualifier suffix to its
	 *         data type
	 */
	public Map<String, Map<String, Class>> getDynamicColumnSuffixes(String schema) {
		return schemaColumnFields.get(schema).getDynamicColumnSuffixes();
	}

	public String getDynamicColumnsSeparator(String schema) {
		return schemaColumnFields.get(schema).getDynamicPartSeparator();
	}

	public List<String> getDynamicPartNames(String schema) {
		return schemaColumnFields.get(schema).getDynamicPartNames();
	}

	public String getColumnDefaultValue(String schema, String columnName) {
		return schemaColumnFields.get(schema).getColumnDefaultValue(columnName);
	}

	public Class<?> getColumnDataType(String schema, String columnName) {
		return schemaColumnFields.get(schema).getColumnDataType(columnName);
	}

	public int getRowKeyHashSizeBytes(String schema) {
		return schemaRowkey.get(schema).getHashSizeBytes();
	}

	/**
	 * Checks if the input array is a valid instance of the row key for the
	 * input schema
	 * 
	 * @param schema
	 *            - name of the schema
	 * @param values
	 *            - array of values of row key fields
	 * @return true if input array is valid, false otherwise
	 */
	public boolean isAValidRowKey(String schema, String[] values) {
		return schemaRowkey.get(schema).isAValidRowKey(values);
	}

	public String getColumnDataFormat(String schema, String columnName) {
		return schemaColumnFields.get(schema).getColumnDataFormat(columnName);
	}

	public void setRowKeySeparator(String sourceSchema, String rowkeySeparator) {
		schemaRowkey.get(sourceSchema).setRowkeySeparator(rowkeySeparator);
	}

	public String getRowKeySeparator(String sourceSchema) {
		return schemaRowkey.get(sourceSchema).getRowkeySeparator();
	}

	public boolean checkKey(String targetSchema, String parentPath, String currentPath, String key, JsonElement value) {
		return schemaColumnFields.get(targetSchema).checkKey(parentPath, currentPath, key, value);
	}

	public boolean isSchemaDefined(String targetSchema) {
		return schemaDdls.containsKey(targetSchema);

	}

	public boolean isRowkeyFieldHashed(String targetSchema, String rowkeyField) {
		return schemaRowkey.get(targetSchema).isRowkeyFieldHashed(rowkeyField);
	}

	public boolean isRowkeyFieldLiteral(String targetSchema, String rowkeyField) {
		return schemaRowkey.get(targetSchema).isRowkeyFieldLiteral(rowkeyField);
	}

	public String getRowkeyFieldLiteralValue(String targetSchema, String rowkeyField) {
		return schemaRowkey.get(targetSchema).getRowkeyFieldLiteralValue(rowkeyField);
	}

}
