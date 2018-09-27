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
package org.gstn.schemaexplorer.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.gstn.schemaexplorer.exception.ColumnFamilyNotFoundException;
import org.gstn.schemaexplorer.exception.ColumnNotFoundException;
import org.gstn.schemaexplorer.exception.InvalidColumnException;
import org.gstn.schemaexplorer.exception.InvalidRecordTypeExcepton;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.gstn.schemaexplorer.util.DataTypeUtil;

/**
 * This class stores of all the processed data in its final form
 *
 */
public class DataRecord {
	// Map of row key field name to value map
	private final Map<String, Tuple> rowKeyTupleMap;
	private List<Tuple> rowKeyTupleList;
	// Map of columnFamily to Map of Column Name to Column Tuple
	private final Map<String, Map<String, Tuple>> columnFamilycolumnNameTupleMap;
	private Map<String, Tuple> columnNameTupleMap;
	private List<Tuple> columnNameTupleList;
	private RecordType recordType;

	private boolean regexInKey;
	private boolean dynamicPartsInKey;
	
	private byte[] salt;

	public DataRecord() {
		rowKeyTupleList = new ArrayList<>();
		rowKeyTupleMap = new HashMap<>();

		columnNameTupleList = new ArrayList<>();
		columnFamilycolumnNameTupleMap = new HashMap<>();

		columnNameTupleMap = new HashMap<>();

		recordType = RecordType.KEY_VALUE;
	}

	public DataRecord(List<Tuple> keyList, List<Tuple> valueList) {
		keyList.forEach(tuple -> {
			assert (null != tuple);
		});
		valueList.forEach(tuple -> {
			assert (null != tuple);
		});

		rowKeyTupleList = new ArrayList<>();
		columnNameTupleList = new ArrayList<>();
		columnNameTupleMap = new HashMap<>();

		rowKeyTupleList.addAll(keyList);
		columnNameTupleList.addAll(valueList);

		rowKeyTupleMap = new HashMap<>();
		keyList.forEach(t -> rowKeyTupleMap.put(t.getColumnName(), t));
		columnNameTupleMap.putAll(rowKeyTupleMap);

		columnFamilycolumnNameTupleMap = new HashMap<>();
		for (Tuple t : valueList) {
			String columnFamily = t.getColumnFamily();
			String columnName = t.getColumnName();

			if (!columnFamilycolumnNameTupleMap.containsKey(columnFamily)) {
				columnFamilycolumnNameTupleMap.put(columnFamily, new HashMap<>());
			}

			columnFamilycolumnNameTupleMap.get(columnFamily).put(columnName, t);

			columnNameTupleMap.put(columnName, t);
		}
		recordType = RecordType.KEY_VALUE;
	}

	public DataRecord(List<Tuple> tupleList) {

		rowKeyTupleMap = null;
		rowKeyTupleList = null;
		columnFamilycolumnNameTupleMap = new HashMap<>();
		columnNameTupleList = new ArrayList<>();
		columnNameTupleMap = new HashMap<>();
		Map<String, Tuple> cnMap = new HashMap<>();

		for (Tuple tuple : tupleList) {
			cnMap.put(tuple.getColumnName(), tuple);
		}
		columnNameTupleList.addAll(tupleList);
		// cf null
		columnFamilycolumnNameTupleMap.put(null, cnMap);

		columnNameTupleMap.putAll(cnMap);

		recordType = RecordType.DELIMITED_RECORD;
	}

	public boolean isRowkeyListEmpty() {
		return rowKeyTupleList.size() == 0;
	}

	public void addTupleToRowkey(Tuple t) throws InvalidRecordTypeExcepton, InvalidColumnException {
		if (t != null) {
			if (recordType == RecordType.KEY_VALUE) {
				rowKeyTupleMap.put(t.getColumnName(), t);
				rowKeyTupleList.add(t);
				columnNameTupleMap.put(t.getColumnName(), t);

			} else {
				throw new InvalidRecordTypeExcepton("Attempted to add column " + t.getColumnName()
						+ " into rowkey of record, but the record type is not " + RecordType.KEY_VALUE);
			}
		} else {
			throw new InvalidColumnException("Attempted to add null column into row key columns of the DataRecord");
		}
	}

	public void addTupleToColumn(String columnFamily, String columnName, Tuple t) throws InvalidColumnException {
		if (null != t) {

			if (!columnFamilycolumnNameTupleMap.containsKey(columnFamily)) {
				columnFamilycolumnNameTupleMap.put(columnFamily, new HashMap<>());
			}

			columnFamilycolumnNameTupleMap.get(columnFamily).put(columnName, t);
			columnNameTupleList.add(t);

			columnNameTupleMap.put(columnName, t);
		} else {
			throw new InvalidColumnException("Attempted to add null column into non row key columns of the DataRecord");
		}
	}

	public void addColumns(Map<String, Map<String, Tuple>> valueMap) throws Exception {
		for (Entry<String, Map<String, Tuple>> cfEntry : valueMap.entrySet()) {
			String cfName = cfEntry.getKey();

			for (Entry<String, Tuple> cnEntry : valueMap.get(cfName).entrySet()) {
				String cName = cnEntry.getKey();
				Tuple tuple = cnEntry.getValue();

				addTupleToColumn(cfName, cName, tuple);
			}
		}
	}

	/**
	 * This method returns a copy of the calling object
	 * 
	 * @return copy of the calling object
	 */
	public DataRecord duplicate() {
		DataRecord copy = new DataRecord();
		copy.columnNameTupleList.addAll(this.columnNameTupleList);
		copy.rowKeyTupleList.addAll(this.rowKeyTupleList);
		copy.rowKeyTupleMap.putAll(this.rowKeyTupleMap);
		copyIntoColumnMap(copy.columnFamilycolumnNameTupleMap, this.columnFamilycolumnNameTupleMap);
		copy.columnNameTupleMap.putAll(this.columnNameTupleMap);
		
		copy.regexInKey=this.regexInKey;
		copy.dynamicPartsInKey=this.dynamicPartsInKey;
		
		return copy;
	}

	/**
	 * This method copies data from one input map to another
	 * 
	 * @param orgMap
	 *            - map to be copied to
	 * @param mapToBeMerged
	 *            - map to be copied from
	 */
	private void copyIntoColumnMap(Map<String, Map<String, Tuple>> orgMap,
			Map<String, Map<String, Tuple>> mapToBeMerged) {
		for (Entry<String, Map<String, Tuple>> cfEntry : mapToBeMerged.entrySet()) {

			Map<String, Tuple> orgCnMap = orgMap.getOrDefault(cfEntry.getKey(), new HashMap<>());

			orgCnMap.putAll(cfEntry.getValue());

			orgMap.put(cfEntry.getKey(), orgCnMap);
		}
	}

	public Tuple getRowkeyTuple(String columnName) throws ColumnNotFoundException {
		if (this.rowKeyTupleMap.containsKey(columnName)) {
			return rowKeyTupleMap.get(columnName);
		} else {
			throw new ColumnNotFoundException("Attempted to fetch column: " + columnName
					+ " from row key columns, but it's not found in row key columns.");
		}
	}

	public Tuple getColumnTuple(String columnName) throws ColumnNotFoundException {
		if (columnNameTupleMap.containsKey(columnName)) {
			return columnNameTupleMap.get(columnName);
		} else {
			throw new ColumnNotFoundException("Attempted to fetch column: " + columnName + ", but it's not found.");
		}
	}

	public String getColumnTupleValue(String columnName) throws ColumnNotFoundException {
		if (columnNameTupleMap.containsKey(columnName)) {
			return columnNameTupleMap.get(columnName).getColumnValue();
		} else {
			throw new ColumnNotFoundException("Attempted to fetch column: " + columnName + ", but it's not found.");
		}
	}
	
	/**
	 * This method returns a list of all the tuples associated with input column
	 * family
	 * 
	 * @param columnFamily
	 *            - column family
	 * @return list of tuples
	 * @throws ColumnFamilyNotFoundException
	 *             if column family is invalid
	 */
	public List<Tuple> getTuplesInCF(String columnFamily) throws ColumnFamilyNotFoundException {
		if (columnFamilycolumnNameTupleMap.containsKey(columnFamily)) {
			List<Tuple> tuplesInCF = new ArrayList<>();

			tuplesInCF.addAll(columnFamilycolumnNameTupleMap.get(columnFamily).values());

			return tuplesInCF;
		} else {
			throw new ColumnFamilyNotFoundException(
					"Attempted to fetch columns for columnfamily " + columnFamily + ", which is not found.");
		}
	}

	public boolean isColumnNamePresent(String columnName) {
		return columnNameTupleMap.containsKey(columnName);
	}
	
	public boolean isColumnNamePresent(String columnFamily, String columnName) {
		Map<String, Tuple> columns = columnFamilycolumnNameTupleMap.get(columnFamily);
		
		if(columns!=null){
			return columns.containsKey(columnName);
		}
		
		return false;
	}

	/**
	 * This method returns all the column tuples as a string, with each tuple
	 * between quote character and separated using a delimiter
	 * 
	 * @param delimiter
	 *            - separator between two tuples
	 * @param quoteChar
	 *            - character to enclose the tuple between
	 * @return all the tuples merged into a single string
	 */
	private String getValueAsString(String delimiter, String quoteChar) {
		return tuplesToString(columnNameTupleList, delimiter, quoteChar);
	}

	private String tuplesToString(Collection<Tuple> tupleList, String delimiter, String quoteChar) {
		StringBuilder sb = new StringBuilder();

		for (Tuple tuple : tupleList) {
			String val = tuple.getColumnValue();
			if (val.equals("\\N")) {
				sb.append(quoteChar).append(val).append(quoteChar).append(delimiter);
			} else {
				sb.append(quoteChar).append(StringEscapeUtils.escapeJson(val)).append(quoteChar).append(delimiter);
			}
		}
		//remove last delimiter
		sb.delete(sb.length()-delimiter.length(), sb.length());
		return sb.toString();
	}

	/**
	 * This method returns all the row key field tuples as a string, with each
	 * tuple between quote character and separated using a delimiter
	 * 
	 * @param delimiter
	 *            - separator between two tuples
	 * @param quoteChar
	 *            - character to enclose the tuple between
	 * @return all the tuples merged into a single string
	 * @throws InvalidRecordTypeExcepton
	 */
	public String getKeyAsString(String delimiter, String quoteChar) throws InvalidRecordTypeExcepton {
		if (recordType == RecordType.KEY_VALUE) {
			return tuplesToString(rowKeyTupleList, delimiter, quoteChar);
		} else {
			throw new InvalidRecordTypeExcepton(
					"Attempted to fetch row key string from record, but the record type is not "
							+ RecordType.KEY_VALUE);
		}
	}

	// quoteChar - if individual values of keys need to be quoted, otherwise
	// keep it blank
	public byte[] getSaltedKey(String delimiter, String quoteChar) throws InvalidRecordTypeExcepton {
		String key = getKeyAsString(delimiter, quoteChar);

		if (salt == null) {
			return Bytes.toBytes(key);
		} else {
			return Bytes.add(salt, Bytes.toBytes(delimiter + key));
		}
	}

	public void setSalt(byte[] salt) throws InvalidRecordTypeExcepton {
		if (recordType == RecordType.KEY_VALUE) {
			this.salt = salt;
		} else {
			throw new InvalidRecordTypeExcepton(
					"Attempted to set salt byte, but the record type is not " + RecordType.KEY_VALUE);
		}
	}

	public String getRecord(String delimiter, String quoteChar) {

		String keyStr, output = null;
		String valStr = getValueAsString(delimiter, quoteChar);

		if (recordType == RecordType.KEY_VALUE) {
			try {
				keyStr = getKeyAsString(delimiter, quoteChar);
				output = keyStr + delimiter + valStr;
			} catch (InvalidRecordTypeExcepton e) {
				// as recordType condition is checked before calling
				// getKeyAsString this exception will never come
			}
		} else {
			output = valStr;
		}
		return output;
	}

	public Put getHBasePut(HBaseTableExplorer hBaseExplorer, String targetTable, String rowKeySeparator)
			throws InvalidSchemaException, InvalidRecordTypeExcepton {
		byte[] rowkey = getSaltedKey(rowKeySeparator, "");

		Put put = new Put(rowkey);

		for (String cf : columnFamilycolumnNameTupleMap.keySet()) {
			Map<String, Tuple> cnMap = columnFamilycolumnNameTupleMap.get(cf);

			for (String cn : cnMap.keySet()) {
				Tuple tuple = cnMap.get(cn);

				byte[] cfBytes = Bytes.toBytes(cf);
				byte[] nm = Bytes.toBytes(cn);
				byte[] val = null;

				String value = tuple.getColumnValue();
				if (value != null) {
					val = parseToByteArray(hBaseExplorer, targetTable, tuple.getColumnFamily(), tuple.getColumnName(),
							value);
				}
				put.addColumn(cfBytes, nm, val);
			}
		}
		return put;
	}

	private byte[] parseToByteArray(HBaseTableExplorer hBaseExplorer, String targetTable, String cf, String cn, String value)
			throws InvalidSchemaException {
		Class<?> dataTypeClass = hBaseExplorer.getColumnDataTypeClass(targetTable, cf, cn);

		return DataTypeUtil.parseStringToByteArray(value, dataTypeClass);
	}

	@Override
	public String toString() {
		return "DataRecord [keyMap=" + rowKeyTupleMap + ", valueMap=" + columnFamilycolumnNameTupleMap + ", recordType="
				+ recordType + ", salt=" + Arrays.toString(salt) + "]";
	}

	public Set<String> getAllColumnNames() {
		return columnNameTupleMap.keySet();
	}

	public Map<String, Map<String, Tuple>> getColumnFamilycolumnNameTupleMap() {
		return columnFamilycolumnNameTupleMap;
	}

	public boolean isRegexInKey() {
		return regexInKey;
	}

	public void setRegexInKey(boolean regexInKey) {
		this.regexInKey = regexInKey;
	}

	public boolean isDynamicPartsInKey() {
		return dynamicPartsInKey;
	}

	public void setDynamicPartsInKey(boolean dynamicPartsInKey) {
		this.dynamicPartsInKey = dynamicPartsInKey;
	}
	
	public Tuple cloneTupleWithNewCF(String targetCn, String targetCf) throws ColumnNotFoundException {
		return getColumnTuple(targetCn).cloneWithNewCF(targetCf);
	}
}
