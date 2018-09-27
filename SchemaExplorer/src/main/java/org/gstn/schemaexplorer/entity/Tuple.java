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

/**
 * Class to store information about individual rows, columns and dynamic parts
 * after all the processing is done
 */
public final class Tuple {

	private final String columnFamily;
	private final String columnName;
	private final Class<?> dataType;
	private final String columnValue;
	private final String dynamicPartValue;
	private final boolean raw, dynamicColumn;
	//type of dynamic column whether it has static prefix or static suffix
	private final DynamicColumnType dynamicColumnType;
	/**
	 * Constructor for row key columns
	 * 
	 * @param columnName
	 *            - column name
	 * @param columnValue
	 *            - column value
	 * @param dataType
	 *            - column data type
	 */
	private Tuple(String columnName, String columnValue, Class<?> dataType) {
		this.columnName = columnName;
		this.columnValue = columnValue;
		this.dataType = dataType;
		this.dynamicPartValue = "";
		this.columnFamily = "";
		this.raw = false;
		this.dynamicColumn = false;
		this.dynamicColumnType=null;
	}

	/**
	 * Constructor for static columns
	 * 
	 * @param columnFamily
	 *            - column family
	 * @param columnName
	 *            - column name
	 * @param columnValue
	 *            - column value
	 * @param dataType
	 *            - column data type
	 */
	private Tuple(String columnFamily, String columnName, String columnValue, Class<?> dataType) {
		this.columnFamily = columnFamily;
		this.columnName = columnName;
		this.columnValue = columnValue;
		this.dataType = dataType;
		this.dynamicPartValue = "";
		this.raw = false;
		this.dynamicColumn = false;
		this.dynamicColumnType=null;
	}

	/**
	 * Constructor for dynamic columns
	 * 
	 * @param columnFamily
	 *            - column family
	 * @param columnName
	 *            - column name
	 * @param columnValue
	 *            - column value
	 * @param dynamicPartValue
	 *            - value of dynamic part
	 * @param dataType
	 *            - column data type
	 */
	private Tuple(String columnFamily, String columnName, String columnValue, String dynamicPartValue,
			DynamicColumnType dynamicColumnType, Class<?> dataType) {
		this.columnFamily = columnFamily;
		this.columnName = columnName;
		this.columnValue = columnValue;
		this.dataType = dataType;
		this.raw = false;
		this.dynamicColumn = true;
		this.dynamicPartValue = dynamicPartValue;
		this.dynamicColumnType=dynamicColumnType;
	}

	public static Tuple rowkeyColumn(String columnName, String columnValue) {
		return new Tuple(columnName, columnValue, String.class);
	}

	public static Tuple staticColumn(String columnFamily, String columnName, String columnValue, Class<?> dataType) {
		return new Tuple(columnFamily, columnName, columnValue, dataType);
	}

	public static Tuple dynamicColumn(String columnFamily, String columnName, String columnValue,
			String dynamicPartValue, DynamicColumnType dynamicColumnType, Class<?> dataType) {
		return new Tuple(columnFamily, columnName, columnValue, dynamicPartValue, dynamicColumnType, dataType);
	}

	/**
	 * Create a new tuple belonging to a different column family
	 * 
	 * @param columnFamily
	 *            - column family
	 * @return tuple with different column family
	 */
	public Tuple cloneWithNewCF(String columnFamily) {
		Tuple output;
		if (this.raw) {
			output = new Tuple(columnFamily, this.columnName, this.dataType);
		} else if (this.dynamicColumn) {
			output = Tuple.dynamicColumn(columnFamily, this.columnName, this.columnValue, this.dynamicPartValue,
					this.dynamicColumnType, this.dataType);
		} else {
			output = Tuple.staticColumn(columnFamily, this.columnName, this.columnValue, this.dataType);
		}

		return output;
	}

	public String getColumnValue() {
		return columnValue;
	}

	public String getColumnName() {
		return columnName;
	}
	
	public String getHbaseColumnName() {
		if(dynamicColumn){
			if(dynamicColumnType.equals(DynamicColumnType.STATIC_PREFIX)){
				return columnName + dynamicPartValue;
			}else{
				return dynamicPartValue + columnName;
			}
		}else{
			return columnName;
		}
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public boolean isDynamicColumn() {
		return dynamicColumn;
	}

	public String getDynamicPartValue() {
		return dynamicPartValue;
	}

	public Class<?> getColumnDataType() {
		return dataType;
	}

	@Override
	public String toString() {
		return "Tuple [columnFamily=" + columnFamily + ", columnName=" + columnName + ", columnValue=" + columnValue
				+ " ]";
	}

	public DynamicColumnType getDynamicColumnType() {
		return dynamicColumnType;
	}

}
