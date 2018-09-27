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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.gstn.schemaexplorer.util.JSON;
import org.gstn.schemaexplorer.util.SnappyCompressedJSON;
import org.gstn.schemaexplorer.util.SnappyCompressedType;

/**
 * This class stores information about individual column of HBase table.
 *
 */
@SuppressWarnings({ "serial", "unchecked" })
public class HBaseColumn implements Serializable {

	private final String columnName;
	private final String columnFamily;
	private final String dataType;
	private final String dataFormat;
	private final String columnDefaultValue;
	private final boolean isJsonField;
	private final String compressionType;
	private final boolean isDynamic;
	private final String dynamicPrefix, dynamicSuffix, dynamicComponent;

	/**
	 * Constructor for static columns
	 * 
	 * @param cf
	 *            - column family name
	 * @param col
	 *            - column qualifier
	 * @param dtype
	 *            - data type of the column
	 * @param format
	 *            - data format(only applicable if data type is date)
	 * @param defaultVal
	 *            - default value to be written(only applicable for target
	 *            table)
	 * @param comptype
	 *            - compression type(only snappy compression is supported)
	 * @param isJson
	 *            - is the column a JSON attribute in source table(only
	 *            applicable for target table)
	 */
	public HBaseColumn(String cf, String col, String dtype, String format, String defaultVal, String comptype,
			boolean isJson) {

		isDynamic = false;
		columnName = col;
		columnFamily = cf;
		dataType = dtype;
		dataFormat = format;
		columnDefaultValue = defaultVal;
		isJsonField = isJson;
		compressionType = comptype;
		dynamicPrefix = null;
		dynamicSuffix = null;
		dynamicComponent = null;
	}

	/**
	 * Constructor for dynamic columns
	 * 
	 * @param cf
	 *            - column family name
	 * @param dynPrefix
	 *            - column qualifier prefix
	 * @param dynSuffix
	 *            - column qualifier suffix
	 * @param dynComponent
	 *            - column qualifier dynamic component
	 * @param dtype
	 *            - data type of the column
	 * @param format
	 *            - data format(only applicable if data type is date)
	 * @param defVal
	 *            - default value to be written(only applicable for target
	 *            table)
	 * @param comptype
	 *            - compression type(only snappy compression is supported)
	 * @param isJson
	 *            - is the column a JSON attribute in source table(only
	 *            applicable for target table)
	 */
	public HBaseColumn(String cf, String dynPrefix, String dynSuffix, String dynComponent, String dtype, String format,
			String defVal, String comptype, boolean isJson) {

		columnName = null;
		isDynamic = true;
		columnFamily = cf;
		dynamicPrefix = dynPrefix;
		dynamicSuffix = dynSuffix;
		dynamicComponent = dynComponent;
		dataType = dtype;
		dataFormat = format;
		columnDefaultValue = defVal;
		compressionType = comptype;
		isJsonField = isJson;
	}

	public String getColumnName() {
		if (isDynamic) {
			return dynamicPrefix + dynamicComponent + dynamicSuffix;
		} else {
			return columnName;
		}
	}
	
	public String getColumnNameRegexPattern() {
		if (isDynamic) {
			return "^" + dynamicPrefix + ".*" + dynamicSuffix + "$";
		} else {
			return columnName;
		}
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public String getColumnDefaultValue() {
		return columnDefaultValue;
	}

	public String getDataType() {
		return dataType;
	}

	public String getDataFormat() {
		return dataFormat;
	}

	public String getColumnDataType() {
		return dataType;
	}

	public String getCompressionType() {
		return compressionType;
	}

	public String getDynamicPrefix() {
		return dynamicPrefix;
	}

	public String getDynamicSuffix() {
		return dynamicSuffix;
	}

	public boolean isJsonField() {
		return isJsonField;
	}

	public boolean isDynamicColumn() {
		return isDynamic;
	}

	boolean isStaticColumn() {
		return !isDynamicColumn();
	}

	public String getColumnNameWithoutDynamicComponent() {
		if (isDynamic) {
			return dynamicPrefix + dynamicSuffix;
		} else {
			return columnName;
		}

	}

	public String getDynamicComponent() {
		return dynamicComponent;
	}

	/**
	 * This function returns the data type of calling object
	 * 
	 * @return class of data type
	 */
	public <T> T getDataTypeClass() {

		if (this.dataType.equalsIgnoreCase("json")) {
			if (this.compressionType.equalsIgnoreCase("snappy")) {
				return (T) SnappyCompressedJSON.class;
			} else {
				return (T) JSON.class;
			}
		} else if (this.compressionType.equalsIgnoreCase("snappy")) {
			return (T) SnappyCompressedType.class;
		} else if (this.dataType.equalsIgnoreCase("string") || this.dataType.equalsIgnoreCase(null)
				|| this.dataType.equalsIgnoreCase("")) {
			return (T) String.class;
		} else if (this.dataType.equalsIgnoreCase("integer") || this.dataType.equalsIgnoreCase("int")) {
			return (T) Integer.class;
		} else if (this.dataType.equalsIgnoreCase("double")) {
			return (T) Double.class;
		} else if (this.dataType.equalsIgnoreCase("float")) {
			return (T) Float.class;
		} else if (this.dataType.equalsIgnoreCase("long")) {
			return (T) Long.class;
		} else if (this.dataType.equalsIgnoreCase("bigdecimal")) {
			return (T) BigDecimal.class;
		} else if (this.dataType.equalsIgnoreCase("biginteger")) {
			return (T) BigInteger.class;
		} else if (this.dataType.equalsIgnoreCase("date")) {
			return (T) Date.class;
		} else {
			return (T) String.class;
		}
	}

}
