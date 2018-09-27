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
package org.gstn.schemaexplorer.hdfs;

import java.io.Serializable;

import org.slf4j.LoggerFactory;

/**
 * This class stores information about the individual target column.
 * Objects of this class are instantiated by ANTLR grammar parser from 
 * target schema file provided by user.
 */
@SuppressWarnings("serial")
public class HdfsColumn implements Serializable {
	
	private final String columnName;
	private final String columnDataType;
	private final String columnDefaultValue;
	private final boolean isJsonField;
	
	/**
	 * Constructor 
	 * @param columnName is the name of the column in target schema.
	 * @param columnDataType is the data type of the column in target schema.
	 * @param columnDefaultValue is the default value of the column in target schema.
	 * @param isJsonField is a flag to represent if the column is JSON attribute.
	 */
	public HdfsColumn(String columnName, String columnDataType, String columnDefaultValue, boolean isJsonField) {
		LoggerFactory.getLogger(this.getClass().getCanonicalName());
		this.columnName = columnName;
		this.columnDataType = columnDataType;
		this.columnDefaultValue = columnDefaultValue;
		this.isJsonField = isJsonField;
	}

	public String getColumnName() {
		return columnName;
	}
	
	public String getColumnDataType() {
		return columnDataType;
	}

	public String getColumnDefaultValue() {
		return columnDefaultValue;
	}
	
	public boolean isJsonField() {
		return isJsonField;
	}
	
}
