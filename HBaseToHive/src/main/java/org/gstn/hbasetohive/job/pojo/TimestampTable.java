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
package org.gstn.hbasetohive.job.pojo;

import java.io.Serializable;

import org.gstn.hbasetohive.exception.ValidationException;

@SuppressWarnings("serial")
public class TimestampTable implements Serializable {

	private String tableName;
	private String columnFamily;
	private String columnName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public void validate(String scFilePath) throws ValidationException {
		if (tableName == null || columnFamily == null || columnName == null) {
			throw new ValidationException("Please specify all of <tableName>, <columnFamily> and"
					+ " <columnName> in config file: " + scFilePath + ", for the incremental load to work");
		}
		if (tableName != null && columnFamily != null && columnName != null) {
			if (tableName.isEmpty() || columnFamily.isEmpty() || columnName.isEmpty()) {
				throw new ValidationException(
						"Please specify a non-empty string value for all of <tableName>, <columnFamily> and"
								+ " <columnName> in config file: " + scFilePath + ", for the incremental load to work");
			}
		}
	}

}
