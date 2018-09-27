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

import static org.gstn.schemaexplorer.target.Constants.PARENT_PATH_SEPARATOR;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gstn.schemaexplorer.exception.SchemaValidationException;

import com.google.gson.JsonElement;

/**
 * This class stores a list of target columns.
 */
@SuppressWarnings("serial")
public class HdfsColumnList implements Serializable {

	private Map<String, List<HdfsColumn>> columnList;
	private Set<String> jsonColumns;
	private Map<String, List<String>> parentPathColumnsMap;

	public HdfsColumnList() {
		columnList = new HashMap<>();
		jsonColumns = new HashSet<>();
	}

	public void addColumn(String parentPath, HdfsColumn hdfsColumn) {
		if (columnList.get(parentPath) == null) {
			columnList.put(parentPath, new ArrayList<>());
		}
		columnList.get(parentPath).add(hdfsColumn);
	}

	/**
	 * This method returns all the target column names for a particular schema.
	 * 
	 * @return ArrayList of names of all target columns in a particular schema.
	 */
	public ArrayList<String> getAllColumnNames() {
		ArrayList<String> columnNames = new ArrayList<>();
		for (String parentPath : columnList.keySet()) {
			for (HdfsColumn column : columnList.get(parentPath)) {
				columnNames.add(column.getColumnName());
			}
		}
		return columnNames;
	}

	/**
	 * This method returns all the target column names for a particular schema.
	 * If the column name is a JSON attribute, column name is prefixed with
	 * "JSON."
	 * 
	 * @return ArrayList of names of all target columns in a particular schema.
	 */
	public ArrayList<String> getAllColumnNamesWithJson() {
		ArrayList<String> columnNames = new ArrayList<>();
		for (String parentPath : columnList.keySet()) {
			for (HdfsColumn column : columnList.get(parentPath)) {
				if (column.isJsonField())
					columnNames.add("JSON." + column.getColumnName());
				else
					columnNames.add(column.getColumnName());
			}
		}
		return columnNames;
	}

	/**
	 * This method returns the default value of a column by scanning through all
	 * the columns in target schema. It returns NULL if column is not found.
	 * 
	 * @param columnName
	 *            is the name of the target column.
	 * @return default value of columnName or NULL.
	 */
	public String getColumnDefaultValue(String columnName) {
		for (String parentPath : columnList.keySet()) {
			for (HdfsColumn hdfsColumn : columnList.get(parentPath)) {
				if (hdfsColumn.getColumnName().equals(columnName))
					return hdfsColumn.getColumnDefaultValue();
			}
		}
		return null;
	}

	public String getColumnDataType(String columnName) {
		for (String parentPath : columnList.keySet()) {
			for (HdfsColumn hdfsColumn : columnList.get(parentPath)) {
				if (hdfsColumn.getColumnName().equals(columnName))
					return hdfsColumn.getColumnDataType();
			}
		}
		return null;
	}

	public boolean isJsonColumn(String columnName) {
		if (jsonColumns.isEmpty()) {
			for (String parentPath : columnList.keySet()) {
				for (HdfsColumn column : columnList.get(parentPath)) {
					if (column.isJsonField())
						jsonColumns.add(column.getColumnName());
				}
			}
		}
		return jsonColumns.contains(columnName);
	}

	public void createParentPathColumnsMap() throws SchemaValidationException {
		if (parentPathColumnsMap == null) {
			parentPathColumnsMap = new HashMap<>();
			for (String parentPath : columnList.keySet()) {
				List<String> columns = new ArrayList<>();
				for (HdfsColumn hdfsColumn : columnList.get(parentPath)) {
					if (hdfsColumn.isJsonField()) {
						String colNameWithoutParentPath;
						if (parentPath == null) {
							colNameWithoutParentPath = hdfsColumn.getColumnName();
						} else {
							colNameWithoutParentPath = hdfsColumn.getColumnName()
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
	}

	public boolean checkKey(String parentPath, String currentPath, String key, JsonElement value) {

		if (parentPathColumnsMap == null) {
			createParentPathColumnsMap();
		}

		boolean considerPrefixCase = false;
		if (value.isJsonObject() || (value.isJsonArray() && value.toString().contains(":"))) {
			considerPrefixCase = true;
		}

		// Check if column having name as key is present under parentPath
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
				if (parent != null
						&& (parent.equals(currentPath) || parent.startsWith(currentPath + PARENT_PATH_SEPARATOR))) {
					return true;
				}
			}
		}
		return false;
	}

}
