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
package org.gstn.schemaexplorer.hive;

import static org.gstn.schemaexplorer.target.Constants.PARENT_PATH_SEPARATOR;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gstn.schemaexplorer.exception.SchemaValidationException;

import com.google.gson.JsonElement;

/*
 * This class have all the column list of a particular table of hive
 */
@SuppressWarnings("serial")
public class HiveColumnList implements Serializable {
	// map of parent path vs. Column...if parent path is not applicable it would
	// be considered as null
	// Parent path is used while defining hierarchy for json columns
	private Map<String, List<HiveColumn>> columnList;
	private Set<String> jsonColumns;
	private Map<String, List<String>> parentPathColumnsMap;

	public HiveColumnList() {
		columnList = new HashMap<>();
		jsonColumns = new HashSet<String>();
	}

	public void addColumn(String parentPath, HiveColumn hdfsColumn) {
		if (columnList.get(parentPath) == null) {
			columnList.put(parentPath, new ArrayList<>());
		}
		columnList.get(parentPath).add(hdfsColumn);
	}

	public List<String> getAllColumnNames() {
		List<String> columnNames = new ArrayList<>();
		for (String parentPath : columnList.keySet()) {
			for (HiveColumn column : columnList.get(parentPath)) {
				columnNames.add(column.getColumnName());
			}
		}
		return columnNames;
	}

	public List<String> getAllColumnNamesWithJson() {
		List<String> columnNames = new ArrayList<>();
		for (String parentPath : columnList.keySet()) {
			for (HiveColumn column : columnList.get(parentPath)) {
				if (column.isJsonField())
					columnNames.add("JSON." + column.getColumnName());
				else
					columnNames.add(column.getColumnName());
			}
		}
		return columnNames;
	}

	public String getColumnDefaultValue(String columnName) {
		for (String parentPath : columnList.keySet()) {
			for (HiveColumn hiveColumn : columnList.get(parentPath)) {
				if (hiveColumn.getColumnName().equals(columnName))
					return hiveColumn.getColumnDefaultValue();
			}
		}
		return null;
	}

	public String getColumnDataType(String columnName) {
		for (String parentPath : columnList.keySet()) {
			for (HiveColumn hiveColumn : columnList.get(parentPath)) {
				if (hiveColumn.getColumnName().equals(columnName))
					return hiveColumn.getColumnDataType();
			}
		}
		return null;
	}

	public Map<String, String> getAllHiveColumnsForDDL() {
		Map<String, String> columnMap = new LinkedHashMap<>();
		for (String parentPath : columnList.keySet()) {
			for (HiveColumn hiveColumn : columnList.get(parentPath)) {
				
					columnMap.put(hiveColumn.getColumnName(), hiveColumn.getColumnDataType());
				
			}
		}
		return columnMap;
	}
	
	public Map<String, String> getHiveColumnsForDDL() {
		Map<String, String> columnMap = new LinkedHashMap<>();
		for (String parentPath : columnList.keySet()) {
			for (HiveColumn hiveColumn : columnList.get(parentPath)) {
				if (!hiveColumn.isPartitionColumn()) {
					columnMap.put(hiveColumn.getColumnName(), hiveColumn.getColumnDataType());
				}
			}
		}
		return columnMap;
	}

	public Map<String, String> getHivePartitionsForDDL() {
		Map<String, String> columnMap = new LinkedHashMap<>();
		for (String parentPath : columnList.keySet()) {
			for (HiveColumn hiveColumn : columnList.get(parentPath)) {
				if (hiveColumn.isPartitionColumn()) {
					columnMap.put(hiveColumn.getColumnName(), hiveColumn.getColumnDataType());
				}
			}
		}
		return columnMap;
	}

	public void createParentPathColumnsMap() throws SchemaValidationException {

		if (parentPathColumnsMap == null) {
			parentPathColumnsMap = new HashMap<>();
			for (String parentPath : columnList.keySet()) {
				List<String> columns = new ArrayList<>();
				for (HiveColumn hiveColumn : columnList.get(parentPath)) {
					if (hiveColumn.isJsonField()) {
						String colNameWithoutParentPath;
						if (parentPath == null) {
							colNameWithoutParentPath = hiveColumn.getColumnName();
						} else {
							colNameWithoutParentPath = hiveColumn.getColumnName()
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

	public boolean isJsonColumn(String columnName) {
		if (jsonColumns.isEmpty()) {
			for (String parentPath : columnList.keySet()) {
				for (HiveColumn column : columnList.get(parentPath)) {
					if (column.isJsonField())
						jsonColumns.add(column.getColumnName());
				}
			}
		}
		return jsonColumns.contains(columnName);
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
