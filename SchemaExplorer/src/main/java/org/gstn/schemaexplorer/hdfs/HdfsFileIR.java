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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;

/**
 * This class is the Internal Representation(IR) of a target hdfs file. It
 * contains all the metadata of target hdfs file.
 */
@SuppressWarnings("serial")
public class HdfsFileIR implements Serializable {

	// Schema_name -> all columns list
	private Map<String, HdfsColumnList> schemaColumnList;
	private Map<String, String> schemaDdls;

	/**
	 * Constructor.
	 */
	public HdfsFileIR() {
		LoggerFactory.getLogger(this.getClass().getCanonicalName());
		schemaColumnList = new HashMap<>();
		schemaDdls = new HashMap<>();
	}

	/**
	 * This method takes schema name as input and returns all the column names.
	 * 
	 * @param schemaName
	 *            is the target schema.
	 * @return ArrayList of column names of particular target schema.
	 */
	public ArrayList<String> getSchemaColumns(String schemaName) {
		return schemaColumnList.get(schemaName).getAllColumnNames();
	}

	/**
	 * Utility method to add column in target hdfs schema.
	 * 
	 * @param currHdfsSchema
	 *            is the current hdfs schema.
	 * @param hdfsColumn
	 *            is the column to be added.
	 */
	public void addColumn(String currHdfsSchema, String parentPath, HdfsColumn hdfsColumn) {
		if (schemaColumnList.get(currHdfsSchema) == null) {
			schemaColumnList.put(currHdfsSchema, new HdfsColumnList());
		}
		schemaColumnList.get(currHdfsSchema).addColumn(parentPath, hdfsColumn);
	}

	/**
	 * This method returns all the hdfs schemas present.
	 * 
	 * @return all the hdfs schemas present.
	 */
	public Set<String> getSchemaNames() {
		return schemaColumnList.keySet();
	}

	/**
	 * This method takes a hdfs schema as input and returns all JSON column.
	 * 
	 * @param schemaName
	 *            is the target hdfs schema.
	 * @return ArrayList of JSON columns of a given hdfs schema.
	 */
	public ArrayList<String> getSchemaColumnsForJsonValidation(String schemaName) {
		return schemaColumnList.get(schemaName).getAllColumnNamesWithJson();
	}
	
	
	public Map<String,Class> getSchemaJsonColumns(String schemaName) {
		return schemaColumnList.get(schemaName).getSchemaJsonColumns();
	}
	
	
	/**
	 * This method returns the default value of a given column present in a
	 * given hdfs schema.
	 * 
	 * @param schemaName
	 *            is the target hdfs schema.
	 * @param columnName
	 *            is the column name in target hdfs schema.
	 * @return the default value of a column.
	 */
	public String getColumnDefaultValue(String schemaName, String columnName) {
		return schemaColumnList.get(schemaName).getColumnDefaultValue(columnName);
	}

	public String getColumnDataType(String schemaName, String columnName) {
		return schemaColumnList.get(schemaName).getColumnDataType(columnName);
	}

	public boolean isJsonColumn(String schemaName, String columnName) {
		return schemaColumnList.get(schemaName).isJsonColumn(columnName);
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

	public boolean checkKey(String targetSchema, String parentPath, String currentPath, String key, JsonElement value) {
		return schemaColumnList.get(targetSchema).checkKey(parentPath, currentPath, key, value);
	}

	public boolean isSchemaDefined(String targetSchema) {
		return schemaDdls.containsKey(targetSchema);
	}
}
