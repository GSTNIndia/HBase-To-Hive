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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;

/*
 * Internal representation of a table. contains all the metadata of hive table
 * Hive schema name will be processed from grammar
 * Hive table name will be same as schema name as present in grammar
 */
@SuppressWarnings("serial")
public class HiveTableIR implements Serializable {

	private Map<String, HiveColumnList> schemaColumnList;

	private Map<String, String> schemaDatabase;

	private Map<String, String> schemaDdls;

	private Map<String, String> schemaStorage;

	public HiveTableIR() {
		LoggerFactory.getLogger(this.getClass().getCanonicalName());
		schemaColumnList = new HashMap<>();
		schemaDatabase = new HashMap<>();
		schemaDdls = new HashMap<>();
		schemaStorage = new HashMap<>();
	}

	public void addColumn(String currHiveSchema, String parentPath, HiveColumn hdfsColumn) {
		if (schemaColumnList.get(currHiveSchema) == null) {
			schemaColumnList.put(currHiveSchema, new HiveColumnList());
		}
		schemaColumnList.get(currHiveSchema).addColumn(parentPath, hdfsColumn);
	}

	public void setStorage(String schema, String storage) {
		schemaStorage.put(schema, storage);
	}

	public Set<String> getSchemaNames() {
		return schemaColumnList.keySet();
	}

	public void setDatabaseName(String schema, String database) {
		if (schemaDatabase.get(schema) == null)
			schemaDatabase.put(schema, database);
	}

	public String getTargetDatabase(String targetSchema) {
		return schemaDatabase.get(targetSchema);
	}

	public List<String> getSchemaColumns(String schemaName) {
		return schemaColumnList.get(schemaName).getAllColumnNames();
	}

	public List<String> getSchemaColumnsForJsonValidation(String schemaName) {
		return schemaColumnList.get(schemaName).getAllColumnNamesWithJson();
	}
	
	public Map<String,Class> getSchemaJsonColumns(String schemaName) {
		return schemaColumnList.get(schemaName).getSchemaJsonColumns();
	}

	public String getColumnDataType(String schemaName, String columnName) {
		return schemaColumnList.get(schemaName).getColumnDataType(columnName);
	}

	public String getColumnDefaultValue(String schemaName, String columnName) {
		return schemaColumnList.get(schemaName).getColumnDefaultValue(columnName);
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

	public void createHiveScript(String targetSchema, String hdfsUrl, String hdfsFilePath, String hdfsPath,
			Map<String, String> sparkConfMap, String jobId, HBaseTableExplorer hBaseTableExplorer,
			String sourceSchemaName, boolean insertionAdded, boolean deleteAllAdded, boolean deleteSingleAdded,
			boolean dynamicColumnsInSelect) throws InvalidSchemaException {

		StringBuilder script = new StringBuilder();

		if (deleteAllAdded || deleteSingleAdded || insertionAdded) {
			script.append(getCommonPart(sparkConfMap, targetSchema));

			if (deleteAllAdded || deleteSingleAdded) {
				// get all row key columns from source and their data type in
				// target
				List<Map<String, String>> result = getKeyColumnsAndTypeForDeletion(hBaseTableExplorer, sourceSchemaName,
						targetSchema);
				
				Map<String,String> nonPartitionKeyColumns = result.get(0);
				Map<String,String> partitionKeyColumns = result.get(1);
				Map<String,String> allColumnsForTextTable = result.get(2);

				boolean deleteSingleHandled = false;

				if (deleteAllAdded || !dynamicColumnsInSelect) {
					String prefix = null;

					if (dynamicColumnsInSelect) {
						prefix = "delete_all_";
					} else {
						// If there are only static columns in select, then
						// both files delete_all and delete_single will have
						// same number of columns and so they can be handled
						// similarly
						prefix = "delete_";
						deleteSingleHandled = true;
					}
					script.append(getDeleteScript(nonPartitionKeyColumns, partitionKeyColumns, allColumnsForTextTable, targetSchema, prefix, hdfsUrl, hdfsFilePath));
				}

				if (deleteSingleAdded && !deleteSingleHandled) {
					// get dynamic part names from source and their data type in
					// target

					result = getDynamicPartColumnsAndTypeForDeletion(hBaseTableExplorer,
							sourceSchemaName, targetSchema);
					
					nonPartitionKeyColumns.putAll(result.get(0));
					partitionKeyColumns.putAll(result.get(1));
					allColumnsForTextTable.putAll(result.get(2));
					
					script.append(getDeleteScript(nonPartitionKeyColumns, partitionKeyColumns, allColumnsForTextTable, targetSchema, "delete_single_", hdfsUrl, hdfsFilePath));
				}

			}

		}

		if (insertionAdded) {
			script.append(getInsertionHiveScript(targetSchema, hdfsUrl, hdfsFilePath));
		}

		// If it is fine, if we don't create any .sql file when no insert/delete
		// files are created, we can move below line inside above if
		writeScript(script.toString(), targetSchema, hdfsUrl, hdfsPath, jobId);
	}

	public void createInsertionHiveScript(String targetSchema, String hdfsUrl, String hdfsFilePath, String hdfsBasePath,
			Map<String, String> sparkConfMap, String jobId) {
		StringBuilder script = new StringBuilder();
		script.append(getCommonPart(sparkConfMap, targetSchema));
		script.append(getInsertionHiveScript(targetSchema, hdfsUrl, hdfsFilePath));
		writeScript(script.toString(), targetSchema, hdfsUrl, hdfsBasePath, jobId);
	}

	
	private StringBuilder getDeleteScript(Map<String, String> nonPartitionColumns, Map<String, String> partitionColumns, Map<String, String> allColumnsForTextTable, String targetSchema, String deleteFilesPrefix, String hdfsUrl, String hdfsFilePath) {

		StringBuilder ddl = new StringBuilder();
		
		String deleteDataTableName =  deleteFilesPrefix + targetSchema;
		
		boolean tableExists = false;
		boolean dropIfExists = true;
		boolean loadTimeStamp = false;
		
		String filePrefix = deleteFilesPrefix + "*"; 
				
		ddl.append(scriptForCreatingAndLoadingParquetTable(targetSchema, deleteDataTableName, hdfsUrl, hdfsFilePath, filePrefix, nonPartitionColumns, partitionColumns, allColumnsForTextTable, tableExists, dropIfExists, loadTimeStamp));
		
		//delete approach based on Left Outer Join
		StringBuilder columnNamesCsv = getDataForCreateClause(nonPartitionColumns).columnNamesCSV;
		columnNamesCsv.append(getDataForCreateClause(partitionColumns).columnNamesCSV);
		//removing last comma from columnNamesCSV
		if(columnNamesCsv.length()>0){
			columnNamesCsv.delete(columnNamesCsv.length()-2, columnNamesCsv.length());
		}
		
		ddl.append(getDeletionUsingLeftOuterJoin(targetSchema, columnNamesCsv, deleteDataTableName));
		
		//Drop deleteDataTableName
		ddl.append("DROP TABLE IF EXISTS "+deleteDataTableName+";\n\n");
		
		return ddl;
	}

	private StringBuilder getDeletionUsingLeftOuterJoin(String targetSchema, StringBuilder deleteColumnNamesCSV,
			String deleteDataTableName) {
		StringBuilder ddl = new StringBuilder();
		String newParquetTable = targetSchema + "_new";
		String parquetTable = targetSchema;
		String parquetTableQualifier = "T2";
		String deleteTableQualifier = "T1";

		// DROP IF Exists AND CREATE new parquet table
		ddl.append("DROP TABLE IF EXISTS " + newParquetTable + ";\n\n");

		Wrapper newParquetWrapper = getCreateTableStatements(targetSchema, newParquetTable);
		ddl.append(newParquetWrapper.ddlParquet);

		// removing last comma from columnNamesCSV
		if (newParquetWrapper.columnNamesCSV.length() > 0) {
			newParquetWrapper.columnNamesCSV.delete(newParquetWrapper.columnNamesCSV.length() - 2,
					newParquetWrapper.columnNamesCSV.length());
		}

		// INSERT INTO new parquet table by doing LEFT OUTER JOIN between
		// parquestTabel and deleteDataTable
		String[] deleteColumnNames = deleteColumnNamesCSV.toString().split(",");
		for (int i = 0; i < deleteColumnNames.length; i++) {
			deleteColumnNames[i] = deleteColumnNames[i].trim();
		}

		String[] parquetColumnNames = newParquetWrapper.columnNamesCSV.toString().split(",");
		if (parquetColumnNames.length > 0) {

			for (int i = 0; i < parquetColumnNames.length; i++) {
				String colName = parquetColumnNames[i].trim();
				if (colName.equals("CURRENT_TIMESTAMP()")) {
					colName = "LOAD_TIMESTAMP";
				}
				parquetColumnNames[i] = colName;
			}
		}

		String qualifiedParquetColumnNames = getQualifiedColumnNames(parquetColumnNames, parquetTableQualifier);

		StringBuilder[] conditions = getJoinAndWhereConditions(deleteColumnNames, deleteTableQualifier,
				parquetTableQualifier);

		StringBuilder joinCondition = conditions[0];
		StringBuilder whereCondition = conditions[1];
		
		ddl.append("INSERT INTO "+newParquetTable+" \n");
		
		if(newParquetWrapper.partitionColumnNamesCSV.length()>0){
			ddl.append(" PARTITION " + newParquetWrapper.partitionColumnNamesCSV+" \n");
		}
		
		ddl.append("SELECT "+qualifiedParquetColumnNames +" \n");
		ddl.append("FROM "+parquetTable + " "+ parquetTableQualifier+" \n");
		ddl.append("LEFT OUTER JOIN "+deleteDataTableName +" "+deleteTableQualifier+" \n");
		ddl.append("ON \n");
		ddl.append("( "+joinCondition+" ) \n");
		ddl.append("WHERE \n");
		ddl.append("( "+whereCondition+" ); \n\n");
		
		//Drop parquet table
		ddl.append("DROP TABLE "+parquetTable+"; \n\n");

		ddl.append("INSERT INTO " + newParquetTable + " \n");
		ddl.append("SELECT " + qualifiedParquetColumnNames + " \n");
		ddl.append("FROM " + parquetTable + " " + parquetTableQualifier + " \n");
		ddl.append("LEFT OUTER JOIN " + deleteDataTableName + " " + deleteTableQualifier + " \n");
		ddl.append("ON \n");
		ddl.append("( " + joinCondition + " ) \n");
		ddl.append("WHERE \n");
		ddl.append("( " + whereCondition + " ); \n\n");

		// Drop parquet table
		ddl.append("DROP TABLE " + parquetTable + "; \n\n");

		// Rename new parquest table to original parquest table name
		ddl.append("ALTER TABLE " + newParquetTable + " RENAME TO " + parquetTable + "; \n\n");

		return ddl;
	}

	private StringBuilder[] getJoinAndWhereConditions(String[] deleteColumnNames, String deleteTableQualifier,
			String parquetTableQualifier) {
		StringBuilder joinSb = new StringBuilder();
		StringBuilder whereSb = new StringBuilder();

		String condition = " AND ";

		for (int i = 0; i < deleteColumnNames.length; i++) {
			joinSb.append(deleteTableQualifier + "." + deleteColumnNames[i] + " = " + parquetTableQualifier + "."
					+ deleteColumnNames[i] + condition);
			whereSb.append(deleteTableQualifier + "." + deleteColumnNames[i] + " IS NULL " + condition);
		}
		// remove last condition
		if (joinSb.length() > 0) {
			joinSb.delete(joinSb.length() - condition.length(), joinSb.length());
			whereSb.delete(whereSb.length() - condition.length(), whereSb.length());
		}

		StringBuilder[] result = new StringBuilder[2];
		result[0] = joinSb;
		result[1] = whereSb;

		return result;
	}

	private String getQualifiedColumnNames(String[] columnNames, String tableNameQualifier) {

		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < columnNames.length; i++) {
			sb.append(tableNameQualifier + "." + columnNames[i] + ", ");
		}
		// remove last ", "
		if (sb.length() > 0) {
			sb.delete(sb.length() - 2, sb.length());
		}

		return sb.toString();
	}

	private List<Map<String, String>> getDynamicPartColumnsAndTypeForDeletion(HBaseTableExplorer hBaseTableExplorer,
			String sourceSchemaName, String targetSchema) {
		// get all dynamic part names from source
		List<String> dynamicPartNames = hBaseTableExplorer.getDynamicPartNames(sourceSchemaName);

		
		Map<String,String> nonPartitionColumns = schemaColumnList.get(targetSchema).getHiveColumnsForDDL();
		Map<String,String> partitionColumns = schemaColumnList.get(targetSchema).getHivePartitionsForDDL();
		
		Map<String,String> nonPartitionDynamicPartColumns = new LinkedHashMap<String,String>();
		Map<String,String> partitionDynamicPartColumns = new LinkedHashMap<String,String>();
		Map<String,String> allColumns = new LinkedHashMap<String,String>();

		for (String colName : dynamicPartNames) {
			String dataType = "";
			if(nonPartitionColumns.containsKey(colName)){
				dataType = nonPartitionColumns.get(colName);
				nonPartitionDynamicPartColumns.put(colName, dataType);
			}else{
				dataType = partitionColumns.get(colName);
				partitionDynamicPartColumns.put(colName, dataType);
			}
			allColumns.put(colName, dataType);
		}
		
		List<Map<String, String>> result = new ArrayList<>();
		result.add(nonPartitionDynamicPartColumns);
		result.add(partitionDynamicPartColumns);
		result.add(allColumns);
		return result;
	}

	private List<Map<String, String>> getKeyColumnsAndTypeForDeletion(HBaseTableExplorer hBaseTableExplorer, String sourceSchemaName, String targetSchema) throws InvalidSchemaException{
		//get all row key columns from source
		List<String> keyColumnNames = hBaseTableExplorer.getNonHashRowkeyFieldNames(sourceSchemaName);

		
		Map<String,String> nonPartitionColumns = schemaColumnList.get(targetSchema).getHiveColumnsForDDL();
		Map<String,String> partitionColumns = schemaColumnList.get(targetSchema).getHivePartitionsForDDL();
		
		Map<String,String> nonPartitionKeyColumns = new LinkedHashMap<String,String>();
		Map<String,String> partitionKeyColumns = new LinkedHashMap<String,String>();
		Map<String,String> allColumns = new LinkedHashMap<String,String>();

		for (String colName : keyColumnNames) {
			String dataType = "";
			if(nonPartitionColumns.containsKey(colName)){
				dataType = nonPartitionColumns.get(colName);
				nonPartitionKeyColumns.put(colName, dataType);
			}
			
			if(partitionColumns.containsKey(colName)){
				dataType = partitionColumns.get(colName);
				partitionKeyColumns.put(colName, dataType);
			}
			
			allColumns.put(colName, dataType);
		}
		
		List<Map<String, String>> result = new ArrayList<>();
		result.add(nonPartitionKeyColumns);
		result.add(partitionKeyColumns);
		result.add(allColumns);
		return result;
	}

	private StringBuilder getCommonPart(Map<String, String> sparkConfMap, String targetSchema) {
		String hiveDatabase = schemaDatabase.get(targetSchema);

		StringBuilder script = new StringBuilder();

		for (String conf : sparkConfMap.keySet()) {
			if (!conf.equals("spark.master")) {
				script.append("SET " + conf + " = " + sparkConfMap.get(conf) + ";\n");
			}
		}

		if (!script.equals("")) {
			script.append("\n");
		}

		script.append("CREATE DATABASE IF NOT EXISTS " + hiveDatabase + ";\n\n");
		script.append("USE " + hiveDatabase + ";\n\n");

		// added parquet table creation in common part, as that table should
		// exists for all the three scripts to work
		// (i.e. delete_all and delete_single require it for join and insert
		// require it for inserting data)
		Wrapper wrapper = getCreateTableStatements(targetSchema, targetSchema);

		script.append(wrapper.ddlParquet + "\n\n");

		return script;
	}

	private StringBuilder getInsertionHiveScript(String targetSchema, String hdfsUrl, String hdfsFilePath) {
		
		// getting all column names for hive script
		Map<String, String> nonPartitionColumnsMap = schemaColumnList.get(targetSchema).getHiveColumnsForDDL();
		Map<String, String> partitionColumnsMap = schemaColumnList.get(targetSchema).getHivePartitionsForDDL();
		Map<String, String> allColumns = schemaColumnList.get(targetSchema).getAllHiveColumnsForDDL();
		
		//table is already created as part of getCommonPart method
		boolean tableExists = true;
		boolean dropIfExists = false;
		boolean loadTimeStamp = true;
		
		String filePrefix = "insert_*"; 
				
		return scriptForCreatingAndLoadingParquetTable(targetSchema, targetSchema, hdfsUrl, hdfsFilePath, filePrefix, nonPartitionColumnsMap, partitionColumnsMap, allColumns, tableExists, dropIfExists, loadTimeStamp);
	}
	
	private StringBuilder scriptForCreatingAndLoadingParquetTable(String targetSchema, String tableName, String hdfsUrl, String hdfsFilePath,
			String fileNameOrPrefix, Map<String, String> nonPartitionColumnsMap, Map<String, String> partitionColumnsMap, Map<String, String> allColumnsForTextTable, boolean tableExists, boolean dropIfExists, boolean loadTimeStamp) {

		Wrapper  wrapper = getCreateTableStatements(targetSchema, tableName, nonPartitionColumnsMap, partitionColumnsMap, allColumnsForTextTable, dropIfExists, loadTimeStamp);

		StringBuilder ddl = wrapper.ddl;
		StringBuilder columnNames = wrapper.columnNamesCSV;

		StringBuilder partitionNames = wrapper.partitionColumnNamesCSV;
		
		StringBuilder ddlParquet;
		if(!tableExists){
			ddlParquet = wrapper.ddlParquet;
		}else{
			ddlParquet = new StringBuilder();
		}
		
		boolean partition = false;

		if(!partitionColumnsMap.isEmpty()){
			partition=true;
		}

		if (!hdfsFilePath.startsWith(File.separator) && !hdfsUrl.endsWith(File.separator)) {
			hdfsFilePath = File.separator + hdfsFilePath;
		}

		if (hdfsFilePath.startsWith(File.separator) && hdfsUrl.endsWith(File.separator)) {
			hdfsFilePath = hdfsFilePath.substring(1, hdfsFilePath.length());
		}

		// loading data from all insert files on hdfs path to hive
		String basePath = hdfsUrl + hdfsFilePath;

		if (!basePath.isEmpty() && !basePath.endsWith(File.separator)) {
			basePath = basePath + File.separator;
		}
		
		String dataFilesPath = basePath + fileNameOrPrefix ;
		
		ddl.append("LOAD DATA INPATH " + "'" + dataFilesPath + "' INTO TABLE " + tableName + "_temp;\n\n");

		// removing ', ' from end
		columnNames.delete(columnNames.length() - 2, columnNames.length());

		if (partition == true) {
			ddlParquet.append("set hive.exec.dynamic.partition = true;\n"
					+ "set hive.exec.dynamic.partition.mode = nonstrict;\n\n");
		}

		ddlParquet.append("INSERT INTO TABLE " + tableName);

		if (partition == true) {
			ddlParquet.append(" PARTITION " + partitionNames);

		}

		ddlParquet.append("\n\nSELECT " + columnNames + " FROM " + tableName + "_temp;\n\n");

		return ddl.append(ddlParquet);

	}
	
	private Wrapper getCreateTableStatements(String targetSchema, String tableName){
		// getting all column names for hive script

		Map<String, String> nonPartitionColumnsMap = schemaColumnList.get(targetSchema).getHiveColumnsForDDL();
		Map<String, String> partitionColumnsMap = schemaColumnList.get(targetSchema).getHivePartitionsForDDL();
		Map<String, String> allColumns = schemaColumnList.get(targetSchema).getAllHiveColumnsForDDL();
		
		return getCreateTableStatements(targetSchema, tableName, nonPartitionColumnsMap, partitionColumnsMap, allColumns, false, true);
	}
	
	private Wrapper getCreateTableStatements(String targetSchema, String tableName, Map<String, String> nonPartitionColumns, Map<String, String> partitionColumns, Map<String, String> allColumnsForTextTable, boolean dropIfExists, boolean loadTimeStamp){
		
		StringBuilder ddlParquet = new StringBuilder();
		
		if(dropIfExists){
			ddlParquet.append("DROP TABLE IF EXISTS " + tableName + ";\n");
			ddlParquet.append("CREATE TABLE " + tableName + " (\n");
		}else{
			ddlParquet.append("CREATE TABLE IF NOT EXISTS " + tableName + " (\n");
		}
		
		StringBuilder columnNames = new StringBuilder();
		StringBuilder partitionNames = new StringBuilder();

		StringBuilder ddl = new StringBuilder();
		ddl.append("CREATE TEMPORARY TABLE IF NOT EXISTS " + tableName + "_temp (\n");
		
		Wrapper wrapper = getDataForCreateClause(nonPartitionColumns);
		
		Wrapper wrapperForTextTable = getDataForCreateClause(allColumnsForTextTable);
		ddl.append(wrapperForTextTable.ddl.toString());
		
		ddlParquet.append(wrapper.ddlParquet.toString());
		columnNames.append(wrapper.columnNamesCSV.toString());
		
		if(loadTimeStamp){
			columnNames.append("CURRENT_TIMESTAMP(), ");
			ddlParquet.append("\tLOAD_TIMESTAMP timestamp\n)\n");
		}else{
			//remove ,\n from last column defination
			ddlParquet.delete(ddlParquet.length()-2, ddlParquet.length());
			ddlParquet.append("\n)\n");
		}
		
		if (!partitionColumns.isEmpty()) {
			partitionNames.append("(");
			ddlParquet.append("PARTITIONED BY (\n");
			
			wrapper = getDataForCreateClause(partitionColumns);

			ddlParquet.append(wrapper.ddlParquet.toString());
			columnNames.append(wrapper.columnNamesCSV.toString());
			partitionNames.append(wrapper.columnNamesCSV.toString());

			// removing ',\n' from end
			ddlParquet.delete(ddlParquet.length() - 2, ddlParquet.length());
			ddlParquet.append("\n)\n\n");

			// removing ', ' from end
			partitionNames.delete(partitionNames.length() - 2, partitionNames.length());
			partitionNames.append(")");
		}

		// removing ',\n' after last column
		ddl.delete(ddl.length() - 2, ddl.length());
		ddl.append("\n)\n");

		ddl.append("STORED AS TEXTFILE;\n\n");
		ddlParquet.append("STORED AS " + schemaStorage.get(targetSchema) + ";\n\n");

		return new Wrapper(ddl, ddlParquet, columnNames, partitionNames);
	}

	private Wrapper getDataForCreateClause(Map<String, String> columnMap) {
		StringBuilder ddl = new StringBuilder();
		StringBuilder ddlParquet = new StringBuilder();

		StringBuilder columnNamesCSV = new StringBuilder();

		String ddlVal;
		String ddlParquetVal;

		for (String columnName : columnMap.keySet()) {
			if (columnMap.get(columnName).equalsIgnoreCase("decimal")) {
				ddlVal = "\t`" + columnName + "` " + columnMap.get(columnName) + "(20,5),\n";
				ddlParquetVal = ddlVal;
			} else if (columnMap.get(columnName).equalsIgnoreCase("date")) {
				ddlVal = "\t`" + columnName + "` " + columnMap.get(columnName) + ",\n";
				ddlParquetVal = "\t`" + columnName + "` timestamp,\n";
			} else {
				ddlVal = "\t`" + columnName + "` " + columnMap.get(columnName) + ",\n";
				ddlParquetVal = ddlVal;
			}
			ddl.append(ddlVal);
			ddlParquet.append(ddlParquetVal);
			columnNamesCSV.append("`" + columnName + "`, ");
		}

		return new Wrapper(ddl, ddlParquet, columnNamesCSV, null);
	}

	private void writeScript(String script, String targetSchemaName, String hdfsUrl, String hdfsPath, String jobId) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", hdfsUrl);
			Path path = new Path(hdfsPath + File.separator + "hiveScripts" + File.separator + targetSchemaName + "_"
					+ jobId + ".sql");
			FSDataOutputStream os = FileSystem.create(FileSystem.get(conf), path, FsPermission.valueOf("-rw-rw-rw-"));
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(os));
			bufferedWriter.write(script);
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			File file = new File(targetSchemaName + "_" + jobId + ".sql");
			file.createNewFile();
			FileWriter writer = new FileWriter(file);
			writer.write(script);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean checkKey(String targetSchema, String parentPath, String currentPath, String key, JsonElement value) {
		return schemaColumnList.get(targetSchema).checkKey(parentPath, currentPath, key, value);
	}

	public boolean isSchemaDefined(String targetSchema) {
		return schemaDdls.containsKey(targetSchema);
	}
}

class Wrapper {
	
	StringBuilder ddl;
	StringBuilder ddlParquet;
	StringBuilder columnNamesCSV;
	StringBuilder partitionColumnNamesCSV;

	public Wrapper(StringBuilder ddl, StringBuilder ddlParquet, StringBuilder columnNamesCSV,
			StringBuilder partitionColumnNamesCSV) {
		this.ddl = ddl;
		this.ddlParquet = ddlParquet;
		this.columnNamesCSV = columnNamesCSV;
		this.partitionColumnNamesCSV = partitionColumnNamesCSV;
	}

}
