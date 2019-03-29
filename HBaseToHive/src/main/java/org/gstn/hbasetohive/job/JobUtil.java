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
package org.gstn.hbasetohive.job;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.gstn.hbasetohive.adapter.HBaseSourceTableAdapter;
import org.gstn.hbasetohive.adapter.HBaseSourceTableModel;
import org.gstn.hbasetohive.adapter.HBaseTargetAdapter;
import org.gstn.hbasetohive.adapter.HBaseTargetModel;
import org.gstn.hbasetohive.adapter.HdfsTargetAdapter;
import org.gstn.hbasetohive.adapter.HdfsTargetModel;
import org.gstn.hbasetohive.adapter.TargetModel;
import org.gstn.hbasetohive.entity.ReconEntity;
import org.gstn.hbasetohive.entity.TargetAdapterWrapper;
import org.gstn.hbasetohive.exception.ValidationException;
import org.gstn.hbasetohive.job.pojo.JobConfig;
import org.gstn.hbasetohive.job.pojo.JobOutput;
import org.gstn.hbasetohive.job.pojo.MinTimestampAndJobType;
import org.gstn.hbasetohive.job.pojo.ScanAndJobType;
import org.gstn.hbasetohive.job.pojo.SystemConfig;
import org.gstn.hbasetohive.pojo.Wrapper;
import org.gstn.hbasetohive.util.ConfigUtil;
import org.gstn.hbasetohive.util.TimeStampUtil;
import org.gstn.schemaexplorer.exception.ColumnNotFoundException;
import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.InvalidReconColumnException;
import org.gstn.schemaexplorer.exception.InvalidReconOperationException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.exception.SchemaValidationException;
import org.gstn.schemaexplorer.hbase.HBaseColumn;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.gstn.schemaexplorer.hbase.RowkeyField;
import org.gstn.schemaexplorer.hdfs.HdfsFileExplorer;
import org.gstn.schemaexplorer.hive.HiveTableExplorer;
import org.gstn.schemaexplorer.sql.SqlBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class JobUtil implements Serializable {
	
	private static Logger log = LoggerFactory.getLogger(JobUtil.class);

	static JobOutput runJob(JavaSparkContext jsc, String query, String sourceSchema, String targetSchema, String appId,
			String jobId, SystemConfig sc, JobConfig jc, HBaseTableExplorer hBaseExplorer, String targetSchemaPath,
			Map<String, List<String>> reconColumnOpMap, long maxTimestamp, String loadType)
			throws IOException, HQLException, ColumnNotFoundException, ValidationException, InvalidReconColumnException,
			InvalidReconOperationException, InvalidSchemaException {

		HdfsFileExplorer hdfsExplorer = null;
		HiveTableExplorer hiveExplorer = null;
		
		String target = jc.getTarget();
		List<String> targetFields = new ArrayList<>();
		Map<String,Class> targetJsonFieldsWithDataType = new HashMap<>();
		
		if (target.equalsIgnoreCase("hive")) {

			hiveExplorer = new HiveTableExplorer(targetSchemaPath);

			if (hiveExplorer.isSchemaDefined(targetSchema) == false) {
				throw new ValidationException("Schema definition not found for target schema: " + targetSchema);
			}

			targetFields = hiveExplorer.getSchemaColumnsForJsonValidation(targetSchema);
			//contains json column name from target n the format (parentpath#)column_name 
			targetJsonFieldsWithDataType = hiveExplorer.getSchemaJsonColumns(targetSchema);
					
		}else if (target.equalsIgnoreCase("hbase")) {

			if (hBaseExplorer.isSchemaDefined(targetSchema) == false) {
				throw new ValidationException("Schema definition not found for source schema: " + targetSchema);
			}

			targetFields = hBaseExplorer.getAllFieldNames(targetSchema);
			//contains json column name from target n the format (parentpath#)column_name
			targetJsonFieldsWithDataType = hBaseExplorer.getSchemaJsonColumns(targetSchema);
					
		} else if (target.equalsIgnoreCase("hdfs")) {

			hdfsExplorer = new HdfsFileExplorer(targetSchemaPath);

			if (hdfsExplorer.isSchemaDefined(targetSchema) == false) {
				throw new ValidationException("Schema definition not found for target schema: " + targetSchema);
			}

			targetFields = hdfsExplorer.getSchemaColumnsForJsonValidation(targetSchema);
			//contains json column name from target n the format (parentpath#)column_name
			targetJsonFieldsWithDataType = hdfsExplorer.getSchemaJsonColumns(targetSchema);
		}else {
			throw new ValidationException("No Target adapter defined for target " + target + ".....Exiting....");
		}

		SqlBean sqlBean = hBaseExplorer.parseAndGetValidatedQuery(query,targetJsonFieldsWithDataType);

		Wrapper wrapper = createHBaseRdd(jsc, hBaseExplorer, sourceSchema, targetSchema, sqlBean, sc, jc, maxTimestamp,
				loadType);

		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = wrapper.gethBaseRDD();

		boolean incremental = wrapper.getScanAndJobType().isIncremental();

		long minTimeStamp = wrapper.getScanAndJobType().getMinTimeStamp();

		if (incremental) {
			// we want to turn off recon column functionality for incremental
			// so clearing recon column map
			if (reconColumnOpMap != null) {
				reconColumnOpMap.clear();
			}
		}

		

		JavaRDD<TargetAdapterWrapper> targetAdapterWrapperRDD=null;
		JavaRDD<ReconEntity> reconRDD = null;

		TargetModel targetModel;

		String hdfsBasePath = sc.getHdfsBasePath();
		

		String hdfsFilePath = ConfigUtil.gethdfsFilePath(hdfsBasePath, target, targetSchema, appId, jobId);

		if (target.equalsIgnoreCase("hive")) {

			targetAdapterWrapperRDD = hBaseRDD
					.mapPartitions(partition -> new HdfsTargetAdapter(sc.getDestHdfsUrl(), hdfsFilePath)
							.createTargetAdapterWrapper(partition), true);

			targetModel = new HdfsTargetModel(targetSchema, hiveExplorer);

			// target is HBase
		} else if (target.equalsIgnoreCase("hbase")) {

			targetAdapterWrapperRDD = hBaseRDD.mapPartitions(
					partition -> new HBaseTargetAdapter(sc.getTargetHBaseZk(), targetSchema, hBaseExplorer)
							.createTargetAdapterWrapper(partition),
					true);

			targetModel = new HBaseTargetModel(targetSchema, hBaseExplorer);

			// target is HDFS
		} else {

			targetAdapterWrapperRDD = hBaseRDD
					.mapPartitions(partition -> new HdfsTargetAdapter(sc.getDestHdfsUrl(), hdfsFilePath)
							.createTargetAdapterWrapper(partition), true);

			targetModel = new HdfsTargetModel(targetSchema, hdfsExplorer);

		}

		List<String> sourceFields = hBaseExplorer.getAllFieldNames(sourceSchema);

		boolean sourceContainsJSON = hBaseExplorer.containsJsonColumn(sourceSchema);

		validateSchemaFields(sourceFields, targetFields, sourceContainsJSON, sourceSchema, targetSchema);

		validateReconColumns(hiveExplorer, targetSchema, reconColumnOpMap);

		validateTargetSchema(targetFields, targetSchema);

		HBaseColumn jsonColumn = validateSchemaWithSqlQuery(sqlBean, targetFields, targetSchema, hBaseExplorer,
				sourceSchema);

		validateSelectColumnsAndTargetSchema(sqlBean, sourceSchema, targetSchema, targetFields, hBaseExplorer);

		if (targetAdapterWrapperRDD != null) {
			reconRDD = targetAdapterWrapperRDD.map(
					partition -> new HBaseSourceTableAdapter(new HBaseSourceTableModel(sourceSchema, hBaseExplorer),
							targetModel).processPartition(partition, sqlBean, jsonColumn, reconColumnOpMap, sc, jc,
									hBaseExplorer, targetSchema, incremental, minTimeStamp, maxTimestamp, loadType));
		}

		return new JobOutput(appId, jobId, reconRDD, hdfsFilePath, hiveExplorer, targetSchema, sourceSchema,
				incremental, sqlBean.isDynamicColumnsInSelect());
	}

	// checks following:
	// 1. Select columns should only have static columns or dynamic columns but
	// not both
	// 2. Whether all select columns are present in target schema
	// 3. Whether all columns in target schema present in select columns/source
	// row key/dynamic parts
	// 4. target schema should have all key columns (row key columns for static
	// columns and row key+columns from dynamic part for dynamic columns)
	private static void validateSelectColumnsAndTargetSchema(SqlBean sqlBean, String sourceSchema, String targetSchema,
			List<String> targetFields, HBaseTableExplorer hBaseExplorer)
			throws ValidationException, InvalidSchemaException {

		if (sqlBean.isStaticColumnsInSelect() && sqlBean.isDynamicColumnsInSelect()) {
			throw new ValidationException("Select columns has both static columns and dynamic columns."
					+ " Please select only static or dynamic columns not both.");
		}

		Map<String, Map<String, HBaseColumn>> selectedColumns = null;
		Map<String, Map<String, HBaseColumn>> conditionColumns = null;
		
		conditionColumns = sqlBean.getCategorisedColumns().getStaticColumnsInWhereNotInSelect();
		if (sqlBean.isStaticColumnsInSelect()) {
			selectedColumns = sqlBean.getCategorisedColumns().getSelectedStaticColumns();
		} else {
			selectedColumns = sqlBean.getCategorisedColumns().getSelectedDynamicColumns();
			addIntoMap(conditionColumns,sqlBean.getCategorisedColumns().getDynamicColumnsInWhereNotInSelect());
		}

		Set<String> targetFieldsSet = new HashSet<String>();
		targetFieldsSet.addAll(targetFields);

		checkIfSelectColumnsPresentInTargetSchema(selectedColumns, targetFieldsSet, targetSchema);

		checkIfTargetColumnsPresentInSelectOrKey(selectedColumns, conditionColumns, sourceSchema, targetFieldsSet, targetSchema,
				hBaseExplorer);

		checkIfTargetHasAllKeyColumns(targetFieldsSet, hBaseExplorer, sourceSchema, targetSchema,
				sqlBean.isDynamicColumnsInSelect());

	}

	private static void addIntoMap(Map<String, Map<String, HBaseColumn>> existingMap,
			Map<String, Map<String, HBaseColumn>> mapToBeAdded) {
		for (String cf : mapToBeAdded.keySet()) {
			Map<String, HBaseColumn> existingColumns = existingMap.getOrDefault(cf, new HashMap<String, HBaseColumn>());
			Map<String, HBaseColumn> columns = mapToBeAdded.get(cf);
			existingColumns.putAll(columns);
			
			existingMap.put(cf, existingColumns);
		}
		
	}

	private static void checkIfTargetHasAllKeyColumns(Set<String> targetFieldsSet, HBaseTableExplorer hBaseExplorer,
			String sourceSchema, String targetSchema, boolean dynamicColumnsInSelect)
			throws InvalidSchemaException, ValidationException {

		StringBuilder missingColumns = new StringBuilder();
		List<String> keyColumns = new ArrayList<>();

		List<RowkeyField> rowkeyFields = hBaseExplorer.getRowkeyFields(sourceSchema);
		for (RowkeyField rowkeyField : rowkeyFields) {
			if(!rowkeyField.isHashed() && !rowkeyField.isLiteral()){
				keyColumns.add(rowkeyField.getName());
			}
		}

		if (dynamicColumnsInSelect) {
			keyColumns.addAll(hBaseExplorer.getDynamicPartNames(sourceSchema));
		}

		for (String keyColumn : keyColumns) {
			if (!targetFieldsSet.contains(keyColumn) && !keyColumn.equals("SKIP")) {
				missingColumns.append(keyColumn + ", ");
			}
		}

		if (missingColumns.length() > 0) {
			// remove last ", "
			missingColumns.delete(missingColumns.length() - 2, missingColumns.length());

			throw new ValidationException("Target schema '" + targetSchema + "' doesn't have key column(s) '"
					+ missingColumns.toString() + "'.");
		}
	}

	private static void checkIfTargetColumnsPresentInSelectOrKey(Map<String, Map<String, HBaseColumn>> selectedColumns,
			Map<String, Map<String, HBaseColumn>> conditionColumns, String sourceSchema, Set<String> targetFieldsSet, String targetSchema, HBaseTableExplorer hBaseExplorer)
			throws ValidationException {
		
		String JSON_FIELD_PATTERN = "JSON.";
		Set<String> selectedColNames = new HashSet<>();
		Set<String> conditionColumnNames = new HashSet<>();
		
		for (String cf : selectedColumns.keySet()) {
			selectedColNames.addAll(selectedColumns.get(cf).keySet());
		}
		
		for (String cf : conditionColumns.keySet()) {
			conditionColumnNames.addAll(conditionColumns.get(cf).keySet());
		}
		
		StringBuilder missingColumns = new StringBuilder();

		for (String targetColumn : targetFieldsSet) {
			if (!targetColumn.startsWith(JSON_FIELD_PATTERN)) {
				// check if it's in select columns
				if (!selectedColNames.contains(targetColumn)) {
					// check if it's a key column
					if (!hBaseExplorer.isColumnPresentInRowkey(sourceSchema, targetColumn)) {
						// check if it's column from dynamic component
						if (!hBaseExplorer.isColumnPresentInDynamicParts(sourceSchema, targetColumn)) {
							// check if it's column from where clause
							if(!conditionColumnNames.contains(targetColumn)){
								missingColumns.append(targetColumn + ", ");
							}
						}
					}
				}
			}
		}

		if (missingColumns.length() > 0) {
			// remove last ", "
			missingColumns.delete(missingColumns.length() - 2, missingColumns.length());

			throw new ValidationException("Target schema '" + targetSchema + "' has columns '"
					+ missingColumns.toString()
					+ "' which are not present in select columns/source row key components/dynamic components.");
		}
	}

	private static void checkIfSelectColumnsPresentInTargetSchema(Map<String, Map<String, HBaseColumn>> selectedColumns,
			Set<String> targetFields, String targetSchema) throws ValidationException {

		StringBuilder missingColumns = new StringBuilder();

		for (String cf : selectedColumns.keySet()) {
			Map<String, HBaseColumn> cnMap = selectedColumns.get(cf);
			for (String cn : cnMap.keySet()) {
				HBaseColumn baseColumn = cnMap.get(cn);
				
				if (!baseColumn.getDataType().equalsIgnoreCase("json") && !targetFields.contains(cn)) {
					missingColumns.append(baseColumn.getColumnName() + ", ");
				}
			}
		}

		if (missingColumns.length() > 0) {
			// remove last ", "
			missingColumns.delete(missingColumns.length() - 2, missingColumns.length());

			throw new ValidationException("Select columns has column(s) '" + missingColumns.toString()
					+ "' which is(are) not present in target schema '" + targetSchema+"'.");
		}
	}

	// validation for json cases.
	// if target has JSON. fields -> select clause (extended) should only have
	// one column and that should be of json data type
	// return HQLRunTimeException if validation fails, otherwise return
	// jsonColumnField if present.
	private static HBaseColumn validateSchemaWithSqlQuery(SqlBean sqlBean, List<String> targetFields,
			String targetSchema, HBaseTableExplorer hBaseExplorer, String sourceSchema) throws ValidationException {

		boolean targetHasJSONFields = false;
		for (String targetField : targetFields) {
			if (targetField.startsWith("JSON.")) {
				targetHasJSONFields = true;
				break;
			}
		}

		HBaseColumn selectedJsonColumn = null;

		if (targetHasJSONFields) {
			HashSet<List<String>> selectedColumns = new HashSet<>();
			selectedColumns.addAll(sqlBean.getColumnsForSelection());
			selectedColumns.addAll(sqlBean.getSelectedColumnPatterns());
			if (selectedColumns.size() == 1) {
				List<String> field = selectedColumns.iterator().next();
				// fetch data type
				String dataType = field.get(2);

				if (!dataType.equalsIgnoreCase("json")) {
					// fetching original column name from sql clause
					String colNameFromSelectClause = sqlBean.getColumnList().get(0).get(1);
					throw new ValidationException("Target schema " + targetSchema + " has JSON column(s), "
							+ "but data type of selected column pattern " + colNameFromSelectClause + " is " + dataType
							+ "...Please select a column of json data type.");
				} else {
					// targetHasJSONFields and select clause has only one column
					// and it's of json type
					// let's find the exact jsonColumnName from source schema to
					// pass it in the job
					String columnFamilyPattern = field.get(0);
					String column_name_pattern = field.get(1);

					Pattern p = Pattern.compile("^.*\\.\\*.*$");

					String columnName = column_name_pattern;
					if (p.matcher(column_name_pattern).matches()) {
						columnName = column_name_pattern.substring(1, column_name_pattern.length() - 1);
						columnName = columnName.replace(".*", "<X>");
					}

					Set<HBaseColumn> matchingColumns = hBaseExplorer.getJsonColumnName(sourceSchema,
							columnFamilyPattern, columnName);

					// for HbaseToHive we are assuming that in select clause we
					// will also get column family
					// so matchingColumns can have only one match
					selectedJsonColumn = matchingColumns.iterator().next();
				}
			} else {
				// System.out.println("selectedColumns: "+selectedColumns);
				throw new ValidationException("As Target schema " + targetSchema + " has JSON column(s)"
						+ ", select clause should only contain one column of type JSON.");
			}
		} else {
			// target doesn't have JSON. fields....validate that select clause
			// doesn't contain a field of json type
			HashSet<List<String>> selectedColumns = new HashSet<>();
			selectedColumns.addAll(sqlBean.getColumnsForSelection());
			selectedColumns.addAll(sqlBean.getSelectedColumnPatterns());
			List<String> jsonColumnsFromSql = new ArrayList<>();
			for (List<String> field : selectedColumns) {
				String dataType = field.get(2);
				if (dataType.equalsIgnoreCase("json")) {
					// fetching original column name from sql clause
					jsonColumnsFromSql.add(field.get(1));
				}
			}

			if (!jsonColumnsFromSql.isEmpty()) {
				throw new ValidationException("Target schema " + targetSchema + " doesn't contain JSON column(s), "
						+ "but select clause contains following column name patterns " + jsonColumnsFromSql
						+ ", which have data type as json.");
			}

		}
		return selectedJsonColumn;
	}

	private static void validateReconColumns(HiveTableExplorer hiveExplorer, String schemaName,
			Map<String, List<String>> reconColumnOpMap)
			throws InvalidReconColumnException, InvalidReconOperationException {

		for (String column : reconColumnOpMap.keySet()) {
			for (String operation : reconColumnOpMap.get(column)) {
				String dataType = hiveExplorer.getColumnDataType(schemaName, column);
				if (dataType == null) {
					throw new InvalidReconColumnException(column + " column not present in Target schema");
				} else if ((dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("date")
						|| dataType.equalsIgnoreCase("boolean")) && operation.equalsIgnoreCase("sum")) {
					throw new InvalidReconOperationException(
							"Cannot do a sum operation on " + dataType + " data type.");
				}
			}
		}
	}

	private static void validateTargetSchema(List<String> targetFields, String targetSchema) {

		Set<String> set = new HashSet<>();
		StringBuilder invalidPartitionsSB = new StringBuilder("");
		String invalidPartitions;

		for (String column : targetFields) {
			if (!set.add(column)) {
				invalidPartitionsSB.append(column).append(", ");
			}
		}
		if (invalidPartitionsSB.length() > 0) {
			// delete ", " at the end
			invalidPartitionsSB.delete(invalidPartitionsSB.length() - 2, invalidPartitionsSB.length());
		}
		invalidPartitions = invalidPartitionsSB.toString();
		if (!invalidPartitions.equals("")) {
			throw new SchemaValidationException("Target schema " + targetSchema + " has column(s) " + invalidPartitions
					+ " which is(are) defined both in column and partition lists");
		}
	}

	private static void validateSchemaFields(List<String> sourceFields, List<String> targetFields,
			boolean sourceContainsJSON, String sourceSchema, String targetSchema) {

		String JSON_FIELD_PATTERN = "JSON.";
		StringBuilder invalidColumnsSB = new StringBuilder("");
		String invalidColumns;

		for (String field : targetFields) {
			// good case - target field is present in source. continue checking
			// the next target field
			if (sourceFields.contains(field)) {
				continue;
			}
			// skip JSON fields in target only if the source contains any JSON
			// field
			if (sourceContainsJSON && field.startsWith(JSON_FIELD_PATTERN)) {
				continue;
			}
			invalidColumnsSB.append(field).append(", ");
		}

		if (invalidColumnsSB.length() > 0) {
			// delete ", " at the end
			invalidColumnsSB.delete(invalidColumnsSB.length() - 2, invalidColumnsSB.length());
		}

		invalidColumns = invalidColumnsSB.toString();

		if (!invalidColumns.equals("")) {
			throw new SchemaValidationException("Target schema " + targetSchema + " has column(s) " + invalidColumns
					+ " which is(are) not found in source schema " + sourceSchema);
		}
	}

	public static ScanAndJobType getScanAndJobTypeFromSql(HBaseTableExplorer hBaseTableExplorer, SqlBean sqlBean,
			SystemConfig systemConfig, JobConfig jobConfig, String sourceSchema, String targetSchema, long maxTimestamp,
			String loadType) throws HQLException, IOException {

		boolean incremental = false;
		long minTimestamp = 0;

		Scan scan = hBaseTableExplorer.getScanInstanceFromValidatedQuery(sqlBean);
		String sourceZK = systemConfig.getSourceHBaseZk();

		if (loadType.equalsIgnoreCase("partial")) {
			if (jobConfig.getMinTimeStamp() != null && jobConfig.getMaxTimeStamp() != null) {
				scan.setTimeRange(jobConfig.getMinTimeStamp(), jobConfig.getMaxTimeStamp());
			} else if (jobConfig.getMinTimeStamp() != null && jobConfig.getMaxTimeStamp() == null) {
				scan.setTimeRange(jobConfig.getMinTimeStamp(), System.currentTimeMillis());
			} else if (jobConfig.getMinTimeStamp() == null && jobConfig.getMaxTimeStamp() != null) {
				scan.setTimeRange(0L, jobConfig.getMaxTimeStamp());
			}
		} else if (loadType.equalsIgnoreCase("incremental")) {
			MinTimestampAndJobType minTimestampAndJobType = TimeStampUtil.getMinTimestampAndJobType(sourceSchema,
					targetSchema, systemConfig);
			minTimestamp = minTimestampAndJobType.getMinTimestamp();

			incremental = minTimestampAndJobType.isIncremental();
			// min timestamp for scan is set as (minTimestamp-90 days) to allow
			// delete markers
			// corresponding to deleteColumn mutations for last three months.
			// Mutations other than
			// deleteColumn having timestamp less than minTimestamp will be
			// filtered out while processing
			long minTimestampForDeleteColumn = (minTimestamp - 7776000000l);

			if (minTimestampForDeleteColumn < 0) {
				minTimestampForDeleteColumn = 0;
			}

			if(log.isDebugEnabled())log.debug("minTimestampForDeleteColumn: "+new Date(minTimestampForDeleteColumn));
			
			scan.setTimeRange(minTimestampForDeleteColumn, maxTimestamp);

			if (incremental) {
				// set raw flag in scan to receive delete markers in result
				scan.setRaw(true);
			}
		}
		return new ScanAndJobType(scan, minTimestamp, incremental);
	}

	private static Wrapper createHBaseRdd(JavaSparkContext jsc, HBaseTableExplorer hBaseExplorer, String sourceSchema,
			String targetSchema, SqlBean sqlQuery, SystemConfig systemConfig, JobConfig jobConfig, long maxTimestamp,
			String loadType) throws HQLException, IOException, ValidationException {

		String sourceTableName = null;
		String sourceZK = systemConfig.getSourceHBaseZk();

		sourceTableName = hBaseExplorer.getTableName(sourceSchema);

		ScanAndJobType scanAndJobType = getScanAndJobTypeFromSql(hBaseExplorer, sqlQuery, systemConfig, jobConfig,
				sourceSchema, targetSchema, maxTimestamp, loadType);

		Scan scan = scanAndJobType.getScan();

		Configuration conf = generateConf(scan, sourceZK, sourceTableName);

		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);

		// repartitioning if present in config xml
		if (systemConfig.getNumberOfPartitions() != null) {
			hBaseRDD = hBaseRDD.repartition(systemConfig.getPartitions());
		}

		Wrapper wrapper = new Wrapper(hBaseRDD, scanAndJobType);
		return wrapper;
	}

	private static Configuration generateConf(Scan scan, String sourceZK, String sourceTableName) throws IOException {

		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", sourceZK);
		conf.set(TableInputFormat.INPUT_TABLE, sourceTableName);
		conf.set(TableInputFormat.SCAN, convertScanToString(scan));

		return conf;
	}

	private static String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}

}
