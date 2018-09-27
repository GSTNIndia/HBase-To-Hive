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
package org.gstn.hbasetohive.adapter;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.gstn.hbasetohive.entity.ReconEntity;
import org.gstn.hbasetohive.entity.TargetAdapterWrapper;
import org.gstn.hbasetohive.job.JobUtil;
import org.gstn.hbasetohive.job.pojo.JobConfig;
import org.gstn.hbasetohive.job.pojo.ScanAndJobType;
import org.gstn.hbasetohive.job.pojo.SystemConfig;
import org.gstn.hbasetohive.pojo.DeleteFamilyMarkerInfo;
import org.gstn.hbasetohive.pojo.DeletionMetadata;
import org.gstn.hbasetohive.pojo.MutationsForAGroup;
import org.gstn.hbasetohive.pojo.MutationsInfo;
import org.gstn.hbasetohive.pojo.ProcessMutationResult;
import org.gstn.hbasetohive.util.HBaseConnectionManager;
import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.entity.DynamicColumnType;
import org.gstn.schemaexplorer.entity.Tuple;
import org.gstn.schemaexplorer.exception.InvalidColumnException;
import org.gstn.schemaexplorer.exception.InvalidJSONException;
import org.gstn.schemaexplorer.exception.InvalidRecordTypeExcepton;
import org.gstn.schemaexplorer.hbase.HBaseColumn;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.gstn.schemaexplorer.sql.ConditionTree;
import org.gstn.schemaexplorer.sql.ConditionTreeManager;
import org.gstn.schemaexplorer.sql.SqlBean;
import org.gstn.schemaexplorer.util.DataTypeUtil;

import scala.Tuple2;

/**
 * This class processes each row from source HBase table. It transforms data
 * from HBase result object into custom format, sends it to target adapter for
 * writing into target.
 *
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class HBaseSourceTableAdapter implements Serializable {

	private HBaseSourceTableModel hBaseSourceTableModel;
	private TargetModel targetModel;
	private String SKIP = "SKIP";

	/**
	 * Constructor
	 * 
	 * @param sourceTableModel
	 *            instance of HBaseSourceTableModel for the source table that
	 *            encapsulates different components of source table
	 * @param targetModel
	 *            instance of TargetModel for the target table that encapsulates
	 *            different components of target table
	 */
	public HBaseSourceTableAdapter(HBaseSourceTableModel sourceTableModel, TargetModel targetModel) {
		this.hBaseSourceTableModel = sourceTableModel;
		this.targetModel = targetModel;
	}

	public ReconEntity processResult(Result result, byte[] rowKey, TargetAdapter targetAdapter, SqlBean sqlQuery,
			HBaseColumn jsonColumnField, Map<String, List<String>> reconColumnOpMap, SystemConfig systemConfig,
			Scan orgScan, long minTimestamp, boolean incremental) throws Exception {

		ReconEntity reconEntity = new ReconEntity();

		// do nothing - if the input resultset or rowkey is empty
		if (null == result || result.isEmpty() || null == rowKey) {
			return reconEntity;
		}

		NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultData = result.getNoVersionMap();

		// do nothing - if the input result has no data
		if (null == resultData || resultData.isEmpty()) {
			return reconEntity;
		}

		if (!incremental) {
			return processResultData(resultData, rowKey, targetAdapter, sqlQuery, jsonColumnField, reconColumnOpMap);
		} else {
			return processRawResult(result, rowKey, targetAdapter, sqlQuery, jsonColumnField, reconColumnOpMap,
					systemConfig, orgScan, minTimestamp);
		}
	}

	public ReconEntity processRawResult(Result result, byte[] rowKey, TargetAdapter targetAdapter, SqlBean sqlQuery,
			HBaseColumn jsonColumnField, Map<String, List<String>> reconColumnOpMap, SystemConfig systemConfig,
			Scan orgScan, long minTimestamp) throws Exception {

		ReconEntity reconEntity = new ReconEntity();

		DataRecord rowKeyRecord = hBaseSourceTableModel.parseRowKey(rowKey);
		// do nothing - if parseRowKey doesn't return any columns
		if (rowKeyRecord == null || rowKeyRecord.isRowkeyListEmpty()) {
			return reconEntity;
		}

		Boolean passed = applyRowKeyConditions(sqlQuery, rowKeyRecord);
		if (passed != null && !passed) {
			return reconEntity;
		}

		boolean deletionAddedForAllRows = false;

		// check if the hbase scan result has deleteFamily marker for selected
		// families
		DeleteFamilyMarkerInfo deleteFamilyMarkerInfo = processDeleteFamilyMarkers(result, sqlQuery, minTimestamp);

		boolean reprocess = false;
		List<DataRecord> deleteRowList = new ArrayList<>();

		if (deleteFamilyMarkerInfo.isDeleteFamilyMarkerFound()) {
			// add entry in the deleteRowList
			addIntoDeleteRowList(deleteRowList, rowKeyRecord, null);

			deletionAddedForAllRows = true;

			if (!deleteFamilyMarkerInfo.isDeleteFamilyMarkerFoundForAll()) {
				// we need to process the entire source row again. Create a new
				// scan using the original scan.
				reprocess = true;
				reconEntity = reprocessThisRow(rowKey, targetAdapter, sqlQuery, jsonColumnField, reconColumnOpMap,
						orgScan, systemConfig);

			}
		}

		if (!reprocess) {
			ProcessMutationResult processMutationResult = processMutationsOtherThanDeleteFamily(result,
					deleteFamilyMarkerInfo, sqlQuery, deletionAddedForAllRows, rowKeyRecord, deleteRowList, orgScan,
					minTimestamp, systemConfig);
			if (processMutationResult.isReprocessRow()) {
				if (!deletionAddedForAllRows) {
					deleteRowList.clear();
					addIntoDeleteRowList(deleteRowList, rowKeyRecord, null);
					deletionAddedForAllRows = true;
				}
				reconEntity = reprocessThisRow(rowKey, targetAdapter, sqlQuery, jsonColumnField, reconColumnOpMap,
						orgScan, systemConfig);
			} else {
				reconEntity = processResultData(processMutationResult.getNoVersionMap(), rowKey, targetAdapter,
						sqlQuery, jsonColumnField, reconColumnOpMap);
			}
		}

		DeletionMetadata deletionMetadata = targetAdapter.processDeleteRowList(targetModel, deleteRowList);
		reconEntity.addDeletionMetadata(deletionMetadata);

		return reconEntity;
	}

	private ProcessMutationResult processMutationsOtherThanDeleteFamily(Result result,
			DeleteFamilyMarkerInfo deleteFamilyMarkerInfo, SqlBean sqlQuery, boolean deletionAddedForAllRows,
			DataRecord rowKeyRecord, List<DataRecord> deleteRowList, Scan orgScan, long minTimestamp,
			SystemConfig systemConfig)
			throws IOException, ParseException, InvalidColumnException, InvalidRecordTypeExcepton {

		// map of groupid (null for static or value of <X> for dynamic) against
		// mutations for that group
		MutationsInfo mutationsInfo = filterAndGroupMutations(result, deleteFamilyMarkerInfo, sqlQuery, minTimestamp);

		// check if there is atleast one mutation was found
		if (mutationsInfo.getMutations().isEmpty()) {
			return new ProcessMutationResult(null, false);
		}

		// Consider case where select columns are dynamic and where condition
		// involves static. Now if mutations involves static column then we will
		// have to reprocess the entire source row again for evaluating where
		// condition.
		// so let's add deletion for all rows if not added already and reprocess
		// the row.
		if (sqlQuery.isDynamicColumnsInSelect() && sqlQuery.isStaticColumnsInWhere()
				&& mutationsInfo.isMutationForStaticPresent()) {
			// need to reprocess entire row
			return new ProcessMutationResult(null, true);
		}

		if (!deletionAddedForAllRows) {
			addDeleteionsForMutations(mutationsInfo, rowKeyRecord, deleteRowList);
		}

		Result missingColumnsResult = null;
		// If deleteFamilyMarkerFoundForAll, we will have all the latest
		// informaton in mutations,
		// we don't need to look for missing columns as they would be missing
		// from source row as well.
		if (!deleteFamilyMarkerInfo.isDeleteFamilyMarkerFoundForAll()) {
			missingColumnsResult = getMissingColumns(mutationsInfo, orgScan, sqlQuery, result.getRow(), systemConfig);
		}

		NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap;
		if (missingColumnsResult != null && !missingColumnsResult.isEmpty()) {
			noVersionMap = missingColumnsResult.getNoVersionMap();
		} else {
			noVersionMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
		}

		// add cells from put mutations into missingColumnsResult
		addColumnsFromPutMutations(noVersionMap, mutationsInfo);

		return new ProcessMutationResult(noVersionMap, false);
	}

	private void addColumnsFromPutMutations(NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap,
			MutationsInfo mutationsInfo) {
		for (Entry<String, MutationsForAGroup> entry : mutationsInfo.getMutations().entrySet()) {
			DataRecord putMutations = entry.getValue().getPutMutations();

			Map<String, Map<String, Tuple>> columnFamilycolumnNameTupleMap = putMutations
					.getColumnFamilycolumnNameTupleMap();

			for (String cf : columnFamilycolumnNameTupleMap.keySet()) {
				byte[] cfBytes = Bytes.toBytes(cf);
				NavigableMap<byte[], byte[]> columns = noVersionMap.get(cfBytes);
				if (columns == null) {
					columns = new TreeMap<>(Bytes.BYTES_COMPARATOR);
					noVersionMap.put(cfBytes, columns);
				}
				Map<String, Tuple> columnsFromMutation = columnFamilycolumnNameTupleMap.get(cf);

				for (String column : columnsFromMutation.keySet()) {
					Tuple tuple = columnsFromMutation.get(column);

					addTupleIntoMap(columns, tuple);
				}
			}
		}

	}

	private void addTupleIntoMap(NavigableMap<byte[], byte[]> columns, Tuple tuple) {
		String colName = tuple.getHbaseColumnName();

		byte[] value = DataTypeUtil.parseStringToByteArray(tuple.getColumnValue(), tuple.getColumnDataType());

		columns.put(Bytes.toBytes(colName), value);
	}

	private Result getMissingColumns(MutationsInfo mutationsInfo, Scan orgScan, SqlBean sqlQuery, byte[] rowKey,
			SystemConfig systemConfig) throws IOException {

		List<Filter> filterList = new ArrayList<>();

		for (Entry<String, MutationsForAGroup> entry : mutationsInfo.getMutations().entrySet()) {
			filterList.addAll(getFiltersForMissingColumnsInAGroup(entry, sqlQuery));
		}

		// If where condition has static columns, add column filters for them
		if (sqlQuery.isStaticColumnsInWhere()) {
			Map<String, Map<String, HBaseColumn>> columnsInGroup = sqlQuery.getCategorisedColumns()
					.getStaticColumnsInWhereNotInSelect();
			MutationsForAGroup staticMutations = mutationsInfo.getMutations().get(null);
			findMissingColumnsForGroup(columnsInGroup, staticMutations, filterList, null);
		}

		if (!filterList.isEmpty()) {
			Scan missingColumnsScan = getScanForMissingColumns(orgScan, rowKey, filterList);

			ResultScanner resultScanner = fireScan(missingColumnsScan, systemConfig);

			if (resultScanner != null) {
				for (Result missingResult : resultScanner) {
					if (missingResult != null && !missingResult.isEmpty()) {
						return missingResult;
					}
					break; // as we are expecting only one row
				}
			}
		}

		return Result.EMPTY_RESULT;
	}

	private List<Filter> getFiltersForMissingColumnsInAGroup(Entry<String, MutationsForAGroup> entry,
			SqlBean sqlQuery) {
		String group = entry.getKey();
		MutationsForAGroup mutations = entry.getValue();

		List<Filter> missingColumnfilters = new ArrayList<>();

		if (group == null) {
			// static group
			Map<String, Map<String, HBaseColumn>> columnsInGroup = sqlQuery.getCategorisedColumns()
					.getSelectedStaticColumns();
			findMissingColumnsForGroup(columnsInGroup, mutations, missingColumnfilters, group);

		} else {
			// dynamic group
			Map<String, Map<String, HBaseColumn>> columnsInGroup = sqlQuery.getCategorisedColumns()
					.getSelectedDynamicColumns();
			findMissingColumnsForGroup(columnsInGroup, mutations, missingColumnfilters, group);

			columnsInGroup = sqlQuery.getCategorisedColumns().getDynamicColumnsInWhereNotInSelect();
			findMissingColumnsForGroup(columnsInGroup, mutations, missingColumnfilters, group);

		}

		return missingColumnfilters;
	}

	private void findMissingColumnsForGroup(Map<String, Map<String, HBaseColumn>> columnsInGroup,
			MutationsForAGroup mutations, List<Filter> missingColumnFilters, String group) {
		for (String cf : columnsInGroup.keySet()) {
			Map<String, HBaseColumn> columns = columnsInGroup.get(cf);
			for (String cn : columns.keySet()) {
				if (mutations == null || !mutations.isMutationPresentForColumn(cf, cn)) {
					addColumnIntoFilterList(missingColumnFilters, columns.get(cn), group);
				}
			}
		}

	}

	private void addColumnIntoFilterList(List<Filter> missingColumnFilters, HBaseColumn hbaseColumn, String group) {

		String colName = null;
		if (hbaseColumn.isDynamicColumn()) {
			// group is the value of dynamic part
			colName = hbaseColumn.getDynamicPrefix() + group + hbaseColumn.getDynamicSuffix();
		} else {
			colName = hbaseColumn.getColumnName();
		}
		Filter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(colName)));

		Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(hbaseColumn.getColumnFamily())));

		Filter columnFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, qualifierFilter, familyFilter);

		missingColumnFilters.add(columnFilter);

	}

	private Scan getScanForMissingColumns(Scan orgScan, byte[] rowKey, List<Filter> columnFilters) throws IOException {
		Scan missingColumnsScan = new Scan();
		Filter rowFilter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(rowKey));
		Filter columnFilterList = new FilterList(Operator.MUST_PASS_ONE, columnFilters);

		FilterList filters = new FilterList(Operator.MUST_PASS_ALL, rowFilter, columnFilterList);

		missingColumnsScan.setFilter(filters);
		missingColumnsScan.setTimeRange(0, orgScan.getTimeRange().getMax());

		return missingColumnsScan;
	}

	private void addDeleteionsForMutations(MutationsInfo mutationsInfo, DataRecord rowKeyRecord,
			List<DataRecord> deleteRowList) throws InvalidRecordTypeExcepton, InvalidColumnException {
		Map<String, MutationsForAGroup> mutations = mutationsInfo.getMutations();
		// if mutations are present for both static and dynamic groups, consider
		// only dynamic mutations
		boolean ignoreStatic = false;
		if (mutationsInfo.isMutationForStaticPresent() && mutationsInfo.isMutationForDyanmicPresent()) {
			ignoreStatic = true;
		}
		for (String group : mutations.keySet()) {
			if (!ignoreStatic || group != null) {
				addIntoDeleteRowList(deleteRowList, rowKeyRecord, group);
			}
		}

	}

	private void addIntoDeleteRowList(List<DataRecord> deleteRowList, DataRecord deleteRecord, String dynamicPart)
			throws InvalidRecordTypeExcepton, InvalidColumnException {
		DataRecord newRecord = deleteRecord.duplicate();
		if (dynamicPart != null) {
			for (Tuple tuple : splitDynamicPartIntoTuples(dynamicPart)) {
				newRecord.addTupleToRowkey(tuple);
			}
			newRecord.setDynamicPartsInKey(true);
		}
		deleteRowList.add(newRecord);
	}

	private MutationsInfo filterAndGroupMutations(Result result, DeleteFamilyMarkerInfo deleteFamilyMarkerInfo,
			SqlBean sqlQuery, long minTimestamp) throws ParseException, InvalidColumnException {
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultData = result.getNoVersionMap();
		Map<String, MutationsForAGroup> mutations = new HashMap<>();

		boolean mutationForStaticPresent = false;
		boolean mutationForDynamicPresent = false;

		if (null != resultData && !resultData.isEmpty()) {
			for (byte[] cf_bytes : resultData.keySet()) {
				String cf = Bytes.toString(cf_bytes);
				NavigableMap<byte[], byte[]> cn_map = resultData.get(cf_bytes);
				for (byte[] cn_bytes : cn_map.keySet()) {
					Cell cell = result.getColumnLatestCell(cf_bytes, cn_bytes);

					if (isTimestampValid(cell, cf, minTimestamp, deleteFamilyMarkerInfo)) {
						String cn = Bytes.toString(cn_bytes);
						Tuple tuple = getTupleForMutation(cell, cf, cn, sqlQuery);

						if (tuple != null) {
							addTupleIntoGroup(tuple, cell, mutations);
							if (!tuple.isDynamicColumn()) {
								mutationForStaticPresent = true;
							} else {
								mutationForDynamicPresent = true;
							}
						}
					}
				}
			}
		}

		return new MutationsInfo(mutations, mutationForStaticPresent, mutationForDynamicPresent);
	}

	private void addTupleIntoGroup(Tuple tuple, Cell cell, Map<String, MutationsForAGroup> mutations)
			throws InvalidColumnException {
		String group = null;

		// for dynamic columns group id will be dynamic value, for static it
		// will be null
		if (tuple.isDynamicColumn()) {
			group = tuple.getDynamicPartValue();
		}

		MutationsForAGroup mutationsForAGroup = mutations.get(group);

		if (mutationsForAGroup == null) {
			mutationsForAGroup = new MutationsForAGroup();
			mutations.put(group, mutationsForAGroup);
		}

		if (CellUtil.isDelete(cell)) {
			mutationsForAGroup.addTupleToDeleteMutation(tuple);
		} else {
			mutationsForAGroup.addTupleToPutMutation(tuple);
		}

	}

	private Tuple getTupleForMutation(Cell cell, String cf, String cn, SqlBean sqlQuery) throws ParseException {
		Tuple tuple = null;
		boolean valid = false;

		if (CellUtil.isDelete(cell) && !CellUtil.isDeleteFamily(cell) && !CellUtil.isDeleteFamilyVersion(cell)) {
			valid = true;
		} else if (Type.codeToType(cell.getTypeByte()).equals(Type.Put)) {
			valid = true;
		}

		if (valid) {
			tuple = parseMutation(cell, cf, cn);
			if (tuple != null) {
				// check if the column name is present in selected columns/where
				// clause columns
				Map<String, Map<String, HBaseColumn>> requiredColumns = sqlQuery.getCategorisedColumns()
						.getAllRequiredColumns();
				if (!isRequiredColumn(tuple, requiredColumns)) {
					return null;
				}
			}
		}

		return tuple;
	}

	private boolean isRequiredColumn(Tuple tuple, Map<String, Map<String, HBaseColumn>> requiredColumns) {
		Map<String, HBaseColumn> cols = requiredColumns.get(tuple.getColumnFamily());

		if (cols != null && cols.containsKey(tuple.getColumnName())) {
			return true;
		}

		return false;
	}

	private Tuple parseMutation(Cell cell, String cf, String cn) throws ParseException {
		if (hBaseSourceTableModel.isStaticColumn(cf, cn)) {
			Class dataType = hBaseSourceTableModel.getStaticColumnDataType(cf, cn);
			byte[] valueBytes = CellUtil.cloneValue(cell);
			String value = null;
			if (valueBytes != null) {
				value = hBaseSourceTableModel.getStaticColumnValue(cf, cn, valueBytes);
			}
			return Tuple.staticColumn(cf, cn, value, dataType);
		} else {
			return hBaseSourceTableModel.parseDynamicColumn(cf, cn, CellUtil.cloneValue(cell));
		}
	}

	/**
	 * If raw scan result has deleteFamily marker, then consider only those
	 * mutations of that family which have timestamp greater than the
	 * deleteFamily marker timestamp
	 */
	private boolean isTimestampValid(Cell cell, String cf, long minTimestamp,
			DeleteFamilyMarkerInfo deleteFamilyMarkerInfo) {
		Long deleteFamilyMarkerTs = deleteFamilyMarkerInfo.getDeleteFamilyTimestamp(cf);

		if (deleteFamilyMarkerTs == null || cell.getTimestamp() > deleteFamilyMarkerTs) {
			// To allow delete markers corresponding to deleteColumn mutations
			// even if they have timestamp earlier to minTimestamp for this job
			if (cell.getTimestamp() >= minTimestamp || CellUtil.isDeleteType(cell)) {
				return true;
			}
		}
		return false;
	}

	private ReconEntity reprocessThisRow(byte[] rowKey, TargetAdapter targetAdapter, SqlBean sqlQuery,
			HBaseColumn jsonColumnField, Map<String, List<String>> reconColumnOpMap, Scan orgScan,
			SystemConfig systemConfig) throws Exception {
		ReconEntity reconEntity = new ReconEntity();
		Scan newScan = getNewScanForThisRow(orgScan, rowKey);

		// fire the newScan and fetch first result, as we have added row filter
		// there can be at max one result
		ResultScanner newResultScanner = fireScan(newScan, systemConfig);

		if (newResultScanner != null) {
			for (Result newResult : newResultScanner) {
				if (newResult != null && !newResult.isEmpty()) {

					reconEntity = processResultData(newResult.getNoVersionMap(), rowKey, targetAdapter, sqlQuery,
							jsonColumnField, reconColumnOpMap);
				}

				break; // as we are expecting only one row
			}
		}
		return reconEntity;
	}

	private DeleteFamilyMarkerInfo processDeleteFamilyMarkers(Result result, SqlBean sqlQuery, long minTimestamp) {
		boolean deleteFamilyMarkerFound = false, deleteFamilyMarkerFoundForAll = false;
		Map<String, Long> deleteFamilyTimestamp = new HashMap<>();

		Set<String> selectedFamilies = sqlQuery.getCategorisedColumns().getAllRequiredColumns().keySet();
		for (String selectedCF : selectedFamilies) {

			// deleteFamily marker cell doesn't have the column name,
			// so we check for it using getColumnLatestCell by passing null col
			// name
			Cell cell = result.getColumnLatestCell(Bytes.toBytes(selectedCF), null);

			if (cell != null && cell.getTimestamp() >= minTimestamp
					&& (CellUtil.isDeleteFamily(cell) || CellUtil.isDeleteFamilyVersion(cell))) {
				deleteFamilyMarkerFound = true;
				deleteFamilyTimestamp.put(selectedCF, cell.getTimestamp());
			} else {
				deleteFamilyMarkerFoundForAll = false;
			}
		}
		return new DeleteFamilyMarkerInfo(deleteFamilyMarkerFound, deleteFamilyMarkerFoundForAll,
				deleteFamilyTimestamp);
	}

	private Boolean applyRowKeyConditions(SqlBean sqlQuery, DataRecord dataRecord) throws Exception {
		// get a copy of ConditionTree to apply the row key conditions and store
		// result of evaluation in the copy
		ConditionTree conditionTreeForRowKey = sqlQuery.getConditionTreeCopy();
		// apply static column and row key conditions from sqlQuery
		Boolean passed = ConditionTreeManager.evaluateRowKeyConditions(conditionTreeForRowKey, dataRecord);

		return passed;
	}

	private ResultScanner fireScan(Scan newScan, SystemConfig systemConfig) throws IOException {
		
		try {
			Connection con = HBaseConnectionManager.getConnection(systemConfig.getSourceHBaseZk());
			String tableName = hBaseSourceTableModel.getTableName();

			try (Table table = con.getTable(TableName.valueOf(tableName))) {
				return table.getScanner(newScan);
			}
		} catch (IOException e) {
			System.err.println("Error in fireScan: " + e.getMessage());
			throw e;
		}
	}

	private Scan getNewScanForThisRow(Scan orgScan, byte[] rowKey) throws IOException {
		
		try {
			Scan newScan = new Scan(orgScan);
			newScan.setRaw(false);
			newScan.setTimeRange(0, orgScan.getTimeRange().getMax());
			// Also add rowFilter in the new scan for this row.
			Filter rowFilter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(rowKey));
			if (orgScan.hasFilter()) {
				Filter orgFilter = orgScan.getFilter();
				FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, orgFilter, rowFilter);
				newScan.setFilter(filterList);
			} else {
				newScan.setFilter(rowFilter);
			}
			return newScan;
		} catch (IOException e) {
			System.err.println("Error in getNewScanForThisRow: " + e.getMessage());
			throw e;
		}
	}

	/**
	 * parses a hbase row to form one/more target rows and calls targetAdapter's
	 * write method for corresponding target rows.
	 * 
	 * @param result
	 *            object encapsulating source hbase row
	 * @param rowKey
	 *            byte[] of source hbase row key
	 * @param targetAdapter
	 *            adapter for the desired Target type
	 * @param query
	 *            object containing data of the sql query
	 * @param jsonColumnField
	 *            ColumnField for json column to be selected from source hbase
	 *            table
	 * @param reconColumnOpMap
	 *            object containing information about reconciliation to be done
	 * @return Returns the ReconEntity object containing number of target rows
	 *         written and reconciliation output corresponding to input source
	 *         hbase row
	 * @throws Exception
	 */
	public ReconEntity processResultData(NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultData, byte[] rowKey,
			TargetAdapter targetAdapter, SqlBean query, HBaseColumn jsonColumnField,
			Map<String, List<String>> reconColumnOpMap) throws Exception {

		ReconEntity reconEntity = new ReconEntity();

		// do nothing - if the input result has no data
		if (null == resultData || resultData.isEmpty()) {
			return reconEntity;
		}
		DataRecord outputDataRecord = hBaseSourceTableModel.parseRowKey(rowKey);
		// do nothing - if parseRowKey doesn't return any columns
		if (outputDataRecord == null || outputDataRecord.isRowkeyListEmpty()) {
			return reconEntity;
		}
		// fetch all static columns
		Map<String, Map<String, Tuple>> staticColumnsMap = getAllStaticColumns(resultData);

		DataRecord staticColumnDataRecord = outputDataRecord.duplicate();
		staticColumnDataRecord.addColumns(staticColumnsMap);

		Boolean staticPassed = query.evaluateStaticConditions(staticColumnDataRecord);
		if (staticPassed != null && !staticPassed) {
			return reconEntity;
		}

		// map of dynamic part to map of cf to map of cn and tuple
		Map<String, Map<String, Map<String, Tuple>>> dynamicPartToCfCnMap = new HashMap<>();

		if (hBaseSourceTableModel.hasDynamicColumns()) {
			for (byte[] columnFamilyBytes : resultData.keySet()) {
				String columnFamily = Bytes.toString(columnFamilyBytes);

				Map<byte[], byte[]> columnNameValueMap = resultData.get(columnFamilyBytes);

				if (columnNameValueMap == null || columnNameValueMap.isEmpty()) {
					continue;
				}

				for (byte[] columnNameBytes : columnNameValueMap.keySet()) {
					String columnName = Bytes.toString(columnNameBytes);

					// ignore static columns. we have them in staticColumnMap
					if (hBaseSourceTableModel.isStaticColumn(columnFamily, columnName)) {
						continue;
					}

					// now processing a dynamic column

					byte[] valueBytes = columnNameValueMap.get(columnNameBytes);

					Tuple dynamicColumn = hBaseSourceTableModel.parseDynamicColumn(columnFamily, columnName,
							valueBytes);
					if (dynamicColumn != null && dynamicColumn.isDynamicColumn()) {

						Map<String, Map<String, Tuple>> cfMap = dynamicPartToCfCnMap
								.get(dynamicColumn.getDynamicPartValue());

						if (cfMap == null) {
							cfMap = new HashMap<>();
							dynamicPartToCfCnMap.put(dynamicColumn.getDynamicPartValue(), cfMap);
						}

						Map<String, Tuple> cnMap = cfMap.get(columnFamily);

						if (cnMap == null) {
							cnMap = new HashMap<>();
							cfMap.put(columnFamily, cnMap);
						}
						cnMap.put(dynamicColumn.getColumnName(), dynamicColumn);
					}
				}
			}
		}

		if (dynamicPartToCfCnMap.isEmpty()) {

			boolean dynamicPassed = query.evaluateDynamicConditions(staticColumnDataRecord);

			if (dynamicPassed) {

				boolean selectedColumnFound = isSelectedColumnFound(staticColumnDataRecord, query);

				if (selectedColumnFound) {

					if (jsonColumnField != null) {
						try {
							ReconEntity reconEntity2 = passThroughJsonAdapterAndWrite(staticColumnDataRecord,
									jsonColumnField, targetAdapter, reconColumnOpMap);
							if (reconEntity2 != null) {
								reconEntity.addReconEntity(reconEntity2);
							}
						} catch (InvalidJSONException e) {
							System.err.println(e.getMessage());
							return null;
						}
					} else {
						DataRecord structuredDR = targetAdapter.writeRow(targetModel, staticColumnDataRecord);
						reconEntity.add(structuredDR, reconColumnOpMap);
					}
				}
			}
		} else {
			// iterate over dynamic columns for each <X>
			for (String dynamicPart : dynamicPartToCfCnMap.keySet()) {

				// split the dynamic part (if the dynamic part has multiple
				// components) and each component to the row key of output
				List<Tuple> tuplesForDynamicPart = splitDynamicPartIntoTuples(dynamicPart);

				if (tuplesForDynamicPart != null && !tuplesForDynamicPart.isEmpty()) {

					Map<String, Map<String, Tuple>> cfMap = dynamicPartToCfCnMap.get(dynamicPart);

					DataRecord dataRecForADynamicPart = new DataRecord();
					dataRecForADynamicPart.addColumns(cfMap);

					for (Tuple tuple : tuplesForDynamicPart) {
						dataRecForADynamicPart.addTupleToRowkey(tuple);
					}

					boolean dynamicPassed = query.evaluateDynamicConditions(dataRecForADynamicPart);

					if (!dynamicPassed) {
						// skipping writing row for this dynamic part
						continue;
					}

					DataRecord outputDataRecordCopy = staticColumnDataRecord.duplicate();

					// adding dynamic columns for this dynamic part into data
					// record
					outputDataRecordCopy.addColumns(cfMap);
					for (Tuple tuple : tuplesForDynamicPart) {
						outputDataRecordCopy.addTupleToRowkey(tuple);
					}

					boolean selectedColumnFound = isSelectedColumnFound(outputDataRecordCopy, query);

					if (selectedColumnFound) {
						if (jsonColumnField != null) {
							try {
								ReconEntity reconEntity2 = passThroughJsonAdapterAndWrite(outputDataRecordCopy,
										jsonColumnField, targetAdapter, reconColumnOpMap);
								if (reconEntity2 != null) {
									reconEntity.addReconEntity(reconEntity2);
								}
							} catch (InvalidJSONException e) {
								System.err.println(e.getMessage());
								return null;
							}
						} else {
							DataRecord structuredDR = targetAdapter.writeRow(targetModel, outputDataRecordCopy);
							reconEntity.add(structuredDR, reconColumnOpMap);
						}
					}
				}
			}
		}
		return reconEntity;

	}

	// checks if at least one of the selected columns is present in dataRecord
	private boolean isSelectedColumnFound(DataRecord dataRecord, SqlBean sqlBean) {
		Map<String, Map<String, HBaseColumn>> selectedColumns = null;

		if (sqlBean.isStaticColumnsInSelect()) {
			selectedColumns = sqlBean.getCategorisedColumns().getSelectedStaticColumns();
		} else {
			selectedColumns = sqlBean.getCategorisedColumns().getSelectedDynamicColumns();
		}

		for (String cf : selectedColumns.keySet()) {
			Map<String, HBaseColumn> cnMap = selectedColumns.get(cf);
			for (String cn : cnMap.keySet()) {

				if (dataRecord.isColumnNamePresent(cf, cn)) {
					return true;
				}
			}
		}

		return false;
	}

	private List<Tuple> splitDynamicPartIntoTuples(String dynamicPartValue) {

		List<Tuple> tuplesFromDynamicPart = new ArrayList<>();

		String[] parts;
		String seperator = this.hBaseSourceTableModel.getDynamicColumnsSeparator();

		if (seperator == null || seperator.isEmpty()) {
			parts = new String[1];
			parts[0] = dynamicPartValue;
		} else {
			parts = dynamicPartValue.split(seperator);
		}

		List<String> dynamicPartNames = this.hBaseSourceTableModel.getDynamicPartNames();
		// ignore columns having names in different format than expected
		if (parts.length != dynamicPartNames.size()) {
			return tuplesFromDynamicPart;
		}

		for (int i = 0; i < parts.length; i++) {
			if (!dynamicPartNames.get(i).equals(SKIP)) {
				tuplesFromDynamicPart.add(Tuple.dynamicColumn(null, dynamicPartNames.get(i), parts[i], "",
						DynamicColumnType.STATIC_PREFIX, String.class));
			}
		}
		return tuplesFromDynamicPart;
	}

	/**
	 * Iterates over all the hbase Result objects belonging to a spark partition
	 * and calls processResult for each of them
	 * 
	 * @param targetAdapterWrapper
	 *            object containing all the Result objects from a spark
	 *            partition and TargetAdapter object
	 * @param sqlBean
	 *            object containing data of the sql query
	 * @param jsonColumnField
	 *            ColumnField for json column to be selected from source hbase
	 *            table
	 * @param reconColumnOpMap
	 *            object containing information about reconciliation to be done
	 * @return Returns the ReconEntity object containing number of target rows
	 *         written and reconciliation output corresponding to input source
	 *         hbase row
	 * @throws Exception
	 */
	public ReconEntity processPartition(TargetAdapterWrapper targetAdapterWrapper, SqlBean sqlBean,
			HBaseColumn jsonColumnField, Map<String, List<String>> reconColumnOpMap, SystemConfig systemConfig,
			JobConfig jobConfig, HBaseTableExplorer hBaseExp, String targetSchema, boolean incremental,
			long minTimestamp, long maxTimestamp, String loadType) throws Exception {

		TargetAdapter targetAdapter = targetAdapterWrapper.getTargetAdapter();
		Iterator<Tuple2<ImmutableBytesWritable, Result>> sparkPartitionIterator = targetAdapterWrapper
				.getSparkPartitionIterator();

		ReconEntity reconEntity = new ReconEntity();

		ScanAndJobType scanAndJobType = JobUtil.getScanAndJobTypeFromSql(hBaseExp, sqlBean, systemConfig, jobConfig,
				hBaseSourceTableModel.getSchemaName(), targetSchema, maxTimestamp, loadType);

		Scan orgScan = scanAndJobType.getScan();

		while (sparkPartitionIterator.hasNext()) {
			Tuple2<ImmutableBytesWritable, Result> tuple = sparkPartitionIterator.next();

			ReconEntity reconEntity2 = processResult(tuple._2, tuple._1.get(), targetAdapter, sqlBean, jsonColumnField,
					reconColumnOpMap, systemConfig, orgScan, minTimestamp, incremental);
			if (reconEntity2 != null) {
				reconEntity.addReconEntity(reconEntity2);
			}
			// flushing after processing each source hbase row
			targetAdapter.flush();
		}
		// closing after processing all hbase rows
		targetAdapter.close();

		return reconEntity;
	}

	private Map<String, Map<String, Tuple>> getAllStaticColumns(
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultData) throws ParseException {
		Map<String, Map<String, Tuple>> staticColumnsMap = new HashMap<>();

		if (resultData != null && !resultData.isEmpty()) {

			for (String staticColumnFamily : hBaseSourceTableModel.getStaticColumnFamilies()) {
				byte[] staticCfBytes = Bytes.toBytes(staticColumnFamily);
				NavigableMap<byte[], byte[]> cfData = resultData.get(staticCfBytes);

				for (String columnName : hBaseSourceTableModel.getStaticColumns(staticColumnFamily)) {
					String value = null;
					Class dataType = null;
					if (cfData != null) {
						byte[] columnValue = cfData.get(Bytes.toBytes(columnName));
						if (columnValue == null) {
							continue;
						}
						value = hBaseSourceTableModel.getStaticColumnValue(staticColumnFamily, columnName, columnValue);
						dataType = hBaseSourceTableModel.getStaticColumnDataType(staticColumnFamily, columnName);

						addIntoColumnsMap(staticColumnsMap, staticColumnFamily, columnName,
								Tuple.staticColumn(staticColumnFamily, columnName, value, dataType));
					}
				}
			}
		}

		return staticColumnsMap;
	}

	private void addIntoColumnsMap(Map<String, Map<String, Tuple>> cfMap, String cf, String cn, Tuple tuple) {
		Map<String, Tuple> cnMap = cfMap.get(cf);
		if (cnMap == null) {
			cnMap = new HashMap<>();
			cfMap.put(cf, cnMap);
		}
		cnMap.put(cn, tuple);
	}

	private ReconEntity passThroughJsonAdapterAndWrite(DataRecord ipRecord, HBaseColumn column,
			TargetAdapter targetAdapter, Map<String, List<String>> reconColumnOpMap) throws Exception {

		String json;
		ReconEntity reconEntity = new ReconEntity();

		if (column != null) {
			if (ipRecord.isColumnNamePresent(column.getColumnNameWithoutDynamicComponent())) {
				json = ipRecord.getColumnTupleValue(column.getColumnNameWithoutDynamicComponent());
				// System.out.println("****** Json " + json);
				if (json != null) {
					List<DataRecord> dataRecords = JsonAdapter.flattenJson(ipRecord, json, targetModel);
					for (DataRecord dataRecord : dataRecords) {
						DataRecord structuredDR = targetAdapter.writeRow(targetModel, dataRecord);
						reconEntity.add(structuredDR, reconColumnOpMap);
					}
				}
			}
		}
		return reconEntity;
	}

}
