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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.entity.Tuple;
import org.gstn.schemaexplorer.exception.ColumnNotFoundException;
import org.gstn.schemaexplorer.exception.SchemaValidationException;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.gstn.schemaexplorer.hbase.RowkeyField;

import com.google.gson.JsonElement;

/**
 * This class is an implementation of TargetModel for target HBase.
 *
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class HBaseTargetModel implements TargetModel, Serializable {
	
	private final HBaseTableExplorer hBaseExplorer;

	private final String targetSchema;

	 /**
	 * @param targetSchema
	 * 			Name of the target HBase table schema
	 * 			
	 * @param hBaseExplorer
	 * 			HBaseTableIR instance which has information of all the user 
	 * 			defined HBase schemas
	 * 
	 * @throws SchemaValidationException
	 * 			if target table schema has a dynamic column
	 */
	public HBaseTargetModel(String targetSchema, HBaseTableExplorer hBaseExplorer) throws SchemaValidationException {
		this.targetSchema = targetSchema;
		this.hBaseExplorer = hBaseExplorer;

		// make sure the target table is a Tall table
		if(!this.hBaseExplorer.isTallTable(targetSchema)){
			throw new SchemaValidationException("Target schema "+targetSchema+" is not for a tall table. Found a dynamic column in it.");
		}
	}

	/** 
	 * @see org.gstn.hbasetohive.adapter.TargetModel#structureDataRecord(org.gstn.schemaexplorer.entity.DataRecord)
	 */
	public DataRecord structureDataRecord(DataRecord dataRecord) throws Exception {
		List<Tuple> targetKeyList = new ArrayList<>();
		List<Tuple> targetValueList = new ArrayList<>();

		boolean isSalted = false;
		byte[] salt = new byte[1];

		// Row Key components
		for (String rowkeyField : hBaseExplorer.getRowkeyFieldNames(targetSchema)) {
			
			if (hBaseExplorer.isRowkeyFieldHashed(targetSchema, rowkeyField)) {
				isSalted = true;
				salt[0] = RowkeyField.hash(dataRecord.getRowkeyTuple(rowkeyField).getColumnValue());
			} else if (hBaseExplorer.isRowkeyFieldLiteral(targetSchema, rowkeyField)) {
				String literalValue = hBaseExplorer.getRowkeyFieldLiteralValue(targetSchema, rowkeyField);
				Tuple rowColumnTuple = Tuple.rowkeyColumn(rowkeyField, literalValue);
				targetKeyList.add(rowColumnTuple);
			} else {
				// We assume data for target row key column is always present in
				// the source
				if (dataRecord.isColumnNamePresent(rowkeyField)) {

					Tuple rowColumnTuple = dataRecord.getRowkeyTuple(rowkeyField);
					targetKeyList.add(rowColumnTuple);
				} else {
					throw new ColumnNotFoundException("Row key column " + rowkeyField + " from target schema "
							+ targetSchema + " not found in record");
				}
			}
		}

		// Data columns
		for (List<String> target_cf_cn : hBaseExplorer.getAllColumnNames(targetSchema, ".*", ".*")) {
			String targetCf = target_cf_cn.get(0);
			String targetCn = target_cf_cn.get(1);

			Tuple columnTuple;
			if (dataRecord.isColumnNamePresent(targetCn)) {
				columnTuple = dataRecord.cloneTupleWithNewCF(targetCn, targetCf);
			} else {
				// Column not present in source. Create a tuple with default
				// value
				String defaultColumnValue = hBaseExplorer.getColumnDefaultValue(targetSchema, targetCn);
				Class dataType = hBaseExplorer.getColumnDataType(targetSchema, targetCn);
				columnTuple = Tuple.staticColumn(targetCf, targetCn, defaultColumnValue, dataType);
			}
			targetValueList.add(columnTuple);
		}

		DataRecord outputDataRecord = new DataRecord(targetKeyList, targetValueList);

		if (isSalted) {
			outputDataRecord.setSalt(salt);
		}
		return outputDataRecord;
	}

	@Override
	public boolean checkKey(String parentPath, String currentPath, String key, JsonElement value) {
		return hBaseExplorer.checkKey(targetSchema, parentPath, currentPath, key, value);
	}

	@Override
	public DataRecord structureDeleteRecord(DataRecord deleteRecord) throws Exception {
		String targetRowKeySeparator = hBaseExplorer.getRowKeySeparator(targetSchema);
		
		//regex value to be used as values if row key component is not found
		String nonHashRegexValue = "[^\\"+targetRowKeySeparator+"]*";
		String hashRegexValue = ".*";
		
		List<Tuple> targetKeyList = new ArrayList<>();
		List<Tuple> targetValueList = new ArrayList<>();

		boolean isSalted = false;
		byte[] salt = new byte[1];
		boolean regexInKey = false;
		
		// Row Key components
		for (RowkeyField rowkeyField : hBaseExplorer.getRowkeyFields(targetSchema)) {
			String rowkeyFieldName = rowkeyField.getName();

			if (deleteRecord.isColumnNamePresent(rowkeyFieldName)) {
				if (rowkeyField.isHashed()) {
					isSalted = true;
					salt[0] = RowkeyField.hash(deleteRecord.getRowkeyTuple(rowkeyFieldName).getColumnValue());
				} else if (rowkeyField.isLiteral()) {
					Tuple rowColumnTuple = Tuple.rowkeyColumn(rowkeyFieldName, rowkeyField.getLiteralValue());
					targetKeyList.add(rowColumnTuple);
				} else {
					Tuple rowColumnTuple = deleteRecord.getRowkeyTuple(rowkeyFieldName);
					targetKeyList.add(rowColumnTuple);
				}
			}else {
				Tuple rowColumnTuple;
				if (rowkeyField.isHashed()) {
					rowColumnTuple = Tuple.rowkeyColumn(rowkeyFieldName, hashRegexValue);
				}else{
					rowColumnTuple = Tuple.rowkeyColumn(rowkeyFieldName, nonHashRegexValue);
				}
				
				targetKeyList.add(rowColumnTuple);
				regexInKey=true;
			}
		}

		DataRecord outputDataRecord = new DataRecord(targetKeyList, targetValueList);
		outputDataRecord.setRegexInKey(regexInKey);
		
		if (isSalted) {
			outputDataRecord.setSalt(salt);
		}

		return outputDataRecord;
	}

	@Override
	public Map<String, Class> getSchemaJsonColumns() {
		return hBaseExplorer.getSchemaJsonColumns(targetSchema);
	}

}
