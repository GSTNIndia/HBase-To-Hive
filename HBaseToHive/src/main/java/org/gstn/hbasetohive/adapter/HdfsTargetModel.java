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

import org.gstn.hbasetohive.util.FormatDateUtil;
import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.entity.Tuple;
import org.gstn.schemaexplorer.target.TargetExplorer;

import com.google.gson.JsonElement;

/**
 * This class is an implementation of TargetModel for target HDFS.
 *
 */
@SuppressWarnings("serial")
public class HdfsTargetModel implements TargetModel, Serializable {

	private final TargetExplorer targetExplorer;
	private final String targetSchema;

	/**
	 * @param targetSchema
	 *            Name of the target HDFS file schema
	 * @param targetExplorer
	 *            TargetEplorer instance which has information about the target
	 *            HDFS or Hive format
	 */
	public HdfsTargetModel(String targetSchema, TargetExplorer targetExplorer) {
		this.targetSchema = targetSchema;
		this.targetExplorer = targetExplorer;
	}

	/**
	 * This method orders the columns in source dataRecord as per the order of
	 * columns required in target hdfs file
	 */
	public DataRecord structureDataRecord(DataRecord dataRecord) throws Exception {

		List<Tuple> targetColumnTuples = new ArrayList<>();

		for (String targetColumn : targetExplorer.getSchemaColumns(targetSchema)) {
			if (dataRecord.isColumnNamePresent(targetColumn)) {
				if (targetExplorer.isJsonColumn(targetSchema, targetColumn)
						&& targetExplorer.getColumnDataType(targetSchema, targetColumn).equalsIgnoreCase("date")) {
					targetColumnTuples.add(Tuple.rowkeyColumn(targetColumn,
							FormatDateUtil.FormatDate(dataRecord.getColumnTupleValue(targetColumn))));
				} else {
					targetColumnTuples
							.add(Tuple.rowkeyColumn(targetColumn, dataRecord.getColumnTupleValue(targetColumn)));
				}
			} else {
				// column not in source. write a default value for the column in
				// hdfs file
				targetColumnTuples.add(Tuple.rowkeyColumn(targetColumn,
						targetExplorer.getColumnDefaultValue(targetSchema, targetColumn)));
			}
		}
		return new DataRecord(targetColumnTuples);
	}

	@Override
	public boolean checkKey(String parentPath, String currentPath, String key, JsonElement value) {
		return targetExplorer.checkKey(targetSchema, parentPath, currentPath, key, value);
	}

	@Override
	public DataRecord structureDeleteRecord(DataRecord deleteRecord) throws Exception {
		//no restructring of delete record required for Hdfs, so returning as it is 
		return deleteRecord;
	}
}
