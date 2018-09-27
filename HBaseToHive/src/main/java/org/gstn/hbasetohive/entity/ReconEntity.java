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
package org.gstn.hbasetohive.entity;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gstn.hbasetohive.pojo.DeletionMetadata;
import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.exception.ColumnNotFoundException;

public class ReconEntity {

	private Long rowCount;
	private Map<String, BigDecimal> reconColumns;

	private boolean insertAdded;
	private boolean deleteAllAdded;
	private boolean deleteSingleAdded;

	public ReconEntity() {
		rowCount = 0L;
		reconColumns = new HashMap<String, BigDecimal>();
	}

	public Long getRowCount() {
		return rowCount;
	}

	public void setRowCount(Long rowCount) {
		this.rowCount = rowCount;
	}

	public void addRowCount(Long rowCount) {
		this.rowCount += rowCount;
	}

	private Set<String> getReconColumnNames() {
		return reconColumns.keySet();
	}

	public BigDecimal getReconValue(String reconEntityKey) {
		return reconColumns.getOrDefault(reconEntityKey, BigDecimal.ZERO);
	}

	public void addReconEntity(ReconEntity reconEntity) {

		addRowCount(reconEntity.getRowCount());

		if (!this.insertAdded && reconEntity.insertAdded) {
			this.insertAdded = reconEntity.insertAdded;
		}

		checkAndSetDeletionFlags(reconEntity.deleteAllAdded, reconEntity.deleteSingleAdded);

		for (String column : reconEntity.getReconColumnNames()) {
			if (!reconColumns.containsKey(column)) {
				reconColumns.put(column, convertToBigDecimal(0));
			}
			BigDecimal res = addBigDecimal(reconColumns.get(column), reconEntity.getReconValue(column));
			reconColumns.put(column, res);
		}
	}

	private void checkAndSetDeletionFlags(boolean deleteAllAdded, boolean deleteSingleAdded) {
		if (!this.deleteAllAdded && deleteAllAdded) {
			this.deleteAllAdded = deleteAllAdded;
		}

		if (!this.deleteSingleAdded && deleteSingleAdded) {
			this.deleteSingleAdded = deleteSingleAdded;
		}

	}

	public void add(DataRecord dataRecord, Map<String, List<String>> reconColOpMap) throws ColumnNotFoundException {

		this.addRowCount(1L);
		this.insertAdded = true;

		for (String column : reconColOpMap.keySet()) {

			for (String operation : reconColOpMap.get(column)) {

				String reconEntityKey = column + "_" + operation;

				BigDecimal res = convertToBigDecimal(0);

				if (!reconColumns.containsKey(reconEntityKey)) {
					reconColumns.put(reconEntityKey, convertToBigDecimal(0));
				}

				if (operation.equalsIgnoreCase("sum") && dataRecord.isColumnNamePresent(column)
						&& !dataRecord.getColumnTupleValue(column).equals("null")
						&& !dataRecord.getColumnTupleValue(column).equals("\\N")) {
					res = addBigDecimal(reconColumns.get(reconEntityKey),
							convertToBigDecimal(dataRecord.getColumnTupleValue(column)));
					reconColumns.put(reconEntityKey, res);
				} else if (operation.equalsIgnoreCase("count") && dataRecord.isColumnNamePresent(column)
						&& !dataRecord.getColumnTupleValue(column).equals("null")
						&& !dataRecord.getColumnTupleValue(column).equals("\\N")) {
					res = addBigDecimal(reconColumns.get(reconEntityKey), convertToBigDecimal(1));
					reconColumns.put(reconEntityKey, res);
				}
			}
		}
	}

	BigDecimal convertToBigDecimal(int a) {
		return new BigDecimal(BigInteger.valueOf(a), new MathContext(30));
	}

	BigDecimal convertToBigDecimal(String a) {
		return new BigDecimal(a, new MathContext(30));
	}

	BigDecimal addBigDecimal(BigDecimal a, BigDecimal b) {
		return a.add(b, new MathContext(30));
	}

	public void addDeletionMetadata(DeletionMetadata deletionMetadata) {
		checkAndSetDeletionFlags(deletionMetadata.isDeleteAllAdded(), deletionMetadata.isDeleteSingleAdded());
	}

	public boolean isInsertAdded() {
		return insertAdded;
	}

	public boolean isDeleteAllAdded() {
		return deleteAllAdded;
	}

	public boolean isDeleteSingleAdded() {
		return deleteSingleAdded;
	}

	public boolean areHdfsFilesCreated() {
		return insertAdded || deleteAllAdded || deleteSingleAdded;
	}

}
