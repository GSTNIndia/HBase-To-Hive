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
package org.gstn.schemaexplorer.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.sql.SqlBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class stores information about the row key structure like different
 * fields of row key and the separator between them
 *
 */
@SuppressWarnings("serial")
public class Rowkey implements Serializable {

	private List<RowkeyField> rowkeyFields;
	private Logger logger;
	private String rowkeySeparator;

	public Rowkey() {
		rowkeyFields = new ArrayList<>();
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
	}

	public String getRowkeySeparator() {
		return rowkeySeparator;
	}

	// row key fields are formed in the order in which they are added
	public void addRowkeyField(RowkeyField f) {
		rowkeyFields.add(f);
	}

	public void setRowkeySeparator(String rowkeySeparator) {
		this.rowkeySeparator = rowkeySeparator;
	}

	public boolean isHashed() {
		if (rowkeyFields.size() == 0) {
			return false;
		} else {
			return rowkeyFields.get(0).isHashed();
		}
	}

	public int getHashSizeBytes() {
		if (isHashed()) {
			RowkeyField hashField = rowkeyFields.get(0);

			short bitStart = hashField.getBitStart();
			short bitEnd = hashField.getBitEnd();

			int sizeBytes = ((bitEnd - bitStart) / 8) + 1;
			return sizeBytes;
		}
		return 0;
	}

	/**
	 * This method generates the HBase row key taking into account the actual
	 * row key fields for which value has been provided in condition clause
	 * 
	 * @param query
	 *            - SqlBean object which stores information about input query
	 * @return byte array of generated row key
	 * @throws HQLException
	 *             if hash size specified in row key is more that 1 byte
	 */
	public byte[] getHbaseRowkey(SqlBean query) throws HQLException {

		logger.debug("Generating row key");
		byte[] delim = null;
		if (rowkeySeparator != null) {
			delim = Bytes.toBytes(rowkeySeparator);
		}
		List<byte[]> rowkeyComponents = new ArrayList<>();

		for (RowkeyField rkf : rowkeyFields) {

			byte[] value;

			if (rkf.isLiteral()) {
				value = Bytes.toBytes(rkf.getLiteralValue());

			} else {
				String rowKey = rkf.getName();

				// check if we have processed all the required row key
				// components
				if (!query.getRowkeyFieldsUsed().contains(rkf.getName()))
					// this field(rkf) is not used in the row-key. No need to
					// record this and can break out of loop
					break;

				// get the value assigned to this row key field as part of SQL
				// conditions clause
				String assignedValue = query.getAssignedValue(rowKey);
				if (assignedValue == null) {
					// assignedValue is not for = operator
					break;
				}

				if (rkf.isHashed()) {
					try {
						value = rkf.getHashValue(assignedValue);
					} catch (Exception e) {
						throw new HQLException(e.getMessage());
					}
				} else
					value = Bytes.toBytes(assignedValue);
			}
			rowkeyComponents.add(value);
		}

		byte[] hbaseRowkey = null;

		if (delim != null) {
			hbaseRowkey = mergeByteArrays(rowkeyComponents, delim);
		} else {
			hbaseRowkey = mergeByteArrays(rowkeyComponents, Bytes.toBytes(""));
		}

		// In case no row key fields are specified in the input query, we scan
		// all rows
		if (null == hbaseRowkey) {
			hbaseRowkey = Bytes.toBytes("");
		}
		return hbaseRowkey;
	}

	/**
	 * This method merges input row key field list into a single byte array
	 * using delimiter
	 * 
	 * @param baList
	 *            - list of row key field values
	 * @param delim
	 *            - separator between row key fields
	 * @return merged byte array
	 */
	public byte[] mergeByteArrays(List<byte[]> baList, byte[] delim) {
		boolean firstItem = true;
		byte[] mergedArray = null;

		for (byte[] ba : baList) {
			if (firstItem) {
				mergedArray = ba;
				firstItem = false;
			} else
				mergedArray = Bytes.add(mergedArray, delim, ba);
		}

		return mergedArray;
	}

	public boolean isColumnPresent(String col) {
		for (RowkeyField rkf : rowkeyFields)
			if (rkf.isColumnPresent(col))
				return true;

		return false;
	}

	/**
	 * This method returns a list of row key component names
	 * 
	 * @return list of all row key field names
	 */
	public List<String> getRowkeyFieldNames() {
		ArrayList<String> allRowkeyNames = new ArrayList<>();
		for (RowkeyField rkf : rowkeyFields) {
			allRowkeyNames.add(rkf.getName());
		}
		return allRowkeyNames;
	}

	/**
	 * This method returns a list of non hash and non literal row key component names
	 * 
	 * @return list of non hash and non literal row key component names
	 */
	public List<String> getNonHashRowkeyFieldNames() {
		ArrayList<String> allRowkeyNames = new ArrayList<>();
		for (RowkeyField rkf : rowkeyFields) {
			if(!rkf.isHashed() && !rkf.isLiteral()){
				allRowkeyNames.add(rkf.getName());
			}
		}
		return allRowkeyNames;
	}
	
	public List<RowkeyField> getRowkeyColumns() {
		List<RowkeyField> copy = new ArrayList<>();
		copy.addAll(rowkeyFields);
		return copy;
	}

	/**
	 * Checks if the input values are a valid instance of the row key
	 * 
	 * @param row
	 *            key component values for this row key instance
	 * @return true if input array is valid, false otherwise
	 */
	public boolean isAValidRowKey(String[] values) {

		// Check if the number of row key components match
		if (rowkeyFields.size() != values.length) {
			return false;
		}

		// check if the literal fields, if any, match
		int pos = 0;
		for (RowkeyField rkf : rowkeyFields) {
			if (rkf.isLiteral()) {
				if (!values[pos].equals(rkf.getLiteralValue())) {
					return false;
				}
			}
			pos++;
		}
		return true;
	}

	public boolean isRowkeyFieldHashed(String rowkeyField) {
		for(RowkeyField rowkeyField2 : rowkeyFields) {
			if(rowkeyField2.getName().equals(rowkeyField) && rowkeyField2.isHashed()) {
				return true;
			}
		}
		return false;
	}

	public boolean isRowkeyFieldLiteral(String rowkeyField) {
		for(RowkeyField rowkeyField2 : rowkeyFields) {
			if(rowkeyField2.getName().equals(rowkeyField) && rowkeyField2.isLiteral()) {
				return true;
			}
		}
		return false;
	}

	public String getRowkeyFieldLiteralValue(String rowkeyField) {
		for(RowkeyField rowkeyField2 : rowkeyFields) {
			if(rowkeyField2.getName().equals(rowkeyField) && rowkeyField2.isLiteral()) {
				return rowkeyField2.getLiteralValue();
			}
		}
		return null;
	}

}
