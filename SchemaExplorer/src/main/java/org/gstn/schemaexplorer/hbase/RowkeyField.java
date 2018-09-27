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

import java.io.Serializable;

import org.apache.hadoop.hbase.util.MurmurHash;
import org.slf4j.Logger;

/**
 * This class stores information about individual fields of the row key like
 * field name, if the field is hashed etc
 *
 */
@SuppressWarnings("serial")
public class RowkeyField implements Serializable {
	private String name;
	private boolean hashRequired;
	private boolean literal;
	private String literalValue;
	private short bitStart;
	private short bitEnd;
	Logger logger;

	/**
	 * Constructor for hashed row key field
	 * 
	 * @param nm
	 *            - name of the row key field
	 * @param start
	 *            - start bit of hash value
	 * @param end
	 *            - end bit of hash value
	 */
	public RowkeyField(String nm, short start, short end) {
		logger = org.slf4j.LoggerFactory.getLogger(this.getClass().getCanonicalName());
		logger.debug("RowKeyField(): adding new row key field with name = " + nm + " start=" + Short.toString(start)
				+ " end=" + Short.toString(end));

		name = nm;
		hashRequired = true;
		literal = false;
		bitStart = start;
		bitEnd = end;
	}

	/**
	 * Constructor for non hashed row key field
	 * 
	 * @param nm
	 *            - name of the row key field
	 * @param value
	 *            - literal value if specified
	 * @param isLiteral
	 *            - flag to mark if row key field is literal
	 */
	public RowkeyField(String nm, String value, boolean isLiteral) {
		logger = org.slf4j.LoggerFactory.getLogger(this.getClass().getCanonicalName());

		hashRequired = false;
		if (isLiteral) {
			literal = true;
			literalValue = value;
			name = nm;
			logger.debug("RowKeyField(): adding new row key field with literal value = " + literalValue);
		} else {
			literal = false;
			name = nm;
			bitStart = 0;
			bitEnd = -1;
			logger.debug("RowKeyField(): adding new row key field with name = " + name);
		}
	}

	public boolean isLiteral() {
		return literal;
	}

	public boolean isHashed() {
		return hashRequired;
	}

	public String getName() {
		return name;
	}

	public String getLiteralValue() {
		return literalValue;
	}

	/**
	 * This method calculates murmur hash value for a given input string
	 * 
	 * @param value
	 *            - string whose hash value is to be calculated
	 * @return hash value as byte array
	 * @throws Exception
	 *             if byte size is more than 1
	 */
	public byte[] getHashValue(String value) throws Exception {
		// The current hash implementation only returns one byte hash so it
		// won't support multiple byte hash as per the old logic commented
		// below.
		byte hashByte = hash(value, (bitEnd - bitStart + 1));
		byte[] hashedValue = new byte[1];
		hashedValue[0] = hashByte;

		/*
		 * Integer hash = MurmurHash.getInstance().hash(value.getBytes());
		 * String binHash= String.format("%32s",
		 * Integer.toBinaryString(hash)).replace(" ", "0"); byte[] hashedValue =
		 * new byte[(bitEnd-bitStart)/8+1]; int counter =0; for (int i =
		 * bitStart; i <= bitEnd ; ) { String byteOfHashString; if(bitEnd-i >
		 * 7){ byteOfHashString = binHash.substring(i,i+8); i=i+8; } else{
		 * byteOfHashString = binHash.substring(i,bitEnd+1); i=i+bitEnd+1; } int
		 * byteOfHashInt = Integer.parseInt(byteOfHashString, 2); byte
		 * byteOfHash = (byte)byteOfHashInt;
		 * 
		 * hashedValue[counter++]=byteOfHash; }
		 */

		logger.debug("getHashValue: returned value = " + hashedValue.toString());
		return hashedValue;
	}

	public static byte hash(String value) throws Exception {
		return hash(value, 7);
	}

	public static byte hash(String value, int noOfBits) throws Exception {
		if (noOfBits < 1 || noOfBits > 8) {
			// can throw custom exception
			throw new Exception("noOfBits supported by current hash implementation is only 8");
		}
		int mask = ((int) Math.pow(2, noOfBits)) - 1;
		int maskOutput = MurmurHash.getInstance().hash(value.getBytes()) & mask;

		byte firstByte = (byte) (maskOutput << (8 - noOfBits));

		return firstByte;
	}

	public boolean isColumnPresent(String col) {
		if (literal || hashRequired)
			return false;

		return name.equals(col);
	}

	public short getBitStart() {
		return bitStart;
	}

	public short getBitEnd() {
		return bitEnd;
	}

}
