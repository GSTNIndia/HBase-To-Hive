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
package org.gstn.schemaexplorer.sql;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseTableIR;

/**
 * Class to store information about a condition to be applied on source HBase
 * table
 */
@SuppressWarnings("serial")
public class Condition implements Serializable {

	private String columnFamily;
	private String columnName;
	private String conditionalOperator;
	private String value;
	// Indicates whether this condition is evaluated
	private boolean evaluated = false;
	// Stores the result of evaluation
	private Boolean result = null;
	// Indicates whether this condition is rowKeyCondition
	private boolean rowKeyCondition = false;
	// Indicates whether this condition is based on dynamic part
	private boolean dynamicPartCondition = false;
	// Indicates whether this condition is pattern based (i.e. REGEXP or NOT REGEXP)
	private Boolean patternCondition = null;
	//stores the compiled regex pattern if the condition is patternCondition
	private Pattern pattern = null;
	
	
	public Condition() {
		columnFamily = "";
		columnName = "";
		conditionalOperator = "";
		value = "";
		evaluated = false;
		result = null;
	}

	public Condition(Condition source) {
		columnFamily = source.getColumnFamily();
		columnName = source.getColumnName();
		conditionalOperator = source.getConditionalOperator();
		value = source.getValue();
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getConditionalOperator() {
		return conditionalOperator;
	}

	public void setConditionalOperator(String operator) {
		this.conditionalOperator = operator;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * This method checks if the condition is valid
	 * 
	 * @param schema
	 *            - schema against which condition is to be validated
	 * @param hBaseIR
	 *            - object which stores information about all schemas
	 * @return true if condition is valid, false otherwise
	 * @throws HQLException
	 * @throws InvalidSchemaException
	 */
	public boolean validateCondition(String schema, HBaseTableIR hBaseIR) throws HQLException {

		// column needs to be present in data or in rowkey or as a dynamic
		// component
		if (!hBaseIR.isColumnPresentInData(schema, columnFamily, columnName)
				&& !hBaseIR.isColumnPresentInRowkey(schema, columnName)
				&& !hBaseIR.isColumnPresentInDynamicParts(schema, columnName)) {
			throw new HQLException(columnFamily + "." + columnName
					+ " cannot be used in condition, since it is not present in table schema."
					+ " (Did you forget the column family?)");
		}

		// check if the condition operator is appropriate
		if (conditionalOperator.equals("<") || conditionalOperator.equals(">") || conditionalOperator.equals("<=")
				|| conditionalOperator.equals(">=")) {
			try {
				if (hBaseIR.getColumnDataType(schema, columnFamily, columnName).equals("string")) {
					throw new HQLException(
							"Numeric comparison not allowed on string column " + columnFamily + "." + columnName);
				}
			} catch (InvalidSchemaException e) {
				throw new HQLException(e.getMessage());
			}
		}
		return true;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	@Override
	public String toString() {
		return "Condition [columnFamily=" + columnFamily + ", column=" + columnName + ", operator="
				+ conditionalOperator + ", value=" + value + ", evaluated=" + evaluated + ", result=" + result
				+ ", rowKeyCondition=" + rowKeyCondition + "]";
	}

	public boolean isEvaluated() {
		return evaluated;
	}

	public void setEvaluated(boolean evaluated) {
		this.evaluated = evaluated;
	}

	public Boolean getResult() {
		return result;
	}

	public void setResult(Boolean result) {
		this.result = result;
	}

	public boolean isRowKeyCondition() {
		return rowKeyCondition;
	}

	public void setRowKeyCondition(boolean rowKeyCondition) {
		this.rowKeyCondition = rowKeyCondition;
	}

	/**
	 * This method return a copy of the condition
	 * 
	 * @return a copy of the condition
	 */
	public Condition getDeepCopy() {
		// We won't be modyfying these so we can pass reference no need to
		// create new object
		Condition conditionCopy = new Condition();
		conditionCopy.columnName = columnName;
		conditionCopy.columnFamily = columnFamily;
		conditionCopy.conditionalOperator = conditionalOperator;
		conditionCopy.value = value;

		// primitives
		conditionCopy.evaluated = evaluated;
		conditionCopy.rowKeyCondition = rowKeyCondition;
		conditionCopy.dynamicPartCondition = dynamicPartCondition;

		// we modify this so need to create new object
		if (result != null)
			conditionCopy.result = new Boolean(result);

		return conditionCopy;
	}

	public boolean isDynamicPartCondition() {
		return dynamicPartCondition;
	}
	
	public boolean isPatternCondition() {
		if(patternCondition==null){
			patternCondition=Arrays.asList("REGEXP","NOT REGEXP").contains(conditionalOperator);
		}
		
		return patternCondition;
	}
	
	public Pattern getPattern(){
		if(pattern==null){
			pattern = Pattern.compile(value);
		}
		return pattern;
		
	}
	
	public void setDynamicPartCondition(boolean dynamicPartCondition) {
		this.dynamicPartCondition = dynamicPartCondition;
	}
}
