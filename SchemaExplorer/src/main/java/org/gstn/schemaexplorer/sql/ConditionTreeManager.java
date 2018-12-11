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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.entity.Tuple;
import org.gstn.schemaexplorer.sql.ConditionTree.Operator;
import org.gstn.schemaexplorer.util.DataTypeUtil;

/**
 * This class manages evaluation of condition tree
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ConditionTreeManager {

	// pattern to identify dynamic column condition based on <X> in column name
	public static Pattern dyanmicColumnPattern = Pattern.compile("<[^<>]+>");

	/**
	 * This method evaluates all the conditions on row key
	 * fields
	 * 
	 * @param conditionTree
	 *            - conditions to be evaluated
	 * @param outputDataRecord
	 *            - object that stores processed data
	 * @return true/false if result of entire condition tree can be determined
	 *         based on row key conditions, null otherwise
	 * @throws Exception
	 *             if conditions are invalid
	 */
	public static Boolean evaluateRowKeyConditions(ConditionTree conditionTree,
			DataRecord outputDataRecord) throws Exception {
		return evaluateTree(conditionTree, outputDataRecord, false, true);
	}
	
	/**
	 * This method evaluates all the conditions on static columns and row key
	 * fields
	 * 
	 * @param conditionTree
	 *            - conditions to be evaluated
	 * @param outputDataRecord
	 *            - object that stores processed data
	 * @return true/false if result of entire condition tree can be determined
	 *         based on static columns, null otherwise
	 * @throws Exception
	 *             if conditions are invalid
	 */
	public static Boolean evaluateStaticColumnAndRowKeyConditions(ConditionTree conditionTree,
			DataRecord outputDataRecord) throws Exception {
		return evaluateTree(conditionTree, outputDataRecord, true, false);
	}

	/**
	 * This method evaluates all the conditions on dynamic columns
	 * 
	 * @param conditionTree
	 *            - conditions to be evaluated
	 * @param outputDataRecord
	 *            - object that stores processed data
	 * @return true if all the condition tree is evaluated as true, false
	 *         otherwise
	 * @throws Exception
	 *             if conditions are invalid
	 */
	public static boolean evaluateDyanmicColumnConditions(ConditionTree conditionTree, DataRecord outputDataRecord)
			throws Exception {
		Boolean result = evaluateTree(conditionTree, outputDataRecord, false, false);
		if (result != null && result) {
			return true;
		} else {
			// if result is null, it means some column(s) specified in
			// condition was missing in data
			// returning false in this case
			return false;
		}
	}

	/**
	 * This method evaluates the condition tree against data that has been read
	 * from HBase table
	 * 
	 * @param conditionTree
	 *            - conditions to be evaluated
	 * @param outputDataRecord
	 *            - object that stores processed data
	 * @param applyStatic
	 *            - flag to specify if the conditions should be evaluated for
	 *            static or dynamic columns
	 * @return true/false/null depending on the flag and evaluation of condition
	 *         tree
	 * @throws Exception
	 */
	public static Boolean evaluateTree(ConditionTree conditionTree, DataRecord outputDataRecord, boolean applyStatic, boolean rowKeyOnlyFlag)
			throws Exception {
		if (!conditionTree.isEvaluated()) {
			Operator operator = conditionTree.getOperator();
			Boolean result = null;

			int noOfConditions = conditionTree.getConditions().size();
			if (noOfConditions == 0) {
				result = true;
			}

			for (int i = 0; i < noOfConditions; i++) {

				Condition condition = conditionTree.getConditions().get(i);

				if (condition instanceof ConditionTree) {
					result = evaluateTree((ConditionTree) condition, outputDataRecord, applyStatic, rowKeyOnlyFlag);
				} else {
					result = evaluateColumnCondition(condition, outputDataRecord, applyStatic, rowKeyOnlyFlag);
				}

				if (i < noOfConditions - 1 && result != null) {
					// check if it is required to evaluate remaining conditions,
					// based on current result and operator
					result = checkResult(result, operator);
				}

				if (result != null) {
					break;
				}
			}

			if (noOfConditions > 1) {

				boolean foundNull = false;
				Boolean finalResult = null;

				for (int i = 0; i < noOfConditions; i++) {
					Condition condition = conditionTree.getConditions().get(i);

					if (condition.getResult() == null) {
						foundNull = true;
					} else if (finalResult == null) {
						finalResult = checkResult(condition.getResult(), operator);
					}
				}

				if (foundNull && finalResult != null) {
					if (operator.equals(Operator.AND) && finalResult.equals(false)) {
						finalResult = false;
					} else if (operator.equals(Operator.OR) && finalResult.equals(true)) {
						finalResult = true;
					}

				}
				if (foundNull)
					result = finalResult;
			}

			// assign result if not null
			if (result != null) {
				conditionTree.setEvaluated(true);
				conditionTree.setResult(result);
			}
			return result;

		} else {
			return conditionTree.getResult();
		}
	}

	/**
	 * This method checks if the current result is conclusive, based on the
	 * operator
	 * 
	 * @param result
	 *            - current result
	 * @param operator
	 *            - operator between currently evaluated condition and next
	 *            condition
	 * @return true/false if result is conclusive, null other wise
	 */
	private static Boolean checkResult(Boolean result, Operator operator) {
		Boolean finalResult = null;
		if (result != null) {
			if (operator.equals(Operator.AND) && result.equals(false)) {
				finalResult = false;
			} else if (operator.equals(Operator.OR) && result.equals(true)) {
				finalResult = true;
			}
		}
		return finalResult;
	}

	/**
	 * This method evaluates a single condition, against its corresponding
	 * column from input data record object
	 * 
	 * @param condition
	 *            - condition to be evaluated
	 * @param outputDataRecord
	 *            - object that stores processed data
	 * @param staticFlag
	 *            - flag to specify if the conditions should be evaluated for
	 *            static or dynamic columns
	 * @return true if condition is evaluated as true, false otherwise
	 * @throws Exception
	 *             if condition is invalid
	 */
	private static Boolean evaluateColumnCondition(Condition condition, DataRecord outputDataRecord, boolean staticFlag, boolean rowKeyOnlyFlag)
			throws Exception {
		Boolean result = null;

		if (!condition.isEvaluated()) {

			String cnFromCondition = condition.getColumnName();

			if (condition.isRowKeyCondition()) {
				Tuple rowKeyField = outputDataRecord.getRowkeyTuple(cnFromCondition);
				if (rowKeyField != null) {
					result = applyOperator(condition, rowKeyField);
				}
			} else if(!rowKeyOnlyFlag){
				// check if column is dynamic

				boolean isColumnStatic;
				if (condition.isDynamicPartCondition())
					isColumnStatic = false;
				else {
					Matcher matcher = dyanmicColumnPattern.matcher(cnFromCondition);

					if (matcher.find()) {
						isColumnStatic = false;
					} else {
						isColumnStatic = true;
					}

					if (!isColumnStatic) {
						// removing <X> from dynamic column name from condition
						cnFromCondition = matcher.replaceFirst("");
					}
				}

				if (staticFlag != isColumnStatic) {
					return null;
				}

				// evaluating the condition
				if (outputDataRecord.isColumnNamePresent(cnFromCondition)) {
					Tuple tuple = outputDataRecord.getColumnTuple(cnFromCondition);
					result = applyOperator(condition, tuple);
				}

			}else{
				//want to evaluate only rowkey conditions, but this is not a row key condition
				return null;
			}

			if (result == null) {
				// result is null at this stage means we did not find some
				// column mentioned in conditions, in the cfMap or in
				// rowKeyColumns
				result = applyOperator(condition, null);
			}

			// mark condition as evaluated and store result
			condition.setEvaluated(true);
			condition.setResult(result);

			return result;
		} else {
			return condition.getResult();
		}

	}

	private static boolean applyOperator(Condition condition, Tuple tuple) throws Exception {

		if (condition.getValue().equalsIgnoreCase("null")) {
			// conditions is based on null
			return handleNullChecks(condition, tuple);
		} else if (tuple == null) {
			// we did not find column mentioned within condition in the data and
			// condition is not null check related
			return false;
		} else {
			try {
				
				if(condition.isPatternCondition()){
					
					String operator = condition.getConditionalOperator();
					
					String valueFromTuple = tuple.getColumnValue();
					
					boolean matchResult = condition.getPattern().matcher(valueFromTuple).matches();
					
					if(operator.equalsIgnoreCase("NOT REGEXP")){
						return !matchResult;
					}else{
						return matchResult;
					}
					
				}else{
				
					Class<?> dataType = tuple.getColumnDataType();
					String operator = condition.getConditionalOperator();
	
					Comparable<?> valueFromCondition = null;
					Comparable<Comparable> valueFromTuple = null;
	
					try {
						valueFromCondition = DataTypeUtil.parseValueToType(condition.getValue(), dataType);
					} catch (NumberFormatException e) {
						System.err
								.println("Exception in applyOperator while parsing condition value: " + condition.getValue()
										+ " for column: " + condition.getColumnName() + " into data type: " + dataType);
						throw e;
					}
	
					try {
						valueFromTuple = DataTypeUtil.parseValueToType(tuple.getColumnValue(), dataType);
					} catch (NumberFormatException e) {
						System.err.println("Exception in applyOperator while parsing tuple value: " + tuple.getColumnValue()
								+ " for column: " + tuple.getColumnName() + " into data type: " + dataType);
						throw e;
					}
	
					// valueFromTuple should be compared with valueFromCondition and
					// not vice versa, as the operator and values from conditions
					// are to be compared with valueFromCondition
					int compareResult = valueFromTuple.compareTo(valueFromCondition);
	
					return interpretCompareResult(compareResult, operator);
				}
			} catch (Exception e) {
				System.err.println("Error during applyOperator: " + e.getMessage());
				throw e;
			}

		}
	}

	/**
	 * This method handles conditions which have value as null, like column !=
	 * null
	 * 
	 * @param condition
	 *            - condition to be evaluated
	 * @param tuple
	 *            - tuple of the column to be evaluated
	 * @return true if condition is evaluated as true, false otherwise
	 */
	private static boolean handleNullChecks(Condition condition, Tuple tuple) {
		if (condition.getConditionalOperator().equals("=")) {
			if (tuple == null || tuple.getColumnValue() == null || tuple.getColumnValue().equals("null"))
				return true;
			else
				return false;
		} else {
			if (tuple != null && tuple.getColumnValue() != null && !tuple.getColumnValue().equals("null"))
				return true;
			else
				return false;
		}
	}

	/**
	 * This method evaluates the result of compareTo method, that was used in
	 * calling method
	 * 
	 * @param compareResult
	 *            - result of compareTo method
	 * @param operator
	 *            - conditional operator
	 * @return true if condition is evaluated as true, false otherwise
	 */
	private static boolean interpretCompareResult(int compareResult, String operator) {
		if (operator.equals("="))
			return test(0, compareResult);
		else if (operator.equals("<"))
			return test(-1, compareResult);
		else if (operator.equals(">"))
			return test(1, compareResult);
		else if (operator.equals("<="))
			return test(0, compareResult) || test(-1, compareResult);
		else if (operator.equals(">="))
			return test(0, compareResult) || test(1, compareResult);
		else {
			// not equals i.e. != , <>
			return test(-1, compareResult) || test(1, compareResult);
		}
	}

	/**
	 * This method checks how the inputs compare to each other
	 * 
	 * @param expected
	 *            - expected value
	 * @param actual
	 *            - actual value
	 * @return true if actual and expected values are similar, false otherwise
	 */
	private static boolean test(int expected, int actual) {

		if (expected == 0) {
			if (actual == 0) {
				return true;
			} else {
				return false;
			}
		} else if (expected == -1) {
			if (actual < 0) {
				return true;
			} else {
				return false;
			}
		} else {
			if (actual > 0) {
				return true;
			} else {
				return false;
			}
		}
	}

}
