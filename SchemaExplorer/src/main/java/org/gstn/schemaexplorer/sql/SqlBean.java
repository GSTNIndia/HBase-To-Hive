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

import static org.gstn.schemaexplorer.sql.ConditionTreeManager.dyanmicColumnPattern;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.exception.ColumnNotFoundException;
import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseColumn;
import org.gstn.schemaexplorer.hbase.HBaseTableIR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;;

/**
 * This class stores all the information related to input query
 *
 */
@SuppressWarnings("serial")
public class SqlBean implements Serializable {

	private String schemaName;

	// It will be List Of (columnFamily,column)
	// This stores the first version of columns to be selected
	// By parsing the HQL query input by the user
	private List<List<String>> columnList;

	// Final list of columns to be selected.
	// Obtained by taking columnList as input
	// expand * to individual column names
	// expand dynamic columns to specific column names
	// stored in format Set of (columnFamily, columnName, DataType)
	// WE KNOW THE COLUMN NAMES BEFORE RUNNING HBASE SCAN
	private Set<List<String>> selectedColumns;

	// Final list of columns to be selected.
	// Obtained by taking columnList as input
	// expand * to individual column names
	// expand dynamic columns to specific column names
	// stored in format Set of (columnFamily, columnName, DataType)
	// WE ONLY KNOW THE COLUMN PATTERN. WE WILL KNOW THE COLUMN NAME ONLY
	// *AFTER* DOING THE SCAN
	private Set<List<String>> selectedColumnPatterns;

	// This stores the first version of conditions to be applied
	// By parsing the HQL query input by the user
	private List<Condition> conditionList;

	// List of schema's row key fields that are actually used
	// In case of prefix scan, this list will be a subset of all of schema's row
	// key components
	// literal fields do not have a field name and are assumed to be included in
	// row key by default
	// (unless there is a non specified row key field that comes to the left of
	// the literal in field order)
	private List<String> rowkeyFieldsUsed;

	// Added for HbaseToHive...will not be used in wideexplorer
	// This stores row key fields that are specified in sql but can not be
	// included in prefix,along with their specifird value
	// example: rowkey= stin|ptin|fp|invoice-type|inum if in sql we specify
	// stin,ptin,invoice-type then stin,ptin can be included in prefix so will
	// be part of rowkey_fields_used list, but invoice-type cann't be included
	// in prefix as fp is not provided. so invoice-type will be part of
	// rowkey_fields_used_non_prefix list
	private Map<String, String> rowkeyFieldsUsedNonPrefix;

	// Added for HbaseToHive
	// this stores the entire tree of conditions along with operators
	private ConditionTree conditionTree;

	private ConditionTree conditionTreeCopy;

	private Logger logger;

	private QueryType queryType;

	private CategorisedColumns categorisedColumns = new CategorisedColumns();

	public SqlBean() {
		schemaName = "";
		columnList = new ArrayList<>();
		conditionList = new ArrayList<>();
		selectedColumns = new HashSet<>();
		selectedColumnPatterns = new HashSet<>();
		conditionTree = new ConditionTree();
		conditionTreeCopy = new ConditionTree();
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public List<List<String>> getColumnList() {
		return columnList;
	}

	public List<Condition> getConditionList() {
		return conditionList;
	}

	public List<String> getRowkeyFieldsUsed() {
		return rowkeyFieldsUsed;
	}

	public QueryType getQueryType() {
		return queryType;
	}

	public ConditionTree getConditionTreeCopy() {
		return conditionTree.getDeepCopy();
	}

	public void setConditionTree(ConditionTree conditionTree) {
		this.conditionTree = conditionTree;
	}

	public Map<String, String> getRowkeyFieldsUsedNonPrefix() {
		return rowkeyFieldsUsedNonPrefix;
	}

	public Set<List<String>> getColumnsForSelection() {
		return selectedColumns;
	}

	public Set<List<String>> getSelectedColumnPatterns() {
		return selectedColumnPatterns;
	}

	public List<Condition> getConditonsFromConditionTree() {
		return conditionTree.getOnlyConditions();
	}

	// check if a column has been provided value in the WHERE clause
	public boolean columnUsedInConditions(String columnName) {
		return conditionList.contains(columnName);
	}

	/**
	 * This method returns the value assigned to input row key component in the
	 * query condition, only if equals operator is specified
	 * 
	 * @param rowKeyField
	 *            - row key component
	 * @return value assigned to input row key component in the query condition,
	 *         null if the operator is not "="
	 * @throws HQLException
	 *             if row key field is invalid
	 */
	public String getAssignedValue(String rowKeyField) throws HQLException {
		for (Condition condition : conditionList) {
			if (condition.getColumnName().equals(rowKeyField)) {
				if (condition.getConditionalOperator().equals("=")) {
					return condition.getValue();
				} else {
					return null;
				}
			}
		}
		throw new HQLException("Value not assigned for column " + rowKeyField);
	}

	/**
	 * This method checks if the schema has been defined in schema file, for the
	 * schema name associated with the query
	 * 
	 * @param hBaseIR
	 *            - object which stores information related to all the schemas
	 *            defined in schema file
	 * @return true if schema associated with the query is defined in schema
	 *         file
	 * @throws InvalidSchemaException
	 *             if schema associated with the query is not defined in schema
	 *             file
	 */
	public boolean validateSchemaName(HBaseTableIR hBaseIR) throws InvalidSchemaException {
		String schema = getSchemaName();

		if (!hBaseIR.getSchemaNames().contains(schema)) {
			throw new InvalidSchemaException("Invalid schema name.");
		}
		return true;
	}

	/**
	 * This method validates all the conditions in the query
	 * 
	 * @param hBaseIR
	 *            - object which stores information related to all the schemas
	 *            defined in schema file
	 * @param whereClauseOptional
	 *            - flag to specify if conditions are optional
	 * @return true if all the conditions are valid
	 * @throws HQLException
	 *             if a condition is invalid
	 */
	public boolean validateColumnList(HBaseTableIR hBaseIR, boolean whereClauseOptional) throws HQLException {
		String schema = getSchemaName();

		// First check if we have at least one condition specified
		if (conditionList.size() == 0 && !whereClauseOptional)
			throw new HQLException(
					"Please specify at least one condition in WHERE clause (values for row key fields, perhaps?).");

		/*
		 * Check if the conditional operators on the columns in WHERE clause are
		 * appropriate e.g. inequalities must be used only on numeric data types
		 */
		for (Condition cond : conditionList) {
			cond.validateCondition(schema, hBaseIR);
		}
		return true;
	}

	/*
	 * replace the dynamic components of a field with values as provided in the
	 * conditions Assumption - (1) columnName is a dynamic column (2) All
	 * dynamic components are supplied value in the WHERE clause
	 */

	/**
	 * This method replaces the dynamic components of a column with values as
	 * provided in the conditions, if input column is a dynamic column and all
	 * dynamic components are supplied value in the WHERE clause of query
	 * 
	 * @param columnFamily
	 *            - column family
	 * @param columnName
	 *            - column name
	 * @return
	 */
	private String instantiateDynamicColumn(String columnFamily, String columnName) {
		String instanceOfDynamicColumn = columnName;

		for (Condition condition : conditionList) {
			if (condition.getColumnFamily().equals(columnFamily)) {

				// check if this condition is applicable for the input dynamic
				// column
				int startPos = instanceOfDynamicColumn.indexOf("<" + condition.getColumnName() + ">");

				while (startPos != -1) {
					int endPos = startPos + ("<" + condition.getColumnName() + ">").length();
					instanceOfDynamicColumn = instanceOfDynamicColumn.substring(0, startPos) + condition.getValue()
							+ instanceOfDynamicColumn.substring(endPos);

					// search again to consider dynamic columns like
					// <invno>_status_<invno>_<date>
					startPos = instanceOfDynamicColumn.indexOf("<" + condition.getColumnName() + ">");
				}
			}
		}
		return instanceOfDynamicColumn;
	}

	/*
	 * Internally record that the column has to be selected from the HBase
	 * schema. if it is a dynamic column, use the value from conditions to
	 * instantiate it.
	 */
	/**
	 * This method adds columns to be selected from HBase table, if it is
	 * dynamic column it modifies it with instantiateDynamicColumn method
	 * 
	 * @param hBaseIR
	 *            - object which stores information related to all the schemas
	 *            defined in schema file
	 * @param columnFamily
	 *            - column family
	 * @param columnName
	 *            - column name
	 * @param columnDataType
	 *            - data type
	 * @throws HQLException
	 *             if column details are invalid
	 */
	private void addColumnForSelection(HBaseTableIR hBaseIR, String columnFamily, String columnName,
			String columnDataType) throws HQLException {

		Pattern p = Pattern.compile("^.*<.*>.*$");

		try {
			if (hBaseIR.isDynamicColumn(getSchemaName(), columnFamily, columnName))
				columnName = instantiateDynamicColumn(columnFamily, columnName);
		} catch (InvalidSchemaException e) {
			throw new HQLException(e.getMessage());
		}

		Matcher m = p.matcher(columnName);

		// check if the column name still has dynamic component after
		// instantiating the values
		if (m.matches()) {
			// if yes, it needs to be a column name filter
			String modifiedColumnPattern = "^" + columnName.replaceAll("<.*>", ".*") + "$";
			logger.debug("addColumnForSelection: Identified a column for regex filter: " + modifiedColumnPattern);

			ArrayList<String> selectedColumnPattern = new ArrayList<>();
			selectedColumnPattern.add(columnFamily);
			selectedColumnPattern.add(modifiedColumnPattern);
			selectedColumnPattern.add(columnDataType);
			selectedColumnPatterns.add(selectedColumnPattern);
		} else {
			List<String> columnDetails = new ArrayList<>();
			columnDetails.add(columnFamily);
			columnDetails.add(columnName);
			columnDetails.add(columnDataType);

			logger.debug("addColumnForSelection: column for selection - " + columnFamily + ":" + columnName);
			selectedColumns.add(columnDetails);
		}
	}

	/**
	 * This method checks if the input column family and column name combination
	 * is already added for selection
	 * 
	 * @param columnFamily
	 *            - column family
	 * @param columnName
	 *            - column name
	 * @return true if column is already added for selection, false otherwise
	 */
	public boolean isSelectedColumn(String columnFamily, String columnName) {
		// first check if this column was explicitly provided in the Select
		// clause of the query
		for (List<String> columnDetails : selectedColumns)
			if (columnFamily.equals(columnDetails.get(0)) && columnName.equals(columnDetails.get(1))) {
				logger.debug("is_selected_column: returning true");
				return true;
			}

		// If we are here, it is possible that this column name was provided as
		// a regex pattern in the input query
		// So we check the column name against the selectedColumnPatterns and
		// see if we can find a match

		for (List<String> columnDetails : selectedColumnPatterns) {
			Pattern columnFamilyPattern = Pattern.compile(columnDetails.get(0));
			Pattern columnNamePattern = Pattern.compile(columnDetails.get(1));

			if (columnFamilyPattern.matcher(columnFamily).matches()
					&& columnNamePattern.matcher(columnName).matches()) {
				logger.debug("is_selected_column: returing true from pattern");
				return true;
			}
		}

		// If we are here, the column is not a selected column
		return false;
	}

	/**
	 * This method returns data type of input column family and column name
	 * combination, as a string
	 * 
	 * @param columnFamily
	 *            - column family
	 * @param columnName
	 *            - column name
	 * @return data type of the column as a string
	 * @throws HQLException
	 *             if input column family and column name combination is invalid
	 */
	public String getSelectedColumnDataType(String columnFamily, String columnName) throws HQLException {

		logger.debug("getSelectedColumnDataType: selectedColumns=" + selectedColumns.toString());

		// first check if this column was explicitly provided in the Select
		// clause of the query
		for (List<String> columnDetails : selectedColumns)
			if (columnFamily.equals(columnDetails.get(0)) && columnName.equals(columnDetails.get(1)))
				return columnDetails.get(2);

		// If we are here, it is possible that this column name was provided as
		// a regex pattern in the input query
		// So we check the column name against the selectedColumnPatterns and
		// see if we can find a match

		for (List<String> columnDetails : selectedColumnPatterns) {
			Pattern columnFamilyPattern = Pattern.compile(columnDetails.get(0));
			Pattern columnNamePattern = Pattern.compile(columnDetails.get(1));

			if (columnFamilyPattern.matcher(columnFamily).matches() && columnNamePattern.matcher(columnName).matches())
				return columnDetails.get(2);
		}

		// If we could still not get a data type, we just throw up our hands
		throw new HQLException("Internal error: Unable to get the data type for - " + columnFamily + ":" + columnName);
	}

	/*
	 * Identify and record all the columns that need to be selected from the
	 * HBase schema and sent as result. We will be expanding '*' and dynamic
	 * column names as part of this process
	 */
	/**
	 * This method generates all the columns that need to be selected from the
	 * HBase schema
	 * 
	 * @param hBaseIR
	 *            - object which stores information related to all the schemas
	 *            defined in schema file
	 * @throws HQLException
	 *             if schema associated with columns is invalid
	 */
	public void generateColumnsForSelection(HBaseTableIR hBaseIR) throws HQLException {
		logger.debug("Generating columns for selection");

		String columnFamily, columnName;

		for (List<String> columnDetails : getColumnList()) {
			columnFamily = columnDetails.get(0);
			columnName = columnDetails.get(1);

			if (columnFamily.isEmpty())
				columnFamily = ".*";
			else
				columnFamily = columnFamily.replaceAll("\\*", ".*");

			if (columnName.isEmpty())
				columnName = ".*";
			else
				columnName = columnName.replaceAll("\\*", ".*");

			// column name for selection (as provided in HQL query)
			// is considered as a *regex pattern*. The columns from schema are
			// matched against this pattern
			// All matched columns are then considered for selection

			Set<List<String>> columnsMatchingPattern = hBaseIR.getAllColumnNames(getSchemaName(), columnFamily,
					columnName);

			// Does the input column match (in select clause) match at least one
			// column in the schema
			if (columnsMatchingPattern.size() > 0)
				for (List<String> columnData : columnsMatchingPattern) {
					// Expansion of dynamic columns is done in a inner function
					// call of below function
					try {
						addColumnForSelection(hBaseIR, columnData.get(0), columnData.get(1),
								hBaseIR.getColumnDataType(getSchemaName(), columnData.get(0), columnData.get(1)));
					} catch (InvalidSchemaException e) {
						throw new HQLException(e.getMessage());
					}
				}
			else if (hBaseIR.isColumnPresentInRowkey(getSchemaName(), columnName)) {
				logger.debug(
						"generate_columns_for_selection: Row key component identified for selection - " + columnName);
				continue;
			} else
				// we have been provided an invalid column in the select clause
				throw new HQLException("No column match found for " + columnFamily + ":" + columnName);
		}
	}

	/*
	 * Analyze the columns selected and conditions to identify the query type.
	 * Query types are defined in Enum QueryType
	 */
	/**
	 * This method analyzes the columns selected and conditions to identify, if
	 * the conditions all row key fields has been specified or not
	 * 
	 * @param hBaseIR
	 *            - object which stores information related to all the schemas
	 *            defined in schema file
	 * @throws HQLException
	 *             if schema associated with columns is invalid
	 */
	public void identifyQueryType(HBaseTableIR hBaseIR) throws HQLException {
		// we can the row key fields that are specified values in the conditions
		// clause.
		// the first row key field that is not provided value in the conditions
		// is recorded below
		// if all row key fields are set value in conditions, then the flag will
		// stay false

		boolean foundNonSpecifiedRowkeyField = false;
		String nonSpecifiedRowkeyField = null;

		rowkeyFieldsUsed = new ArrayList<>();

		try {
			for (String rowkeyField : hBaseIR.getRowkeyFieldNames(getSchemaName())) {
				if (columnUsedInConditions(rowkeyField)) {
					if (foundNonSpecifiedRowkeyField) {
						throw new HQLException("Missing value for row key field " + nonSpecifiedRowkeyField);
					} else {
						rowkeyFieldsUsed.add(rowkeyField);
					}
				} else {
					foundNonSpecifiedRowkeyField = true;
					nonSpecifiedRowkeyField = rowkeyField;
				}
			}
		} catch (InvalidSchemaException e) {
			throw new HQLException(e.getMessage());
		}
		if (foundNonSpecifiedRowkeyField) {
			queryType = QueryType.ROWKEY_PREFIX_SPECIFIED;
		} else {
			queryType = QueryType.FULL_ROWKEY_SPECIFIED;
		}
		logger.debug("Identified query type = " + queryType.toString());
	}

	// identify_query_type implementation for HbaseToHive.
	// The difference is it doesn't return HQLException, if it found a non
	// specified rowkey field after a specified row key field
	public void identifyQueryTypeHbaseToHive(HBaseTableIR hBaseIR) throws HQLException {
		// we can the row key fields that are specified values in the conditions
		// clause with '=' operator.
		// the first row key field that is not provided value in the conditions
		// is recorded below
		// if all row key fields are set value in conditions, then the flag will
		// stay false

		boolean foundNonSpecifiedRowkeyField = false;
		/* String non_specified_rowkey_field=null; */

		rowkeyFieldsUsed = new ArrayList<>();
		rowkeyFieldsUsedNonPrefix = new HashMap<String, String>();
		String assignedValue;
		try {
			for (String rowkeyField : hBaseIR.getRowkeyFieldNames(getSchemaName())) {
				if (columnUsedInConditions(rowkeyField) && (assignedValue = getAssignedValue(rowkeyField)) != null) {
					if (foundNonSpecifiedRowkeyField) {
						rowkeyFieldsUsedNonPrefix.put(rowkeyField, assignedValue);
					} else {
						rowkeyFieldsUsed.add(rowkeyField);
					}
				} else {
					foundNonSpecifiedRowkeyField = true;
				}
			}
		} catch (InvalidSchemaException e) {
			throw new HQLException(e.getMessage());
		}
		if (foundNonSpecifiedRowkeyField) {
			queryType = QueryType.ROWKEY_PREFIX_SPECIFIED;
		} else {
			queryType = QueryType.FULL_ROWKEY_SPECIFIED;
		}

		logger.debug("rowkey_fields_used = " + rowkeyFieldsUsed);
		logger.debug("rowkey_fields_used_non_prefix = " + rowkeyFieldsUsedNonPrefix);
		logger.debug("Identified query type = " + queryType.toString());
	}

	// Iterate over ConditionsTree and mark row key and dynamic part conditions
	/**
	 * This method iterates over ConditionsTree and marks row key and dynamic
	 * part conditions
	 * 
	 * @param hBaseIR
	 *            - object which stores information related to all the schemas
	 *            defined in schema file
	 */
	public void markRowKeyAndDynamicPartConditionsInTree(HBaseTableIR hBaseIR) {
		markRowKeyAndDynamicPartConditionsInTree(hBaseIR, conditionTree);
	}

	/**
	 * This method iterates over ConditionsTree and marks row key and dynamic
	 * part conditions
	 * 
	 * @param hBaseIR
	 *            - object which stores information related to all the schemas
	 *            defined in schema file
	 * @param conditionTree
	 *            - collection of all the conditions
	 */
	private void markRowKeyAndDynamicPartConditionsInTree(HBaseTableIR hBaseIR, ConditionTree conditionTree) {
		List<Condition> conditionList = conditionTree.getConditions();
		for (Condition condition : conditionList) {
			if (condition instanceof ConditionTree) {
				markRowKeyAndDynamicPartConditionsInTree(hBaseIR, (ConditionTree) condition);
			} else {
				checkAndMarkConditionAsRowKeyOrDyanmicPart(hBaseIR, condition);
			}
		}
	}

	/**
	 * This method checks if the condition is for a row key field or dynamic
	 * part component
	 * 
	 * @param hBaseIR
	 *            - object which stores information related to all the schemas
	 *            defined in schema file
	 * @param condition
	 *            - condition in the query
	 */
	private void checkAndMarkConditionAsRowKeyOrDyanmicPart(HBaseTableIR hBaseIR, Condition condition) {
		String columnName = condition.getColumnName();
		boolean result = hBaseIR.isColumnPresentInRowkey(schemaName, columnName);

		if (result) {
			condition.setRowKeyCondition(result);
		} else {
			// check if condition is based on dynamic part
			result = hBaseIR.isColumnPresentInDynamicParts(schemaName, columnName);

			condition.setDynamicPartCondition(result);
		}
	}

	public CategorisedColumns getCategorisedColumns() {
		return categorisedColumns;
	}

	public boolean isDynamicColumnsInSelect() {
		if (!getCategorisedColumns().getSelectedDynamicColumns().isEmpty())
			return true;
		else
			return false;
	}

	public boolean isStaticColumnsInSelect() {
		if (!getCategorisedColumns().getSelectedStaticColumns().isEmpty())
			return true;
		else
			return false;
	}

	public boolean isStaticColumnsInWhere() {
		if (!getCategorisedColumns().getStaticColumnsInWhereNotInSelect().isEmpty())
			return true;
		else
			return false;
	}

	public Boolean evaluateStaticConditions(DataRecord dataRecord) throws Exception {
		// get a copy of ConditionTree to apply the static conditions and store
		// result of evaluation in the copy
		conditionTreeCopy = this.conditionTree.getDeepCopy();

		// apply static column and row key conditions from query
		return ConditionTreeManager.evaluateStaticColumnAndRowKeyConditions(conditionTreeCopy, dataRecord);
	}

	public boolean evaluateDynamicConditions(DataRecord dataRecord) throws Exception {
		// get a copy of ConditionTree to apply the dynamic conditions and store
		// result of evaluation in the copy
		ConditionTree conditionTreeForDynamic = conditionTreeCopy.getDeepCopy();

		// apply dynamic column and dynamic part conditions from query
		return ConditionTreeManager.evaluateDyanmicColumnConditions(conditionTreeForDynamic, dataRecord);
	}

	@Override
	public String toString() {
		return "Sql_Bean [schemaName=" + schemaName + ", columnList=" + columnList + ", conditionList=" + conditionList
				+ "]";
	}

	public class CategorisedColumns implements Serializable {
		
		private Map<String, Map<String, HBaseColumn>> selectedStaticColumns = new HashMap<>();
		private Map<String, Map<String, HBaseColumn>> selectedDynamicColumns = new HashMap<>();;
		private Map<String, Map<String, HBaseColumn>> staticColumnsInWhereNotInSelect = new HashMap<>();;
		private Map<String, Map<String, HBaseColumn>> dynamicColumnsInWhereNotInSelect = new HashMap<>();;
		private Map<String, Map<String, HBaseColumn>> allRequiredColumns = new HashMap<>();;

		boolean initialized = false;

		public void initialize(List<HBaseColumn> allColumns) throws ColumnNotFoundException {
			if (!initialized) {
				initializeSelectedColumnsFromSchema(allColumns);
				initializeColumnsInWhereClauseNotInSelect(allColumns);
				initialized = true;
			}
		}

		public void initializeSelectedColumnsFromSchema(List<HBaseColumn> allColumns) {

			for (HBaseColumn hbaseColumn : allColumns) {

				if (isSelectedColumn(hbaseColumn.getColumnFamily(), hbaseColumn.getColumnName())) {
					Map<String, Map<String, HBaseColumn>> mapForInsertion;
					if (hbaseColumn.isDynamicColumn()) {
						mapForInsertion = selectedDynamicColumns;
					} else {
						mapForInsertion = selectedStaticColumns;
					}
					addIntoMap(mapForInsertion, hbaseColumn);
					addIntoMap(allRequiredColumns, hbaseColumn);
				}
			}
		}

		public void initializeColumnsInWhereClauseNotInSelect(List<HBaseColumn> allColumns)
				throws ColumnNotFoundException {

			for (Condition condition : getConditonsFromConditionTree()) {
				if (!condition.isRowKeyCondition() && !condition.isDynamicPartCondition()) {
					// column condition: static or dynamic
					String cn = condition.getColumnName();
					String cf = condition.getColumnFamily();

					Matcher matcher = dyanmicColumnPattern.matcher(cn);
					boolean dynamic = false;
					if (matcher.find()) {
						cn = matcher.replaceFirst("");
						dynamic = true;
					}

					HBaseColumn hbaseColumn = getHbaseColumnEntity(allColumns, cf, cn, condition.getColumnName(),
							dynamic);

					if (!isPresentInselectedColumns(dynamic, cf, cn)) {
						Map<String, Map<String, HBaseColumn>> mapForInsertion;
						if (dynamic) {
							mapForInsertion = dynamicColumnsInWhereNotInSelect;
						} else {
							mapForInsertion = staticColumnsInWhereNotInSelect;
						}
						addIntoMap(mapForInsertion, hbaseColumn);
						addIntoMap(allRequiredColumns, hbaseColumn);
					}
				}
			}
		}

		private HBaseColumn getHbaseColumnEntity(List<HBaseColumn> allColumns, String cf, String cn, String fullColName,
				boolean dynamic) throws ColumnNotFoundException {
			for (HBaseColumn hbaseColumn : allColumns) {
				if (dynamic == hbaseColumn.isDynamicColumn() && hbaseColumn.getColumnFamily().equals(cf)
						&& hbaseColumn.getColumnNameWithoutDynamicComponent().equals(cn)) {
					return hbaseColumn;
				}
			}

			throw new ColumnNotFoundException("Column specified in condition having family " + cf + " and column name "
					+ fullColName + " not found in schema.");
		}

		private void addIntoMap(Map<String, Map<String, HBaseColumn>> mapForInsertion, HBaseColumn hbaseColumn) {
			String cf = hbaseColumn.getColumnFamily();
			String cn = hbaseColumn.getColumnNameWithoutDynamicComponent();

			Map<String, HBaseColumn> map = mapForInsertion.get(cf);
			if (map == null) {
				map = new HashMap<>();
				mapForInsertion.put(cf, map);
			}
			map.put(cn, hbaseColumn);
		}

		private boolean isPresentInselectedColumns(boolean dynamic, String cf, String cn) {
			Map<String, Map<String, HBaseColumn>> mapForSearch;
			if (dynamic) {
				mapForSearch = selectedDynamicColumns;
			} else {
				mapForSearch = selectedStaticColumns;
			}

			Map<String, HBaseColumn> cols = mapForSearch.get(cf);

			if (cols != null && cols.containsKey(cn)) {
				return true;
			}

			return false;
		}

		public Map<String, Map<String, HBaseColumn>> getSelectedStaticColumns() {
			return selectedStaticColumns;
		}

		public Map<String, Map<String, HBaseColumn>> getSelectedDynamicColumns() {
			return selectedDynamicColumns;
		}

		public Map<String, Map<String, HBaseColumn>> getStaticColumnsInWhereNotInSelect() {
			return staticColumnsInWhereNotInSelect;
		}

		public Map<String, Map<String, HBaseColumn>> getDynamicColumnsInWhereNotInSelect() {
			return dynamicColumnsInWhereNotInSelect;
		}

		public Map<String, Map<String, HBaseColumn>> getAllRequiredColumns() {
			return allRequiredColumns;
		}

	}

}
