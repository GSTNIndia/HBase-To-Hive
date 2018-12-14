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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseTableIR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles queries on source HBase table
 *
 */
@SuppressWarnings("serial")
public class HQLEngine implements Serializable {

	private final HBaseTableIR hBaseIR;
	private Logger logger;

	public HQLEngine(HBaseTableIR hBaseIR) {
		this.hBaseIR = hBaseIR;
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
		logger.info("Initializing HQL Engine");
	}

	/**
	 * This method validates the input query
	 * 
	 * @param query
	 *            - object which parses and stores information about query
	 * @param targetJsonFields 
	 * 			  - List of all target json column names
	 * @throws HQLException
	 *             if the query is invalid
	 */
	public void validateQueryForHbaseToHive(SqlBean query, Map<String,Class> targetJsonFieldsDataTypes) throws HQLException {
		try {
			query.validateSchemaName(hBaseIR);

			query.validateColumnList(hBaseIR, true, targetJsonFieldsDataTypes);

			// identify the type of SELECT query
			// it can be a select based on row key prefix
			// or it can be a select based on complete row key
			query.identifyQueryTypeHbaseToHive(hBaseIR);

			// identify complete list of specific HBase columns to be fetched
			query.generateColumnsForSelection(hBaseIR);

			query.markConditionTypesInTree(hBaseIR,targetJsonFieldsDataTypes);

		} catch (InvalidSchemaException e) {
			throw new HQLException(e.getMessage());
		} catch (HQLException e) {
			throw e;
		}
	}

	/**
	 * This method generates HBase scan object based on input query
	 * 
	 * @param query
	 *            - object which parses and stores information about query
	 * @return scan instance based on input query
	 * @throws HQLException
	 *             if the query is invalid
	 */
	public Scan getScanInstanceFromValidatedQuery(SqlBean query) throws HQLException {
		try {
			// Generates complete row key in case all row key components are
			// specified
			// In case only a prefix of components is specified, this generates
			// the prefix
			byte[] hbaseRowkey = hBaseIR.getHbaseRowkey(query.getSchemaName(), query);
			Scan hbaseScan = generateHbaseScan(hbaseRowkey, query);
			return hbaseScan;
		} catch (HQLException e) {
			System.err.println("Parse error: " + e.getMessage());
			throw e;
		} catch (Exception e) {
			String errMsg = "Exception when accessing schema " + query.getSchemaName();
			System.err.println(errMsg);
			throw e;
		}
	}

	/**
	 * This method generates HBase scan object based on input query for given
	 * row key
	 * 
	 * @param hbaseRowkey
	 *            - row key generated based on conditions in the input query
	 * @param query
	 *            - object which parses and stores information about query
	 * @return scan instance based on input query
	 * @throws HQLException
	 *             if the query is invalid
	 */
	private Scan generateHbaseScan(byte[] hbaseRowkey, SqlBean query) throws HQLException {
		logger.debug("Generating HBase scan");

		Scan hbaseScan = new Scan(hbaseRowkey);

		logger.debug("generateHbaseScan: HBase row key = " + Bytes.toString(hbaseRowkey));

		Filter rowFilter;

		// set up row key prefix filter
		if (query.getQueryType() == QueryType.ROWKEY_PREFIX_SPECIFIED) {
			rowFilter = new PrefixFilter(hbaseRowkey);
			logger.debug("generateHbaseScan: Setting Prefix filter" + hbaseScan.toString());
		} else if (query.getQueryType() == QueryType.FULL_ROWKEY_SPECIFIED) {
			rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(hbaseRowkey));
			logger.debug("generateHbaseScan: Setting Row filter" + hbaseScan.toString());
		} else {
			throw new HQLException(
					"Internal error in generateHbaseScan : Unknown query type - " + query.getQueryType().toString());
		}

		// set column filters
		List<Filter> columnFilters = getAllColumnFilters(query);
		FilterList allFilters;

		if (columnFilters.size() > 0) {
			logger.debug("generateHbaseScan: Adding row and column filters");
			allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL, rowFilter,
					new FilterList(FilterList.Operator.MUST_PASS_ONE, columnFilters));
		} else {
			// no columns are selected. Adding a key only filter
			logger.debug("generateHbaseScan: Adding a key only filter and first key only filter");
			allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL, rowFilter, new FirstKeyOnlyFilter(),
					new KeyOnlyFilter());
		}

		hbaseScan = hbaseScan.setFilter(allFilters);

		return hbaseScan;
	}

	/**
	 * This method generates a list of column filters based on conditions in the
	 * query
	 * 
	 * @param query
	 *            - object which parses and stores information about query
	 * @return list of filters
	 */
	private List<Filter> getAllColumnFilters(SqlBean query) {
		String columnName, columnNameRegex, columnFamily;
		QualifierFilter qualifierFilter;
		FamilyFilter familyFilter;

		List<Filter> allColumnFilters = new ArrayList<>();
		// Map to store columns added to filters
		Map<String, Set<String>> cfCnMap = new HashMap<>();

		// below are the columns to be considered - where the column name is
		// specified in full
		for (List<String> columnDetails : query.getColumnsForSelection()) {
			columnFamily = columnDetails.get(0);
			columnName = columnDetails.get(1);

			Set<String> cnNames = cfCnMap.getOrDefault(columnFamily, new HashSet<String>());
			cnNames.add(columnName);
			cfCnMap.put(columnFamily, cnNames);

			logger.debug("getAllColumnFilters: Creating a new filter for column(BinaryComparator): " + columnFamily
					+ ":" + columnName);

			qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(columnName)));
			familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(columnFamily)));
			allColumnFilters.add(new FilterList(FilterList.Operator.MUST_PASS_ALL, qualifierFilter, familyFilter));
		}

		// Below are column patterns to be considered - where the column name is
		// a regex
		for (List<String> columnDetails : query.getSelectedColumnPatterns()) {
			columnFamily = columnDetails.get(0);
			columnNameRegex = columnDetails.get(1);

			Set<String> cnNames = cfCnMap.getOrDefault(columnFamily, new HashSet<String>());
			cnNames.add(columnNameRegex);
			cfCnMap.put(columnFamily, cnNames);

			logger.debug("getAllColumnFilters: Creating a new filter for pattern(RegexStringComparator) : "
					+ columnFamily + ":" + columnNameRegex);

			qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
					new RegexStringComparator(columnNameRegex));
			familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(columnFamily)));
			allColumnFilters.add(new FilterList(FilterList.Operator.MUST_PASS_ALL, qualifierFilter, familyFilter));
		}

		// Adding the columns which are specified in the where clause, but are
		// not specified in select clause
		// these would be required while evaluating the where clause conditions
		for (Condition condition : query.getConditonsFromConditionTree()) {
			if (!condition.isRowKeyCondition()) {
				String cf = condition.getColumnFamily();
				String cn = condition.getColumnName();

				String modifiedCn = parseColumnNameForFilter(cn);

				// check if column filter is already added for this column
				if (cfCnMap.get(cf) == null || !cfCnMap.get(cf).contains(modifiedCn)) {
					qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
							new RegexStringComparator(modifiedCn));
					familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
							new BinaryComparator(Bytes.toBytes(cf)));
					allColumnFilters
							.add(new FilterList(FilterList.Operator.MUST_PASS_ALL, qualifierFilter, familyFilter));

					Set<String> cnNames = cfCnMap.getOrDefault(cf, new HashSet<String>());
					cnNames.add(modifiedCn);
					cfCnMap.put(cf, cnNames);
				}
			}
		}
		logger.debug("getAllColumnFilters: filters = " + allColumnFilters.toString());

		return allColumnFilters;
	}

	/**
	 * This method returns a regex if column is dynamic, else returns column
	 * name as it is
	 * 
	 * @param columnName
	 *            - name of the column
	 * @return a regex if column is dynamic, else returns column name as it is
	 */
	private String parseColumnNameForFilter(String columnName) {

		Pattern p = Pattern.compile("^.*<.*>.*$");
		Matcher m = p.matcher(columnName);

		// check if the column has dynamic component
		if (m.matches()) {
			String modifiedColumnPattern = "^" + columnName.replaceAll("<.*>", ".*") + "$";
			return modifiedColumnPattern;
		} else {
			return columnName;
		}
	}

}
