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
package org.gstn.schemaexplorer.hive;

import static org.gstn.schemaexplorer.target.Constants.PARENT_PATH_SEPARATOR;

import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.exception.SchemaValidationException;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarBaseListener;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarParser.ColumnContext;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarParser.ColumnListContext;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarParser.ColumnsCSVContext;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarParser.DatabaseNameContext;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarParser.HiveTableContext;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarParser.OptionalStorageContext;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarParser.PartitionListContext;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarParser.SchemaNameContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableListener extends HiveGrammarBaseListener implements ParseTreeListener {
	HiveTableIR hiveSchema;
	Logger logger;
	String currHiveSchema;

	public HiveTableListener(HiveTableIR schema) {
		super();
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
		hiveSchema = schema;
	}

	@Override
	public void exitHiveTable(@NotNull HiveTableContext ctx) throws SchemaValidationException {
		super.exitHiveTable(ctx);
		String ddl = ctx.getText();
		try {
			hiveSchema.addDdl(currHiveSchema, ddl);
		} catch (InvalidSchemaException e) {
			System.err.println("Hive Schema parse error: " + e.getMessage());
			throw new SchemaValidationException(e.getMessage());
		}
	}

	@Override
	public void exitSchemaName(@NotNull SchemaNameContext ctx) {
		super.exitSchemaName(ctx);
		currHiveSchema = ctx.STRICTSTRING().getText();
		logger.debug("setting schema name = " + currHiveSchema);
	}

	@Override
	public void exitDatabaseName(@NotNull DatabaseNameContext ctx) {
		super.exitDatabaseName(ctx);
		String database = ctx.STRICTSTRING().getText();
		hiveSchema.setDatabaseName(currHiveSchema, database);
		logger.debug("setting database name = " + database);
	}

	@Override
	public void exitOptionalStorage(@NotNull OptionalStorageContext ctx) throws SchemaValidationException {
		super.exitOptionalStorage(ctx);
		String storage = "PARQUET";
		List<String> formats = Arrays.asList("TEXTFILE", "SEQUENCEFILE", "ORC", "PARQUET", "AVRO", "RCFILE");
		if (ctx.STRICTSTRING() != null) {
			storage = ctx.STRICTSTRING().getText().toUpperCase();
			if (!formats.contains(storage)) {
				throw new SchemaValidationException(
						"Storage format " + storage + " is not supported. Try " + formats.toString());
			}
		}
		hiveSchema.setStorage(currHiveSchema, storage);
		logger.debug("setting storage = " + storage);
	}

	@Override
	public void exitColumnList(@NotNull ColumnListContext ctx) throws SchemaValidationException {
		super.exitColumnList(ctx);
		String parentPath;
		if (ctx.parentPath() != null) {
			parentPath = ctx.parentPath().getText();
		} else {
			parentPath = null;
		}

		ColumnsCSVContext csvCtx = ctx.columnsCSV();
		parseColumnCsv(parentPath, csvCtx, false);

	}

	@Override
	public void exitPartitionList(@NotNull PartitionListContext ctx) throws SchemaValidationException {
		super.exitPartitionList(ctx);
		String parentPath;
		if (ctx.parentPath() != null) {
			parentPath = ctx.parentPath().getText();
		} else {
			parentPath = null;
		}
		ColumnsCSVContext csvCtx = ctx.columnsCSV();
		parseColumnCsv(parentPath, csvCtx, true);

	}

	private void parseColumnCsv(String parentPath, ColumnsCSVContext csvCtx, boolean partition)
			throws SchemaValidationException {
		for (int i = 0; i < csvCtx.getChildCount(); i++) {
			ColumnContext columnCtx = csvCtx.getChild(ColumnContext.class, i);
			if (columnCtx != null) {
				ParseColumn(parentPath, columnCtx, partition);
			}
		}
	}

	private void ParseColumn(String parentPath, ColumnContext ctx, boolean partition) throws SchemaValidationException {

		String columnName = null;
		String dataType = ctx.columnDataType().getText();
		String defaultValue = ctx.columnDefaultValue().getText();

		if (dataType.equals("")) {
			dataType = "string";
		}

		if (!defaultValue.equals("")) {
			defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
		} else {
			defaultValue = "\\N";
		}

		if (ctx.columnName().STRICTSTRING() != null) {
			columnName = ctx.columnName().STRICTSTRING().getText();
			if (parentPath != null) {
				logger.error("Parent path specified for " + columnName
						+ ". It should only be specified for json attributes");
				throw new SchemaValidationException("Parent path specified for " + columnName
						+ ". It should only be specified for json attributes");
			}
			hiveSchema.addColumn(currHiveSchema, parentPath,
					new HiveColumn(columnName, dataType, defaultValue, false, partition));
		} else {
			columnName = ctx.columnName().jsonField().getText();
			columnName = columnName.substring(5, columnName.length());
			if (parentPath != null) {
				columnName = parentPath + PARENT_PATH_SEPARATOR + columnName;
			}
			hiveSchema.addColumn(currHiveSchema, parentPath,
					new HiveColumn(columnName, dataType, defaultValue, true, partition));
		}

		logger.debug("exiting from column - col name : " + columnName + ", col data type : " + dataType
				+ ", col default value " + defaultValue + " for schema " + currHiveSchema);

	}

}
