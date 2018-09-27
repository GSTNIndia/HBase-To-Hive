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

import static org.gstn.schemaexplorer.target.Constants.PARENT_PATH_SEPARATOR;

import org.antlr.v4.runtime.misc.NotNull;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.exception.SchemaValidationException;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarBaseListener;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.ColumnContext;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.ColumnListContext;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.ColumnsCSVContext;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.DynamicPartMetadataContext;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.DynamicPartNameContext;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.HbaseTableContext;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.HbaseTableNameContext;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.RowFieldContext;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.RowKeyMetaDataContext;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser.SchemaNameContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HBaseTableListener extends HBaseTableGrammarBaseListener {

	HBaseTableIR hBaseIR;
	Logger logger;
	String currHbaseTable, currHbaseSchema;

	public HBaseTableListener(HBaseTableIR hBaseIR) {
		super();

		this.hBaseIR = hBaseIR;
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
		currHbaseTable = null;
		currHbaseSchema = null;
	}

	@Override
	public void exitRowKeyMetaData(@NotNull RowKeyMetaDataContext ctx) {
		super.exitRowKeyMetaData(ctx);
		if (ctx.rowKeySeparator() != null) {
			hBaseIR.setRowKeySeparator(currHbaseSchema, ctx.rowKeySeparator().getText());
			logger.debug("setting rowkey separator as " + ctx.rowKeySeparator().getText());
		}
	}

	@Override
	public void exitRowField(@NotNull RowFieldContext ctx) throws SchemaValidationException {
		super.exitRowField(ctx);

		logger.debug("exiting after parsing row field: " + ctx.getText());

		if (null == ctx.hashedField()) {
			// not a hashed field
			if (null == ctx.literal()) {
				// non-literal, non-hashed field
				hBaseIR.addRowkeyField(currHbaseSchema, new RowkeyField(ctx.getText(), "", false));
			} else {
				// the field is a literal
				hBaseIR.addRowkeyField(currHbaseSchema, new RowkeyField(ctx.literal().STRICTSTRING().getText(),
						ctx.literal().string().getText(), true));
			}
		} else {
			if (ctx.bitPrefix() == null) {
				logger.error("Bit positions need to be specified for hashed row key field");
				throw new SchemaValidationException("Bit positions need to be specified for hashed row key field");
			} else {
				String nm = ctx.hashedField().STRICTSTRING().getText();
				logger.debug("exitRowField(): Found a hash() field with name-" + nm);
				short start, end;
				start = Short.parseShort(ctx.bitPrefix().NUMBER(0).getText());
				end = Short.parseShort(ctx.bitPrefix().NUMBER(1).getText());

				if (start >= end) {
					logger.error("start index for field " + nm + " must be less than equal to the end index.");
					throw new SchemaValidationException(
							"start index for field " + nm + " must be less than equal to the end index.");
				}
				hBaseIR.addRowkeyField(currHbaseSchema, new RowkeyField(nm, start, end));
			}
		}
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
		parseColumnCsv(parentPath, csvCtx);

	}

	private void parseColumnCsv(String parentPath, ColumnsCSVContext csvCtx) throws SchemaValidationException {
		for (int i = 0; i < csvCtx.getChildCount(); i++) {
			ColumnContext columnCtx = csvCtx.getChild(ColumnContext.class, i);
			if (columnCtx != null) {
				ParseColumn(parentPath, columnCtx);
			}
		}
	}

	public void ParseColumn(String parentPath, ColumnContext ctx) throws SchemaValidationException {

		String cf = ctx.columnFamily().getText();
		String dataType, dataFormat, compType, defaultValue;
		boolean isJsonField = false;

		try {
			if (null != ctx.staticColumn()) {

				// Static column
				String columnName = "";
				// check for json field
				if (ctx.staticColumn().jsonField() != null) {
					columnName = ctx.staticColumn().jsonField().getText();
					columnName = columnName.substring(5, columnName.length());
					isJsonField = true;
				} else {
					columnName = ctx.staticColumn().STRICTSTRING().getText();
				}

				if (ctx.columnDataType().dataType() == null) {
					dataType = "string";
				} else {
					dataType = ctx.columnDataType().dataType().getText().toLowerCase();
				}

				if (ctx.columnDataType().format() != null) {
					if (!dataType.equalsIgnoreCase("date")) {
						throw new InvalidSchemaException("Data format: " + ctx.columnDataType().format()
								+ " is only supported for datatype: date");
					}
					dataFormat = ctx.columnDataType().format().getText();
				} else if (dataType.equalsIgnoreCase("date")) {
					dataFormat = "dd-MM-yyyy";
				} else {
					dataFormat = null;
				}

				if (!ctx.columnDefaultValue().getText().equals("")) {
					defaultValue = ctx.columnDefaultValue().getText();
					defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
				} else
					defaultValue = null;

				if (ctx.compressionType().getText().equals("")) {
					compType = "";
				} else {
					compType = ctx.compressionType().getText().toLowerCase();
				}

				logger.debug("exiting from column - " + ctx.getText() + " col family: " + cf + " col name: "
						+ columnName + " defaul value: " + defaultValue);
				if (parentPath != null) {
					columnName = parentPath + PARENT_PATH_SEPARATOR + columnName;
				}
				hBaseIR.addColumn(currHbaseSchema, parentPath,
						new HBaseColumn(cf, columnName, dataType, dataFormat, defaultValue, compType, isJsonField));

			} else if (null != ctx.dynamicColumn()) {
				// Dynamic column
				String dynamicPrefix, dynamicSuffix, dynamicComponent;
				dynamicPrefix = ctx.dynamicColumn().nullableString(0).getText();
				dynamicComponent = "<" + ctx.dynamicColumn().STRICTSTRING().getText() + ">";
				dynamicSuffix = ctx.dynamicColumn().nullableString(1).getText();

				String columnName = dynamicPrefix + dynamicComponent + dynamicSuffix;

				if (ctx.columnDataType().dataType() == null) {
					dataType = "string";
				} else {
					dataType = ctx.columnDataType().dataType().getText().toLowerCase();
					if (dataType.equalsIgnoreCase("json")) {
						isJsonField = true;
					}
				}

				if (ctx.columnDataType().format() != null) {
					dataFormat = ctx.columnDataType().format().getText();
				} else if (dataType.equalsIgnoreCase("date")) {
					dataFormat = "dd-MM-yyyy";
				} else
					dataFormat = null;

				if (!ctx.columnDefaultValue().getText().equals("")) {
					defaultValue = ctx.columnDefaultValue().getText();
					defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
				} else
					defaultValue = null;

				if (ctx.compressionType().getText().equals("")) {
					compType = "";
				} else {
					compType = ctx.compressionType().getText().toLowerCase();
				}

				logger.debug("exiting from column - " + ctx.getText() + " col family: " + cf + " col name: "
						+ columnName + " defaul value: " + defaultValue);

				hBaseIR.addColumn(currHbaseSchema, parentPath, new HBaseColumn(cf, dynamicPrefix, dynamicSuffix,
						dynamicComponent, dataType, dataFormat, defaultValue, compType, isJsonField));

			} else {
				// Control must not come here
				logger.error("Internal error: Unknown column type when parsing - " + ctx.getText());
			}
		} catch (InvalidSchemaException e) {
			System.err.println("HBase Schema parse error: " + e.getMessage());
			throw new SchemaValidationException(e.getMessage());
		}
	}

	@Override
	public void exitHbaseTableName(@NotNull HbaseTableNameContext ctx) {
		super.exitHbaseTableName(ctx);
		currHbaseTable = ctx.getText();
		hBaseIR.addTablename(currHbaseSchema, currHbaseTable);
		logger.debug("setting table name = " + currHbaseTable + " for schema=" + currHbaseSchema);
	}

	@Override
	public void exitHbaseTable(@NotNull HbaseTableContext ctx) throws SchemaValidationException {
		super.exitHbaseTable(ctx);
		String ddl = ctx.getText();
		try {
			hBaseIR.addDdl(currHbaseSchema, ddl);
		} catch (InvalidSchemaException e) {
			System.err.println("HBase Schema parse error: " + e.getMessage());
			throw new SchemaValidationException(e.getMessage());
		}
	}

	@Override
	public void exitSchemaName(@NotNull SchemaNameContext ctx) {
		super.exitSchemaName(ctx);
		currHbaseSchema = ctx.getText();
		logger.debug("setting schema name = " + currHbaseSchema);
	}

	@Override
	public void exitDynamicPartMetadata(@NotNull DynamicPartMetadataContext ctx) {
		super.exitDynamicPartMetadata(ctx);
		if (ctx.dynamicPartSeparator() != null) {
			hBaseIR.setDynamicPartSeparator(currHbaseSchema, ctx.dynamicPartSeparator().getText());
			logger.debug("setting dynamic part seperator as " + ctx.dynamicPartSeparator().getText());
		}
	}

	@Override
	public void enterDynamicPartName(@NotNull DynamicPartNameContext ctx) {
		super.enterDynamicPartName(ctx);
		if (null != ctx.getText()) {
			hBaseIR.addDynamicPartName(currHbaseSchema, ctx.STRICTSTRING().getText());
			logger.debug("added dynamic part name " + ctx.STRICTSTRING().getText());
		}
	}
}
