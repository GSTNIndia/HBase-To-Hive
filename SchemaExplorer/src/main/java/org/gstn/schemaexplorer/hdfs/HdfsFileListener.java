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
package org.gstn.schemaexplorer.hdfs;

import static org.gstn.schemaexplorer.target.Constants.PARENT_PATH_SEPARATOR;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.exception.SchemaValidationException;
import org.gstn.schemaexplorer.hdfsgrammar.HdfsGrammarBaseListener;
import org.gstn.schemaexplorer.hdfsgrammar.HdfsGrammarParser;
import org.gstn.schemaexplorer.hdfsgrammar.HdfsGrammarParser.ColumnContext;
import org.gstn.schemaexplorer.hdfsgrammar.HdfsGrammarParser.ColumnListContext;
import org.gstn.schemaexplorer.hdfsgrammar.HdfsGrammarParser.ColumnsCSVContext;
import org.gstn.schemaexplorer.hdfsgrammar.HdfsGrammarParser.HdfsFileContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a listener class to populate the HdfsFileIR object by traversing
 * rules in the grammar.
 */
public class HdfsFileListener extends HdfsGrammarBaseListener implements ParseTreeListener {
	HdfsFileIR hdfsSchema;
	Logger logger;
	String currHdfsSchema;

	public HdfsFileListener(HdfsFileIR schema) {
		super();
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
		hdfsSchema = schema;
	}

	@Override
	public void exitHdfsFile(HdfsFileContext ctx) throws SchemaValidationException {
		super.exitHdfsFile(ctx);
		String ddl = ctx.getText();
		try {
			hdfsSchema.addDdl(currHdfsSchema, ddl);
		} catch (InvalidSchemaException e) {
			System.err.println("HDFS Schema parse error: " + e.getMessage());
			throw new SchemaValidationException(e.getMessage());
		}
	}

	@Override
	public void exitSchemaName(@NotNull HdfsGrammarParser.SchemaNameContext ctx) {
		super.exitSchemaName(ctx);
		currHdfsSchema = ctx.STRICTSTRING().getText();
		logger.debug("setting schema name = " + currHdfsSchema);
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

	private void ParseColumn(String parentPath, ColumnContext ctx) throws SchemaValidationException {

		String columnName = null;
		String dataType = ctx.columnDataType().getText();
		String defaultValue = ctx.columnDefaultValue().getText();

		if (dataType.equals("")) {
			dataType = "string";
		}

		if (defaultValue != "") {
			defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
		} else {
			defaultValue = "NULL";
		}

		if (ctx.columnName().STRICTSTRING() != null) {
			columnName = ctx.columnName().STRICTSTRING().getText();
			if (parentPath != null) {
				logger.error("Parent path specified for " + columnName
						+ ". It should only be specified for json attributes");
				throw new SchemaValidationException("Parent path specified for " + columnName
						+ ". It should only be specified for json attributes");
			}
			hdfsSchema.addColumn(currHdfsSchema, parentPath, new HdfsColumn(columnName, dataType, defaultValue, false));
		} else {
			columnName = ctx.columnName().jsonField().getText();
			columnName = columnName.substring(5, columnName.length());
			if (parentPath != null) {
				columnName = parentPath + PARENT_PATH_SEPARATOR + columnName;
			}
			hdfsSchema.addColumn(currHdfsSchema, parentPath, new HdfsColumn(columnName, dataType, defaultValue, true));
		}

		logger.debug("exiting from column - col name : " + columnName + ", col data type : " + dataType
				+ ", col default value " + defaultValue + " for schema " + currHdfsSchema);
	}
}
