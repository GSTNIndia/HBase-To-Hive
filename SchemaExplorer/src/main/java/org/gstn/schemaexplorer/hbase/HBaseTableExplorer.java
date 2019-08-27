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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.gstn.schemaexplorer.antlr4error.ThrowingErrorListener;
import org.gstn.schemaexplorer.exception.ColumnNotFoundException;
import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.HQLRunTimeException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.exception.SchemaFileException;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarLexer;
import org.gstn.schemaexplorer.hbasegrammar.HBaseTableGrammarParser;
import org.gstn.schemaexplorer.sql.HQLEngine;
import org.gstn.schemaexplorer.sql.SqlBean;
import org.gstn.schemaexplorer.sql.SqlListener;
import org.gstn.schemaexplorer.sqlgrammar.sqlLexer;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;

/**
 * This class processes HBase schema file and stores that information in various
 * composite classes.
 *
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class HBaseTableExplorer implements Serializable {

	private Logger logger;
	private HBaseTableIR hBaseIR;
	private HQLEngine engine;

	/**
	 * Constructor of the class. Reads and process schema file
	 * 
	 * @param schemaPath
	 *            - path of the HBase schema file
	 */
	public HBaseTableExplorer(String schemaPath) {
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
		logger.debug("Launching shell");

		refreshSchema(schemaPath);

		// showSplash();
	}

	/**
	 * This method reads the schema file and parses it
	 * 
	 * @param schemaPath
	 *            - path of the HBase schema file
	 */
	private void refreshSchema(String schemaPath) {
		try {
			logger.info("Reading hbase schema");

			InputStream is = null;
			try {
				if (schemaPath.contains("hdfs://")) {
					Configuration configuration = new Configuration();
					FileSystem fileSystem = FileSystem.get(configuration);
					Path path = new Path(schemaPath);
					is = fileSystem.open(path);
				} else {
					is = new FileInputStream(schemaPath);
				}
			} catch (HQLRunTimeException e) {
				// to avoid throwing error to constructor
				logger.error("Error while reading schema from: " + schemaPath);
			}

			if (is != null) {
				readHbaseSchema(is);
				is.close();

			} else {
				hBaseIR = new HBaseTableIR();
			}
			engine = new HQLEngine(hBaseIR);
			logger.info("Initializing HQL engine");

		} catch (FileNotFoundException e) {
			logger.error("Error when trying to read schema file: " + e.getMessage());
			throw new SchemaFileException("Error when trying to read schema file: " + e.getMessage());
		} catch (IOException e) {
			logger.error("Exception when trying to read schema file: " + e.getMessage());
			throw new SchemaFileException("Exception when trying to read schema file: " + e.getMessage());
		}
	}

	public void showSplash() {
		Scanner sc = new Scanner(getClass().getClassLoader().getResourceAsStream("splashHQL"));
		while (sc.hasNextLine())
			System.out.println(sc.nextLine());

		sc.close();
	}

	public void launch() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("HBase Wide Table Explorer");
		System.out.println("=========================");
		System.out.println("Type 'help' for options ");

		boolean exit = false;

		while (!exit) {
			String command = getCommand(br);

			switch (command.toLowerCase()) {
			case "hi":
			case "hello":
				System.out.println("Hi !");
			case "help":
				printHelpOptions();
				break;
			case "list":
				listSchemas();
				break;
			case "bye":
				System.out.println("Good bye!");
			case "quit":
			case "exit":
				exit = true;
				break;
			default:
				processCommand(command);
			}
		}
		
		br.close();
	}

	private void listSchemas() {
		hBaseIR.getSchemaNames().forEach(tableName -> System.out.println(tableName));
	}

	private void printHelpOptions() {

		TreeMap<String, String> options = new TreeMap<>();

		options.put("help\t\t\t", "Display this message");
		options.put("list\t\t\t", "List available hbase tables");
		options.put("show schema <schema_name> ", "Display the table schema");
		options.put("exit\t\t\t", "Exit the utility");

		System.out.println("Command\t\t\tDescription");
		System.out.println("=======\t\t\t===========");

		options.forEach((key, value) -> System.out.println(key + value));
	}

	private String getCommand(BufferedReader br) {
		String command = "";
		System.out.print("\n>> ");
		
		try {
			command = br.readLine();
		} catch (IOException e) {
			System.err.println("Error getting user input" + e.getMessage());
			e.printStackTrace();
		}
		return command;
	}

	public void processCommand(String command) {

		if (command.startsWith("show schema")) {
			String[] tokens = command.split("[ ]+");
			if (tokens.length < 3) {
				System.err.println("Please specify schema name. e.g. show schema INVOICES");
			}
			String table = tokens[2];
			try {
				System.out.println(hBaseIR.getDdl(table));
			} catch (Exception e) {
				System.out.println(e.getMessage());
				System.out.println("Try 'list' command to get list of schemas.");
			}
		} else {
			System.out.println("Invalid Command. Use help to know all the supported commands");
		}
	}

	/**
	 * This function parses and validates the select query
	 * 
	 * @param queryString
	 *            - The select query
	 * @param targetJsonFields 
	 * 			  - List of all target json column names
	 * @return An object of class SqlBean, which stores information about the
	 *         query
	 * @throws IOException
	 * @throws HQLException
	 *             If columns and conditions used in the query are invalid
	 * @throws ColumnNotFoundException 
	 */
	public SqlBean parseAndGetValidatedQuery(String queryString, Map<String,Class> targetJsonFields) throws IOException, HQLException, ColumnNotFoundException {
		InputStream is = new ByteArrayInputStream(queryString.getBytes(StandardCharsets.UTF_8));
		try {
			SqlBean sqlBean = parseHql(is);
			engine.validateQueryForHbaseToHive(sqlBean,targetJsonFields);
			sqlBean.getCategorisedColumns().initialize(hBaseIR.getAllColumns(sqlBean.getSchemaName()));
			return sqlBean;
		} catch (IOException | HQLException e) {
			String errMsg = "Exception in parseAndGetValidatedQuery: " + e.getMessage();
			logger.error(errMsg);
			throw e;
		}
	}

	public Scan getScanInstanceFromValidatedQuery(SqlBean query) throws HQLRunTimeException, HQLException {

		try {
			return engine.getScanInstanceFromValidatedQuery(query);
		} catch (HQLRunTimeException | HQLException e) {
			String errMsg = "Exception in getScanInstanceFromValidatedQuery: " + e.getMessage();
			logger.error(errMsg);
			throw e;
		}

	}

	/**
	 * This method converts the input stream to ANTLR input stream and parses it
	 * 
	 * @param inputStream
	 *            - input stream to be parsed
	 * @throws IOException
	 */
	public void readHbaseSchema(InputStream inputStream) throws IOException {

		ANTLRInputStream input = new ANTLRInputStream(inputStream);

		try {
			hBaseIR = parse(input);
		} catch (HQLRunTimeException e) {
			logger.error("Error parsing schema: " + e.getMessage());
			throw e;
		} catch (ParseCancellationException e) {
			logger.error(e.getMessage());
			throw e;
		}
	}

	/**
	 * This method parses the input ANTLR stream
	 * 
	 * @param input
	 *            - ANTLR stream to be parsed
	 * @return An object of HBaseTableIR class which stores all the information
	 *         contained in schema file
	 */
	private HBaseTableIR parse(ANTLRInputStream input) {

		HBaseTableGrammarLexer lexer = new HBaseTableGrammarLexer(input);
		lexer.removeErrorListeners();
		lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

		CommonTokenStream tokens = new CommonTokenStream(lexer);

		HBaseTableGrammarParser parser = new HBaseTableGrammarParser(tokens);
		parser.removeErrorListeners();
		parser.addErrorListener(ThrowingErrorListener.INSTANCE);

		HBaseTableListener listener;
		HBaseTableIR hBaseIR = new HBaseTableIR();

		listener = new HBaseTableListener(hBaseIR);

		ParseTree tree = parser.input();

		ParseTreeWalker walker = new ParseTreeWalker();

		walker.walk(listener, tree);
		return hBaseIR;
	}

	public SqlBean parseHql(InputStream is) throws IOException {

		try {
			ANTLRInputStream input = new ANTLRInputStream(is);
			sqlLexer sl = new sqlLexer(input);
			sl.removeErrorListeners();
			sl.addErrorListener(ThrowingErrorListener.INSTANCE);
			// create a buffer of tokens pulled from the lexer
			CommonTokenStream tokens = new CommonTokenStream(sl);

			// create a parser that feeds off the tokens buffer
			sqlParser sp = new sqlParser(tokens);
			sp.removeErrorListeners();
			sp.addErrorListener(ThrowingErrorListener.INSTANCE);

			ParseTree tree = sp.selectStmt();

			SqlBean sqlBean = new SqlBean();
			SqlListener scl = new SqlListener(sqlBean);

			ParseTreeWalker walker = new ParseTreeWalker();

			// Walk the tree created during the parse, trigger callbacks
			walker.walk(scl, tree);

			return sqlBean;
		} catch (ParseCancellationException e) {
			logger.error("Error while trying to parse query: " + e.getMessage());
			throw new HQLRunTimeException(e.getMessage());
		}
	}

	public boolean isSchemaDefined(String targetSchema) {
		return hBaseIR.isSchemaDefined(targetSchema);
	}

	public Map<String, Map<String, Class>> getStaticColumns(String sourceSchema) {
		return hBaseIR.getStaticColumns(sourceSchema);
	}

	public Map<String, Map<String, Class>> getDynamicColumnPrefixes(String sourceSchema) {
		return hBaseIR.getDynamicColumnPrefixes(sourceSchema);
	}

	public Map<String, Map<String, Class>> getDynamicColumnSuffixes(String sourceSchema) {
		return hBaseIR.getDynamicColumnSuffixes(sourceSchema);
	}

	public String getDynamicColumnsSeparator(String sourceSchema) {
		return hBaseIR.getDynamicColumnsSeparator(sourceSchema);
	}

	public String getRowKeySeparator(String sourceSchema) {
		return hBaseIR.getRowKeySeparator(sourceSchema);
	}

	public List<String> getDynamicPartNames(String sourceSchema) {
		return hBaseIR.getDynamicPartNames(sourceSchema);
	}
	
	public List<String> getDynamicPartNamesExcludingSKIP(String sourceSchema) {
		List<String> allDynamicParts =  hBaseIR.getDynamicPartNames(sourceSchema);
		
		List<String> requiredDynamicParts = new ArrayList<>();
		for (String dynamicPart : allDynamicParts) {
			if(!dynamicPart.equalsIgnoreCase("SKIP")){
				requiredDynamicParts.add(dynamicPart);
			}
		}
		
		return requiredDynamicParts;
	}

	public List<String> getAllFieldNames(String targetSchema) {
		return hBaseIR.getAllFieldNames(targetSchema);
	}
	
	public Map<String,Class> getSchemaJsonColumns(String targetSchema) {
		return hBaseIR.getSchemaJsonColumns(targetSchema);
	}

	public boolean containsJsonColumn(String sourceSchema) {
		return hBaseIR.containsJsonColumn(sourceSchema);
	}

	public boolean isTallTable(String targetSchema) {
		return hBaseIR.isTallTable(targetSchema);
	}

	public String getTableName(String targetSchema) {
		return hBaseIR.getTableName(targetSchema);
	}

	public Set<HBaseColumn> getJsonColumnName(String sourceSchema, String columnFamilyPattern, String columnName) {
		return hBaseIR.getJsonColumnName(sourceSchema, columnFamilyPattern, columnName);
	}

	public Class<?> getColumnDataTypeClass(String targetTable, String cf, String cn) throws InvalidSchemaException {
		return hBaseIR.getColumnDataTypeClass(targetTable, cf, cn);
	}

	public String getColumnDataFormat(String sourceSchema, String columnName) {
		return hBaseIR.getColumnDataFormat(sourceSchema, columnName);
	}

	public boolean isRowKeyHashed(String sourceSchema) {
		return hBaseIR.isRowKeyHashed(sourceSchema);
	}

	public int getRowKeyHashSizeBytes(String sourceSchema) {
		return hBaseIR.getRowKeyHashSizeBytes(sourceSchema);
	}

	public List<String> getRowkeyFieldNames(String sourceSchema) throws InvalidSchemaException {
		return hBaseIR.getRowkeyFieldNames(sourceSchema);
	}
	
	public List<RowkeyField> getRowkeyFields(String sourceSchema) throws InvalidSchemaException {
		return hBaseIR.getRowkeyFields(sourceSchema);
	}

	public boolean isAValidRowKey(String sourceSchema, String[] rowKeyParts) {
		return hBaseIR.isAValidRowKey(sourceSchema, rowKeyParts);
	}

	public Set<List<String>> getAllColumnNames(String targetSchema, String columnFamilyPattern,
			String columnNamePattern) throws HQLException {
		return hBaseIR.getAllColumnNames(targetSchema, columnFamilyPattern, columnNamePattern);
	}

	public String getColumnDefaultValue(String targetSchema, String targetCn) {
		return hBaseIR.getColumnDefaultValue(targetSchema, targetCn);
	}

	public Class getColumnDataType(String targetSchema, String targetCn) {
		return hBaseIR.getColumnDataType(targetSchema, targetCn);
	}

	public boolean checkKey(String targetSchema, String parentPath, String currentPath, String key, JsonElement value) {
		return hBaseIR.checkKey(targetSchema, parentPath, currentPath, key, value);
	}

	public boolean isRowkeyFieldHashed(String targetSchema, String rowkeyField) {
		return hBaseIR.isRowkeyFieldHashed(targetSchema, rowkeyField);
	}

	public boolean isRowkeyFieldLiteral(String targetSchema, String rowkeyField) {
		return hBaseIR.isRowkeyFieldLiteral(targetSchema, rowkeyField);
	}

	public String getRowkeyFieldLiteralValue(String targetSchema, String rowkeyField) {
		return hBaseIR.getRowkeyFieldLiteralValue(targetSchema, rowkeyField);
	}

	public List<HBaseColumn> getAllColumns(String schemaName) {
		return hBaseIR.getAllColumns(schemaName);
	}

	public List<String> getNonHashRowkeyFieldNames(String schemaName) throws InvalidSchemaException {
		return hBaseIR.getNonHashRowkeyFieldNames(schemaName);
	}
	
	public boolean isColumnPresentInRowkey(String schema, String column) {
		return hBaseIR.isColumnPresentInRowkey(schema, column);
	}
	
	public boolean isColumnPresentInDynamicParts(String schema, String component) {
		return hBaseIR.isColumnPresentInDynamicParts(schema, component);
	}

}
