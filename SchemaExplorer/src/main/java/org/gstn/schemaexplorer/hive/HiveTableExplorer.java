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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gstn.schemaexplorer.antlr4error.ThrowingErrorListener;
import org.gstn.schemaexplorer.exception.HQLRunTimeException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.exception.SchemaFileException;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarLexer;
import org.gstn.schemaexplorer.hivegrammar.HiveGrammarParser;
import org.gstn.schemaexplorer.target.TargetExplorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;

@SuppressWarnings("serial")
public class HiveTableExplorer implements TargetExplorer, Serializable {

	private Logger logger;
	private HiveTableIR hiveIR;

	public HiveTableExplorer(String schemaPath) {
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
		logger.debug("Launching shell");

		refreshSchema(schemaPath);

		// showSplash();
	}

	public void refreshSchema(String schemaPath) {
		try {
			logger.info("Reading hive schema");
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
				readHiveSchema(is);
				is.close();

			} else {
				hiveIR = new HiveTableIR();
			}

		} catch (FileNotFoundException e) {
			logger.error("Error when trying to read schema file:" + e.getMessage());
			throw new SchemaFileException("Error when trying to read schema file:" + e.getMessage());
		} catch (IOException e) {
			logger.error("Exception when trying to read schema file " + e.getMessage());
			throw new SchemaFileException("Exception when trying to read schema file " + e.getMessage());
		}
	}

	public void showSplash() {
		Scanner sc = new Scanner(getClass().getClassLoader().getResourceAsStream("splashHDFS"));
		while (sc.hasNextLine())
			System.out.println(sc.nextLine());

		sc.close();
	}

	public void launch() {
		System.out.println("Hive Explorer");
		System.out.println("=========================");
		System.out.println("Type 'help' for options ");

		boolean exit = false;

		while (!exit) {
			String command = getCommand();

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
	}

	private void listSchemas() {
		hiveIR.getSchemaNames().forEach(schemaName -> System.out.println(schemaName));
	}

	private void printHelpOptions() {
		TreeMap<String, String> options = new TreeMap<>();

		options.put("help\t\t\t", "Display this message");
		options.put("list\t\t\t", "List available hbase tables");
		options.put("show schema <schema_name> ", "Display the table schema");
		options.put("exit\t\t\t", "exit the utility");

		System.out.println("Command\t\t\tDescription");
		System.out.println("=======\t\t\t===========");

		options.forEach((key, value) -> System.out.println(key + value));
	}

	private String getCommand() {
		String command = "";
		System.out.print("\n>> ");

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
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
			String schema = tokens[2];
			try {
				System.out.println(hiveIR.getDdl(schema));
			} catch (Exception e) {
				System.out.println(e.getMessage());
				System.out.println("Try 'list' command to get list of schemas");
			}
		} else {
			System.out.println("Invalid Command. Use help to know all the supported commands");
		}
	}

	private void readHiveSchema(InputStream inputStream) throws IOException {

		ANTLRInputStream input = new ANTLRInputStream(inputStream);

		try {
			hiveIR = parse(input);
		} catch (HQLRunTimeException e) {
			logger.error("Error parsing schema: " + e.getMessage());
			throw e;
		} catch (ParseCancellationException e) {
			logger.error(e.getMessage());
			throw e;
		}
	}

	private HiveTableIR parse(ANTLRInputStream input) {

		HiveGrammarLexer lexer = new HiveGrammarLexer(input);
		lexer.removeErrorListeners();
		lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

		CommonTokenStream tokens = new CommonTokenStream(lexer);

		HiveGrammarParser parser = new HiveGrammarParser(tokens);
		parser.removeErrorListeners();
		parser.addErrorListener(ThrowingErrorListener.INSTANCE);

		HiveTableListener listener;
		HiveTableIR newHdfsSchema = new HiveTableIR();

		listener = new HiveTableListener(newHdfsSchema);

		ParseTree tree = parser.input();

		ParseTreeWalker walker = new ParseTreeWalker();

		walker.walk(listener, tree);
		return newHdfsSchema;
	}

	public boolean isSchemaDefined(String targetSchema) {
		return hiveIR.isSchemaDefined(targetSchema);
	}

	public List<String> getSchemaColumnsForJsonValidation(String targetSchema) {
		return hiveIR.getSchemaColumnsForJsonValidation(targetSchema);
	}
	
	@Override
	public Map<String,Class> getSchemaJsonColumns(String targetSchema) {
		return hiveIR.getSchemaJsonColumns(targetSchema);
	}

	public void createHiveScript(String targetSchema, String hdfsUrl, String hdfsFilePath, String hdfsPath,
			Map<String, String> sparkConfMap, String jobId, HBaseTableExplorer hBaseTableExplorer, String sourceSchemaName,
			boolean insertionAdded, boolean deleteAllAdded, boolean deleteSingleAdded, boolean dynamicColumnsInSelect) throws InvalidSchemaException {
		hiveIR.createHiveScript(targetSchema, hdfsUrl, hdfsFilePath, hdfsPath, sparkConfMap, jobId, hBaseTableExplorer, sourceSchemaName, insertionAdded, deleteAllAdded, deleteSingleAdded, dynamicColumnsInSelect);
	}
	
	public void createInsertionHiveScript(String targetSchema, String hdfsUrl, String hdfsFilePath, String hdfsBasePath, Map<String, String> sparkConfMap, String jobId){
		hiveIR.createInsertionHiveScript(targetSchema, hdfsUrl, hdfsFilePath, hdfsBasePath, sparkConfMap, jobId);
	}

	@Override
	public List<String> getSchemaColumns(String schemaName) {
		return hiveIR.getSchemaColumns(schemaName);
	}

	@Override
	public String getColumnDataType(String schemaName, String column) {
		return hiveIR.getColumnDataType(schemaName, column);
	}

	@Override
	public String getColumnDefaultValue(String schemaName, String columnName) {
		return hiveIR.getColumnDefaultValue(schemaName, columnName);
	}

	@Override
	public boolean isJsonColumn(String schemaName, String columnName) {
		return hiveIR.isJsonColumn(schemaName, columnName);
	}

	@Override
	public boolean checkKey(String targetSchema, String parentPath, String currentPath, String key, JsonElement value) {
		return hiveIR.checkKey(targetSchema, parentPath, currentPath, key, value);
	}

}
