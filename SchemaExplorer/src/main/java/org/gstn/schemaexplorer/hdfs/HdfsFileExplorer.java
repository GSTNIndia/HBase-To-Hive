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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;
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
import org.gstn.schemaexplorer.exception.SchemaFileException;
import org.gstn.schemaexplorer.hdfsgrammar.HdfsGrammarLexer;
import org.gstn.schemaexplorer.hdfsgrammar.HdfsGrammarParser;
import org.gstn.schemaexplorer.target.TargetExplorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;

/**
 * This class reads schema from input schema file and stores that information
 * in an object of HdfsFileIR class. 
 */
@SuppressWarnings("serial")
public class HdfsFileExplorer implements TargetExplorer, Serializable {

	private Logger logger;
	private HdfsFileIR hdfsIR;

	/**
	 * Constructor.
	 * @param schemaPath is the local input schema file path.
	 */
	public HdfsFileExplorer(String schemaPath) {
		logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
		logger.debug("Launching shell");

		refreshSchema(schemaPath);
		
//		showSplash();
	}

	/**
	 * This method takes a file path as input and converts it into FileInputStream object
	 * and passes it to readHdfsSchema method which parses the Stream object.
	 * @param schemaPath is the local input schema file path.
	 */
	public void refreshSchema(String schemaPath) {
		try {
			logger.info("Reading hdfs schema");
			
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
				readHdfsSchema(is);
				is.close();
			} else {
				hdfsIR = new HdfsFileIR();
			}			
		} catch (FileNotFoundException e) {
			logger.error("Error when trying to read " + schemaPath + " " + e.getMessage());
			throw new SchemaFileException("Error when trying to read " + schemaPath + " " + e.getMessage());
		} catch (IOException e) {
			logger.error("Exception when trying to read " + schemaPath + " " + e.getMessage());
			throw new SchemaFileException("Exception when trying to read " + schemaPath + " " + e.getMessage());
		}
	}

	/**
	 * Prints splash to console.
	 */
	public void showSplash() {
        Scanner sc = new Scanner(getClass().getClassLoader().getResourceAsStream("splashHDFS"));
        while(sc.hasNextLine())
            System.out.println(sc.nextLine());
        
        sc.close();
    }

	/**
	 * This method reads input command from console through getCommand method and 
	 * processes it accordingly. 
	 */
	public void launch() {
		System.out.println("Hdfs Explorer");
		System.out.println("=========================");
		System.out.println("Type 'help' for options ");

		boolean exit = false;

		while (!exit) {
			String command = getCommand();

			switch (command.toLowerCase()) {
			case "hi":
			case "hello":
				System.out.println("Hi!");
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
	
	/**
	 * Prints all the target schema names in the input schema file.
	 */
	private void listSchemas() {
		hdfsIR.getSchemaNames().forEach(schemaName -> System.out.println(schemaName));
	}

	/**
	 * This method list all the options available
	 * i.e. help, list, show table, exit.
	 */
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
	
	public void processCommand(String command) {
		if (command.startsWith("show schema")) {
			String[] tokens = command.split("[ ]+");
			if (tokens.length < 3) {
				System.err.println("Please specify schema name. e.g. show schema INVOICES");
			}
			String schema = tokens[2];
			try {
				System.out.println(hdfsIR.getDdl(schema));
			} catch (Exception e) {
				System.out.println(e.getMessage());
				System.out.println("Try 'list' command to get list of schemas");
			}
		} else {
			System.out.println("Invalid Command. Use help to know all the supported commands");
		}
	}
	
	/**
	 * Reads input command from the console and returns it as a String.
	 * @return Input String from the console.
	 */
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

	/**
	 * This method creates an ANTLRInputStream object out of inputStream object and passes
	 * it to parse method which parses the schema file and stores the information in 
	 * a HdfsFileIR object.
	 * @param inputStream is the InputStream object of the input schema file.
	 * @throws IOException is there is an error in the schema.
	 */
	private void readHdfsSchema(InputStream inputStream) throws IOException {
		
		ANTLRInputStream input = new ANTLRInputStream(inputStream);

		try {
			hdfsIR = parse(input);
		} catch (HQLRunTimeException e) {
			logger.error("Error parsing schema: " + e.getMessage());
			throw e;
		} catch (ParseCancellationException e) {
			logger.error(e.getMessage());
			throw e;
		}

	}

	/**
	 * This method parses the input ANTLRInputStream object using a custom ANTLR listener 
	 * method.
	 * @param input is an ANTLRInputStream object.
	 * @return HdfsFileIR object which stores all the information about target schemas.
	 */
	private HdfsFileIR parse(ANTLRInputStream input) {
		
		HdfsGrammarLexer lexer = new HdfsGrammarLexer(input);
		lexer.removeErrorListeners();
		lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

		CommonTokenStream tokens = new CommonTokenStream(lexer);

		HdfsGrammarParser parser = new HdfsGrammarParser(tokens);
		parser.removeErrorListeners();
		parser.addErrorListener(ThrowingErrorListener.INSTANCE);

		HdfsFileListener listener;
		HdfsFileIR newHdfsSchema = new HdfsFileIR();

		listener = new HdfsFileListener(newHdfsSchema);

		ParseTree tree = parser.input();

		ParseTreeWalker walker = new ParseTreeWalker();

		walker.walk(listener, tree);
		return newHdfsSchema;
	}

	public boolean isSchemaDefined(String targetSchema) {
		return hdfsIR.isSchemaDefined(targetSchema);
	}

	public List<String> getSchemaColumnsForJsonValidation(String targetSchema) {
		return hdfsIR.getSchemaColumnsForJsonValidation(targetSchema);
	}

	@Override
	public List<String> getSchemaColumns(String schemaName) {
		return hdfsIR.getSchemaColumns(schemaName);
	}

	@Override
	public String getColumnDataType(String schemaName, String columnName) {
		return hdfsIR.getColumnDataType(schemaName, columnName);
	}

	@Override
	public String getColumnDefaultValue(String schemaName, String columnName) {
		return hdfsIR.getColumnDefaultValue(schemaName, columnName);
	}

	@Override
	public boolean isJsonColumn(String schemaName, String columnName) {
		return hdfsIR.isJsonColumn(schemaName, columnName);
	}

	@Override
	public boolean checkKey(String targetSchema, String parentPath, String currentPath, String key, JsonElement value) {
		return hdfsIR.checkKey(targetSchema, parentPath, currentPath, key, value);
	}

}
