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

import java.io.IOException;
import java.util.Scanner;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.gstn.schemaexplorer.sqlgrammar.sqlLexer;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser;

class TestSqlQuery {

	public static void main(String[] args) throws IOException {

		System.out.println("Starting Sql Parsing...... Enter a valid Select Sql Statement");
		Scanner sc = new Scanner(System.in);

		String inputStr = sc.nextLine();
		ANTLRInputStream input = new ANTLRInputStream(inputStr);
		sqlLexer sl = new sqlLexer(input);

		// create a buffer of tokens pulled from the lexer
		CommonTokenStream tokens = new CommonTokenStream(sl);
		// create a parser that feeds off the tokens buffer
		sqlParser sp = new sqlParser(tokens);
		ParseTree tree = sp.selectStmt();

		System.out.println("\n" + tree.toStringTree(sp));

		SqlBean sqlBean = new SqlBean();
		SqlListener scl = new SqlListener(sqlBean);

		ParseTreeWalker walker = new ParseTreeWalker();
		// Walk the tree created during the parse, trigger callbacks
		walker.walk(scl, tree);
		System.out.println();
		System.out.println(sqlBean);

		sc.close();

	}

}
