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
import java.util.ArrayList;
import java.util.List;

import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.entity.DynamicColumnType;
import org.gstn.schemaexplorer.entity.Tuple;
import org.gstn.schemaexplorer.exception.ColumnNotFoundException;
import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.HQLRunTimeException;
import org.gstn.schemaexplorer.exception.InvalidColumnException;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.junit.Assert;
import org.junit.Test;



public class ConditionTreeManagerTest {

	static HBaseTableExplorer exp;
	static{
		exp = new HBaseTableExplorer("./src/test/resources/HBaseExplorerTest.schema");
	}
	
	// each case should be tested for true,false and null(if applicable) And
	// each pair should be tested for AND,OR
	// single condition - rowkey , static , dynamic
	// two conditions - both rowkey , both static , both dynamic , rowkey-static , rowkey-dynamic , static-dynamic
	// multiple conditions (three) - (c1 op1 c2 ) op2 c3 test for conditionInBracket,c3 to be true,false,null and op2 to be true,false

	//one test case for testing all supported relational operators
	
	@Test
	public void testSingleRowKeyCondition() {
		
		List<TestData> tests = new ArrayList<>();
		
		// single condition - rowkey true
		tests.add(new TestData("rec_type = \"B2B\"", true));
		// single condition - rowkey false
		tests.add(new TestData("rec_type = \"B2CL\"", false));
		
		runTests(tests);
	}
	
	@Test
	public void testSingleStaticCondition() {
		
		List<TestData> tests = new ArrayList<>();
		
		// single condition - static true
		tests.add(new TestData("D.SR1=\"Y\"", true));
		// single condition - static false
		tests.add(new TestData("D.SR1=\"N\"", false));
		// single condition - static missing column
		tests.add(new TestData("D.SR3=\"Y\"", false));
		// single condition - static missing column null check
		tests.add(new TestData("D.SR3=\"null\"", true));
		// single condition - static missing column not null check
		tests.add(new TestData("D.SR3!=\"null\"", false));
		// single condition - static column null check
		tests.add(new TestData("D.SR1=\"null\"", false));
		// single condition - static column not null check
		tests.add(new TestData("D.SR1!=\"null\"", true));
		
		runTests(tests);
	}
	
	@Test
	public void testSingleDynamicCondition() {
		
		List<TestData> tests = new ArrayList<>();
		// single condition - dynamic true
		tests.add(new TestData("D.ECOM<X> = \"Y\"", null ,  true));
		// single condition - dynamic false
		tests.add(new TestData("D.ECOM<X> = \"N\"", null ,  false));
		// single condition - dynamic non missing null check
		tests.add(new TestData("D.ECOM<X> = \"null\"", null , false));
		// single condition - dynamic non missing not null check
		tests.add(new TestData("D.ECOM<X> != \"null\"", null , true));
		// single condition - dynamic missing
		tests.add(new TestData("D.RTYP<X> = \"N\"", null , false));
		// single condition - dynamic missing null check
		tests.add(new TestData("D.RTYP<X> = \"null\"", null , true));
		// single condition - dynamic missing not null check
		tests.add(new TestData("D.RTYP<X> != \"null\"", null , false));
		
		runTests(tests);
	}
	
	@Test
	public void testBothRowKeyConditions(){
		
		List<TestData> tests = new ArrayList<>();
		// true and true
		tests.add(new TestData("rec_type = \"B2B\" and rtin = \"R0001\"", true));
		// true and false
		tests.add(new TestData("rec_type = \"B2B\" and rtin = \"R0002\"", false));
		// false and false
		tests.add(new TestData("rec_type != \"B2B\" and rtin = \"R0002\"", false));
		// false and true
		tests.add(new TestData("rec_type != \"B2B\" and rtin = \"R0001\"", false));
				
		//OR
		// true or true
		tests.add(new TestData("rec_type = \"B2B\" or rtin = \"R0001\"", true));
		// true or false
		tests.add(new TestData("rec_type = \"B2B\" or rtin = \"R0002\"", true));
		// false or false
		tests.add(new TestData("rec_type != \"B2B\" or rtin = \"R0002\"", false));
		// false or true
		tests.add(new TestData("rec_type != \"B2B\" or rtin = \"R0001\"", true));
		
		runTests(tests);
	}
	
	@Test
	public void testBothStaticConditions(){
		
		List<TestData> tests = new ArrayList<>();
		// true and true
		tests.add(new TestData(" D.SR1 = \"Y\" and D.SR1 = \"Y\" ", true));
		// true and false
		tests.add(new TestData(" D.SR1 = \"Y\" and D.SR1 = \"N\" ", false));
		// false and false
		tests.add(new TestData(" D.SR1 = \"N\" and D.SR1 = \"Y\" ", false));
		// false and true
		tests.add(new TestData(" D.SR1 != \"Y\" and D.SR2 = \"Y\" ", false));
				
		//OR
		// true or true
		tests.add(new TestData(" D.SR1 = \"Y\" or D.SR3 = \"null\" ", true));
		// true or false
		tests.add(new TestData(" D.SR1 = \"Y\" or D.SR3 != \"null\"  ", true));
		// false or false
		tests.add(new TestData(" D.SR1 = \"N\" or D.SR3 = \"Y\" ", false));
		// false or true
		tests.add(new TestData(" D.SR1 != \"Y\" or D.SR2 = \"Y\" ", true));
		
		runTests(tests);
	}
	
	@Test
	public void tesBothDynamicConditions() {
		
		List<TestData> tests = new ArrayList<>();
		// true and true
		tests.add(new TestData(" D.ECOM<X> = \"Y\" and D.RC<X> = \"Y\" ", null , true));
		// true and false
		tests.add(new TestData(" D.ECOM<X> = \"Y\" and D.RTYP<X> = \"Y\" ", null , false));
		// false and true
		tests.add(new TestData(" D.RC<X> = \"N\" and D.RTYP<X> != \"null\" ", null , false));
		// false and false
		tests.add(new TestData(" D.ECOM<X> = \"N\" and D.RTYP<X> = \"null\" ", null , false));
		
		// true or true
		tests.add(new TestData(" D.ECOM<X> = \"Y\" or D.RC<X> = \"Y\" ", null , true));
		// true or false
		tests.add(new TestData(" D.ECOM<X> = \"Y\" or D.RTYP<X> = \"Y\" ", null , true));
		// false or false
		tests.add(new TestData(" D.ECOM<X> = \"N\" or D.RTYP<X> != \"null\" ", null , false));
		// false or true
		tests.add(new TestData(" D.RC<X> = \"N\" or  D.RTYP<X> = \"null\" ", null , true));
				
		runTests(tests);
	}
	
	@Test
	public void testRowKeyStaticConditions(){
		List<TestData> tests = new ArrayList<>();
		// true and true
		tests.add(new TestData(" rec_type = \"B2B\" and D.SR1 = \"Y\" ", true));
		// true and false
		tests.add(new TestData("rec_type = \"B2B\" and D.SR3 = \"Y\" ", false));
		// false and false
		tests.add(new TestData("rec_type != \"B2B\" and D.SR3 != \"null\" ", false));
		// false and true
		tests.add(new TestData("rec_type != \"B2B\" and D.SR3 = \"null\" ", false));
				
		//OR
		// true or true
		tests.add(new TestData(" rec_type = \"B2B\" or D.SR1 = \"Y\" ", true));
		// true or false
		tests.add(new TestData("rec_type = \"B2B\" or D.SR3 = \"Y\" ", true));
		// false or false
		tests.add(new TestData("rec_type != \"B2B\" or D.SR3 != \"null\" ", false));
		// false or true
		tests.add(new TestData("rec_type != \"B2B\" or D.SR3 = \"null\" ", true));
		
		runTests(tests);
	}
	
	@Test
	public void testRowKeyDynamicConditions(){
		List<TestData> tests = new ArrayList<>();
		// true and true
		tests.add(new TestData(" rec_type = \"B2B\" and D.RC<X> = \"Y\" ", null, true));
		// true and false
		tests.add(new TestData("rec_type = \"B2B\" and D.RTYP<X> = \"Y\" ", null, false));
		// false and false
		tests.add(new TestData("rec_type != \"B2B\" and D.RTYP<X> != \"null\" ", false));
		// false and true
		tests.add(new TestData("rec_type != \"B2B\" and D.RTYP<X> = \"null\" ", false));
				
		//OR
		// true or true
		tests.add(new TestData(" rec_type = \"B2B\" or D.RC<X> = \"Y\" ", true));
		// true or false
		tests.add(new TestData("rec_type = \"B2B\" or D.RTYP<X> = \"Y\" ", true));
		// false or false
		tests.add(new TestData("rec_type != \"B2B\" or D.RTYP<X> != \"null\" ", null, false));
		// false or true
		tests.add(new TestData("rec_type != \"B2B\" or D.RTYP<X> = \"null\" ", null, true));
		
		runTests(tests);
	}
	
	@Test
	public void testStaticDynamicConditions(){
		List<TestData> tests = new ArrayList<>();
		// true and true
		tests.add(new TestData(" D.SR1=\"Y\" and D.RC<X> = \"Y\" ", null, true));
		// true and false
		tests.add(new TestData(" D.SR1=\"Y\" and D.RTYP<X> = \"Y\" ", null, false));
		// false and false
		tests.add(new TestData(" D.SR1=\"N\" and D.RTYP<X> != \"null\" ", false));
		// false and true
		tests.add(new TestData(" D.SR1=\"N\" and D.RTYP<X> = \"null\" ", false));
				
		//OR
		// true or true
		tests.add(new TestData(" D.SR1=\"Y\" or D.RC<X> = \"Y\" ", true));
		// true or false
		tests.add(new TestData(" D.SR1=\"Y\" or D.RTYP<X> = \"Y\" ", true));
		// false or false
		tests.add(new TestData(" D.SR1=\"N\" or D.RTYP<X> != \"null\" ", null, false));
		// false or true
		tests.add(new TestData(" D.SR1=\"N\" or D.RTYP<X> = \"null\" ", null, true));
		
		runTests(tests);
	}
	
	@Test
	public void testMultipleConditions(){
		List<TestData> tests = new ArrayList<>();
		// conditionsInBracket op staticCondition
		// op=and 
		//true and true
		tests.add(new TestData(" ( D.SR1=\"Y\" and D.RC<X> = \"Y\" ) and D.SR2=\"Y\" ", null, true));
		// true and false
		tests.add(new TestData(" ( D.SR1=\"Y\" and D.RC<X> = \"Y\" ) and D.SR2=\"N\" ", false));
		// false and false
		tests.add(new TestData(" ( D.SR1=\"N\" and D.RTYP<X> != \"null\" ) and D.SR2=\"N\" ", false));
		// false and true
		tests.add(new TestData(" ( D.SR1=\"N\" and D.RTYP<X> = \"null\" ) and D.SR2=\"Y\" ", false));
				
		//op=OR
		// true or true
		tests.add(new TestData(" ( D.SR1=\"Y\" or D.RC<X> = \"Y\" ) or D.SR2=\"Y\" ", true));
		// true or false
		tests.add(new TestData(" ( D.SR1=\"N\" or D.RTYP<X> = \"null\" ) or D.SR2=\"N\" ", null, true));
		// false or false
		tests.add(new TestData(" ( D.SR1=\"N\" or D.RTYP<X> != \"null\" ) or D.SR2=\"N\" ", null, false));
		// false or true
		tests.add(new TestData(" ( D.SR1=\"N\" or D.RTYP<X> = \"Y\" ) or D.SR2=\"Y\" ",true));
		
		// conditionsInBracket op dynamicCondition
		// op=and 
		//true and true
		tests.add(new TestData(" ( D.SR1=\"Y\" and D.RC<X> = \"Y\" ) and D.ECOM<X> = \"Y\" ", null, true));
		// true and false
		tests.add(new TestData(" ( D.SR1=\"Y\" and D.RC<X> = \"Y\" ) and D.RTYP<X> = \"Y\" ", null, false));
		// false and false
		tests.add(new TestData(" ( D.SR1=\"N\" and D.RTYP<X> != \"null\" ) and D.RTYP<X> != \"null\" ", false));
		// false and true
		tests.add(new TestData(" ( D.SR1=\"Y\" and D.RTYP<X> != \"null\" ) and D.RTYP<X> = \"null\" ", null, false));
				
		//op=OR
		// true or true
		tests.add(new TestData(" ( D.SR1=\"Y\" or D.RC<X> = \"Y\" ) or D.ECOM<X> = \"Y\" ", true));
		// true or false
		tests.add(new TestData(" ( D.SR1=\"N\" or D.RTYP<X> = \"null\" ) or D.ECOM<X> = \"N\" ", null, true));
		// false or false
		tests.add(new TestData(" ( D.SR1=\"N\" or D.RTYP<X> != \"null\" ) or D.ECOM<X> != \"Y\" ", null, false));
		// false or true
		tests.add(new TestData(" ( D.SR1=\"N\" or D.RTYP<X> != \"null\" ) or D.ECOM<X> = \"Y\" ", null, true));
		// (true and false) and true
		tests.add(new TestData("((D.SR1=\"Y\" or D.RC<X> != \"Y\") and (D.ECOM<X> =\"Y\" and D.RTYP<X>!=\"null\")) and D.TAX=\"10\"", null, false));
		// (true or false) and true
		tests.add(new TestData("((D.SR1=\"Y\" or D.SR2 != \"N\") or (D.ECOM<X> =\"Y\" and D.RTYP<X>!=\"null\")) and D.TAX=\"10\"", true, true));
		
		runTests(tests);
	}
	
	@Test
	public void testDynamicPartCondition(){
		List<TestData> tests = new ArrayList<>();
		// (true or false) and true
		tests.add(new TestData("((D.SR1=\"Y\" or D.SR2 != \"N\") or (D.ECOM<X> =\"Y\" and D.RTYP<X>!=\"null\")) and provider=\"S\"", null, true));
		// (true or false) and false
		tests.add(new TestData("((D.SR1=\"Y\" or D.SR2 != \"N\") or (D.ECOM<X> =\"Y\" and D.RTYP<X>!=\"null\")) and provider=\"R\"", null, false));
		// true and true
		tests.add(new TestData("provider=\"S\" and fy=\"2017\"", null, true));
		// false and true
		tests.add(new TestData("inum=\"R0001\" and fy=\"2016\"", null, false));
		
		runTests(tests);
	}

	@Test
	public void testRelationalOperators(){
		List<TestData> tests = new ArrayList<>();
		
		//for integer column
		tests.add(new TestData(" D.TAX=\"10\"  ", true));
		tests.add(new TestData(" D.TAX=\"20\"  ", false));
		tests.add(new TestData(" D.TAX!=\"20\"  ", true));
		tests.add(new TestData(" D.TAX!=\"10\"  ", false));
		tests.add(new TestData(" D.TAX<>\"20\"  ", true));
		tests.add(new TestData(" D.TAX<>\"10\"  ", false));
		tests.add(new TestData(" D.TAX<\"20\"  ", true));
		tests.add(new TestData(" D.TAX<\"9\"  ", false));
		tests.add(new TestData(" D.TAX>\"9\"  ", true));
		tests.add(new TestData(" D.TAX>\"20\"  ", false));
		tests.add(new TestData(" D.TAX>=\"10\"  ", true));
		tests.add(new TestData(" D.TAX>=\"8\"  ", true));
		tests.add(new TestData(" D.TAX>=\"20\"  ", false));
		tests.add(new TestData(" D.TAX<=\"10\"  ", true));
		tests.add(new TestData(" D.TAX<=\"20\"  ", true));
		tests.add(new TestData(" D.TAX<=\"9\"  ", false));
		
		//for string column
		tests.add(new TestData(" D.SR1=\"Y\"  ", true));
		tests.add(new TestData(" D.SR1=\"N\"  ", false));
		tests.add(new TestData(" D.SR1!=\"N\"  ", true));
		tests.add(new TestData(" D.SR1!=\"Y\"  ", false));
		tests.add(new TestData(" D.SR1<>\"N\"  ", true));
		tests.add(new TestData(" D.SR1<>\"Y\"  ", false));
		
		runTests(tests);
		
		tests = new ArrayList<>();
		tests.add(new TestData(" D.SR1>\"Y\"  ", null));
		tests.add(new TestData(" D.SR1<\"Y\"  ", null));
		tests.add(new TestData(" D.SR1>=\"Y\"  ", null));
		tests.add(new TestData(" D.SR1<=\"Y\"  ", null));
		
		runExceptionTests(tests);
	}
	

	public void runTests(List<TestData> tests){
		for (TestData testData : tests) {
			Boolean result = null;
			try {
				String query = "select * from test where " + testData.conditions;
				ConditionTree conditionTree = getConditionTreeFromSql(query);
				DataRecord dataRecord = getRowKeyStaticColumnsDataRecord();

				result = ConditionTreeManager.evaluateStaticColumnAndRowKeyConditions(conditionTree, dataRecord);
				Assert.assertEquals(testData.expectedStaticResult, result);
				
				if(testData.applyDynamic){
					addDynamicColumnsIntoDataRecord(dataRecord);
					result = ConditionTreeManager.evaluateDyanmicColumnConditions(conditionTree, dataRecord);
					Assert.assertEquals(testData.expectedDynamicResult, result);
				}
			} catch (Exception | AssertionError e) {
				throw new AssertionError("For Condition: "+testData.conditions +"\n"+e);
			}
			
		}
	}
	
	public void runExceptionTests(List<TestData> tests){
		for (TestData testData : tests) {
			try {
				String query = "select * from test where "+testData.conditions;
				ConditionTree conditionTree = getConditionTreeFromSql(query);
				DataRecord dataRecord = getRowKeyStaticColumnsDataRecord();

				ConditionTreeManager.evaluateStaticColumnAndRowKeyConditions(conditionTree, dataRecord);
				throw new AssertionError("For Condition: "+testData.conditions +".......Expected HQLException but didn't received it.");
			}catch (HQLException e) {
				if(!e.getMessage().contains("Numeric comparison not allowed")){
					throw new AssertionError("For Condition: "+testData.conditions +".......Expected HQLException having Numeric comparison message, but received HQLException with some other message. \n"+e);
				}
			} 
			catch (Exception e) {
				throw new AssertionError("For Condition: "+testData.conditions +".......Expected HQLException but received some other exception. \n"+e);
			}
			
		}
	}
	
	public ConditionTree getConditionTreeFromSql(String query) throws HQLRunTimeException, IOException, HQLException, ColumnNotFoundException{
		SqlBean sqlBean = exp.parseAndGetValidatedQuery(query);
		return sqlBean.getConditionTreeCopy();
	}
	
	public DataRecord getRowKeyStaticColumnsDataRecord(){
		List<Tuple> keyList = new ArrayList<>();
		keyList.add(Tuple.rowkeyColumn("fp","2017-09"));
		keyList.add(Tuple.rowkeyColumn("rec_type","B2B"));
		keyList.add(Tuple.rowkeyColumn("rtin","R0001"));
		keyList.add(Tuple.rowkeyColumn("stin","S0001"));
		
		List<Tuple> valueList = new ArrayList<>();
		valueList.add(Tuple.staticColumn("D", "SR1", "Y", String.class));
		valueList.add(Tuple.staticColumn("D", "SR2", "Y", String.class));
		valueList.add(Tuple.staticColumn("D", "TAX", "10", Integer.class));
		
		return new DataRecord(keyList, valueList);
	}
	
	public void addDynamicColumnsIntoDataRecord(DataRecord dataRecord) throws InvalidColumnException{
		
		dataRecord.addTupleToColumn("D", "RC", Tuple.dynamicColumn("D", "RC", "Y", "S|2017|I0001", DynamicColumnType.STATIC_PREFIX, String.class));
		dataRecord.addTupleToColumn("D", "ECOM", Tuple.dynamicColumn("D", "ECOM", "Y", "S|2017|I0001", DynamicColumnType.STATIC_PREFIX, String.class));
		
		//adding dynamic parts
		dataRecord.addTupleToColumn("D", "provider", Tuple.dynamicColumn("D", "provider", "S", "", DynamicColumnType.STATIC_PREFIX, String.class));
		dataRecord.addTupleToColumn("D", "fy", Tuple.dynamicColumn("D", "fy", "2017", "", DynamicColumnType.STATIC_PREFIX, String.class));
		dataRecord.addTupleToColumn("D", "inum", Tuple.dynamicColumn("D", "inum", "I0001", "", DynamicColumnType.STATIC_PREFIX, String.class));
	}
	
	
}

class TestData{
	String conditions;
	Boolean expectedStaticResult;
	boolean applyDynamic;
	Boolean expectedDynamicResult;
	
	Boolean staticEvaluationExceptionExpected;
	Boolean dynamicEvaluationExceptionExpected;
	
	public TestData(String conditions, Boolean expectedStaticResult) {
		super();
		this.conditions = conditions;
		this.expectedStaticResult = expectedStaticResult;
	}
	
	public TestData(String conditions, Boolean expectedStaticResult, Boolean expectedDynamicResult) {
		super();
		this.conditions = conditions;
		this.expectedStaticResult = expectedStaticResult;
		this.applyDynamic = true;
		this.expectedDynamicResult = expectedDynamicResult;
	}

	public TestData(Boolean staticEvaluationExceptionExpected, Boolean dynamicEvaluationExceptionExpected) {
		super();
		this.staticEvaluationExceptionExpected = staticEvaluationExceptionExpected;
		this.dynamicEvaluationExceptionExpected = dynamicEvaluationExceptionExpected;
		if(dynamicEvaluationExceptionExpected!=null){
			applyDynamic=true;
		}
	}
	
}
