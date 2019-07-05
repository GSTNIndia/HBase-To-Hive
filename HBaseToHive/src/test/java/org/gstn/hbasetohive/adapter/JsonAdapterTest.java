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
package org.gstn.hbasetohive.adapter;

import static org.gstn.schemaexplorer.target.Constants.PARENT_PATH_SEPARATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.entity.Tuple;
import org.gstn.schemaexplorer.exception.ColumnFamilyNotFoundException;
import org.gstn.schemaexplorer.exception.ColumnNotFoundException;
import org.gstn.schemaexplorer.exception.InvalidColumnException;
import org.gstn.schemaexplorer.exception.InvalidJSONException;
import org.gstn.schemaexplorer.hdfs.HdfsFileExplorer;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class JsonAdapterTest extends TestCase {

	private String nestedJson;
	private String nestedJsonHavingList;
	private String jsonHavingMultipleList;
	private String jsonArray;
	private DataRecord dataRecord;
	private HdfsFileExplorer hdfsExplorer = new HdfsFileExplorer("./src/test/resources/JsonAdapterTest.schema");

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		// json structure: A>B -> AB1,AB2 | A>C -> AC | A>D>E -> ADE
		nestedJson = "{\"A\":{\"B\":{\"AB1\":\"AB1\",\"AB2\":\"AB2\"},\"C\":{\"AC\":\"AC\"},\"D\":{\"E\":{\"ADE\":\"ADE\"}}}}";

		// json structure: A>B -> AB1,AB2 | A>C -> AC | A>D>E -> ADE
		// 1>2 -> 12 | 1>3 -> [131,132,133] (list of json) | 1>4 -> [141,142]
		// (list of non json)
		nestedJsonHavingList = "{\"A\":{\"B\":{\"AB1\":\"AB1\",\"AB2\":\"AB2\"},\"C\":{\"AC\":\"AC\"},\"D\":{\"E\":{\"ADE\":\"ADE\"}}},\"1\":{\"2\":{\"12\":\"12\"},\"3\":[{\"131\":\"131\"},{\"132\":\"132\"},{\"133\":\"133\"}],\"4\":[\"141\",\"142\"]}}";

		// json structure: A | E | B -> [BD,BE,BF] (list of json) | C ->
		// [CD,CE,CF] (list of json)
		jsonHavingMultipleList = "{\"A\":\"A\",\"E\":\"E\",\"B\":[{\"BD\":\"BD\"},{\"BE\":\"BE\"},{\"BF\":\"BF\"}],\"C\":[{\"CD\":\"CD\"},{\"CE\":\"CE\"},{\"CF\":\"CF\"}]}";

		// json structure: [ {rt , trnovr , tax , items->[{ no , val}] } ]
		jsonArray = "[{\"rt\":\"1\",\"trnovr\":\"10000\",\"tax\":\"1000\",\"items\":[{\"no\":\"1\",\"val\":\"100\"},{\"no\":\"2\",\"val\":\"101\"}]},{\"rt\":\"5\",\"trnovr\":\"20000\",\"tax\":\"2000\",\"items\":[{\"no\":\"a\",\"val\":\"200\"},{\"no\":\"b\",\"val\":\"201\"}]}]";

		dataRecord = new DataRecord(Arrays.asList(Tuple.rowkeyColumn("fp", "2017-09")),
				Arrays.asList(Tuple.staticColumn("D", "SR1", "Y", String.class)));
	}

	@Test
	public void testFlattenJsonOnNestedJsonGivingParentPath() throws Exception {

		// test to select some of the json elements by specifying their parent
		// paths
		// expected result is dataRecords for only the selected json elements
		// having column name with parent path i.e parentPath_colName

		TargetModel target = new HdfsTargetModel("test1", hdfsExplorer);

		List<DataRecord> result = JsonAdapter.flattenJson(dataRecord, nestedJson, target);

		// creating the expected data
		List<Map<String, String>> expectedColumnsList = new ArrayList<>();
		Map<String, String> colNameValuesMap = new HashMap<>();
		colNameValuesMap.put("fp", "2017-09");
		colNameValuesMap.put("SR1", "Y");
		colNameValuesMap.put("A" + PARENT_PATH_SEPARATOR + "B" + PARENT_PATH_SEPARATOR + "AB1", "AB1");
		colNameValuesMap.put("A" + PARENT_PATH_SEPARATOR + "B" + PARENT_PATH_SEPARATOR + "AB2", "AB2");
		colNameValuesMap.put(
				"A" + PARENT_PATH_SEPARATOR + "D" + PARENT_PATH_SEPARATOR + "E" + PARENT_PATH_SEPARATOR + "ADE", "ADE");
		expectedColumnsList.add(colNameValuesMap);

		Assert.assertTrue(compareResult(result, expectedColumnsList));

	}

	// We have made parent path mandatory for non root elements
	@Test
	public void testFlattenJsonOnNestedJsonWithoutGivingParentPath() throws Exception {

		// trying to fetch non root columns without specifying their parent path
		// expected result is no columns from json to be fetched

		TargetModel target = new HdfsTargetModel("test2", hdfsExplorer);

		List<DataRecord> result = JsonAdapter.flattenJson(dataRecord, nestedJson, target);

		// creating the expected data
		List<Map<String, String>> expectedColumnsList = new ArrayList<>();
		Map<String, String> colNameValuesMap = new HashMap<>();
		colNameValuesMap.put("fp", "2017-09");
		colNameValuesMap.put("SR1", "Y");
		/*
		 * colNameValuesMap.put("AB1","AB1"); colNameValuesMap.put("AB2","AB2");
		 * colNameValuesMap.put("AC","AC"); colNameValuesMap.put("ADE","ADE");
		 */
		expectedColumnsList.add(colNameValuesMap);

		Assert.assertTrue(compareResult(result, expectedColumnsList));
	}

	@Test
	public void testFlattenJsonOnNestedJsonHavingListGivingParentPath() throws Exception {

		// testing for json having nested json & list of json elements & list of
		// non json
		// test1. to select some of the json elements by specifying their parent
		// paths

		// selecting AB1,AB2,ADE,12,[131,133],[14 ->(will result in selecting
		// two tuples with column name 14 and values 141 and 142)]

		TargetModel target = new HdfsTargetModel("test3", hdfsExplorer);

		List<DataRecord> result = JsonAdapter.flattenJson(dataRecord, nestedJsonHavingList, target);

		// creating the expected data [four data records should be there]
		List<Map<String, String>> expectedColumnsList = new ArrayList<>();
		Map<String, String> commonColumnValuesMap = new HashMap<>();
		commonColumnValuesMap.put("fp", "2017-09");
		commonColumnValuesMap.put("SR1", "Y");
		commonColumnValuesMap.put("A" + PARENT_PATH_SEPARATOR + "B" + PARENT_PATH_SEPARATOR + "AB1", "AB1");
		commonColumnValuesMap.put("A" + PARENT_PATH_SEPARATOR + "B" + PARENT_PATH_SEPARATOR + "AB2", "AB2");
		commonColumnValuesMap.put(
				"A" + PARENT_PATH_SEPARATOR + "D" + PARENT_PATH_SEPARATOR + "E" + PARENT_PATH_SEPARATOR + "ADE", "ADE");
		commonColumnValuesMap.put("1" + PARENT_PATH_SEPARATOR + "2" + PARENT_PATH_SEPARATOR + "12", "12");

		Map<String, String> otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("1" + PARENT_PATH_SEPARATOR + "3" + PARENT_PATH_SEPARATOR + "131", "131");
		otherColumnValuesMap.put("1" + PARENT_PATH_SEPARATOR + "4", "141");
		expectedColumnsList.add(otherColumnValuesMap); // added for 131 with 141

		otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("1" + PARENT_PATH_SEPARATOR + "3" + PARENT_PATH_SEPARATOR + "131", "131");
		otherColumnValuesMap.put("1" + PARENT_PATH_SEPARATOR + "4", "142");
		expectedColumnsList.add(otherColumnValuesMap); // added for 131 with 142

		otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("1" + PARENT_PATH_SEPARATOR + "3" + PARENT_PATH_SEPARATOR + "133", "133");
		otherColumnValuesMap.put("1" + PARENT_PATH_SEPARATOR + "4", "141");
		expectedColumnsList.add(otherColumnValuesMap); // added for 133 with 141

		otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("1" + PARENT_PATH_SEPARATOR + "3" + PARENT_PATH_SEPARATOR + "133", "133");
		otherColumnValuesMap.put("1" + PARENT_PATH_SEPARATOR + "4", "142");
		expectedColumnsList.add(otherColumnValuesMap); // added for 133 with 142

		Assert.assertTrue(compareResult(result, expectedColumnsList));
	}

	// We have made parent path mandatory for non root elements
	@Test
	public void testFlattenJsonOnNestedJsonHavingListWithoutGivingParentPath() throws Exception {

		// trying to fetch non root columns without specifying their parent path
		// expected result is no columns from json to be fetched

		TargetModel target = new HdfsTargetModel("test4", hdfsExplorer);

		List<DataRecord> result = JsonAdapter.flattenJson(dataRecord, nestedJsonHavingList, target);

		// creating the expected data [four data records should be there]
		List<Map<String, String>> expectedColumnsList = new ArrayList<>();
		Map<String, String> commonColumnValuesMap = new HashMap<>();
		commonColumnValuesMap.put("fp", "2017-09");
		commonColumnValuesMap.put("SR1", "Y");

		expectedColumnsList.add(commonColumnValuesMap);

		Assert.assertTrue(compareResult(result, expectedColumnsList));
	}

	@Test
	public void testFlattenJsonForFetchingSomeRootAndNonRootElements() throws Exception {

		TargetModel target = new HdfsTargetModel("test5", hdfsExplorer);

		List<DataRecord> result = JsonAdapter.flattenJson(dataRecord, jsonHavingMultipleList, target);

		// creating the expected data [four data records should be there]
		List<Map<String, String>> expectedColumnsList = new ArrayList<>();
		Map<String, String> commonColumnValuesMap = new HashMap<>();
		commonColumnValuesMap.put("fp", "2017-09");
		commonColumnValuesMap.put("SR1", "Y");
		commonColumnValuesMap.put("A", "A");

		// for BD-CE
		Map<String, String> otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("B" + PARENT_PATH_SEPARATOR + "BD", "BD");
		otherColumnValuesMap.put("C" + PARENT_PATH_SEPARATOR + "CE", "CE");
		expectedColumnsList.add(otherColumnValuesMap);

		// for BD-CF
		otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("B" + PARENT_PATH_SEPARATOR + "BD", "BD");
		otherColumnValuesMap.put("C" + PARENT_PATH_SEPARATOR + "CF", "CF");
		expectedColumnsList.add(otherColumnValuesMap);

		// for BE-CE
		otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("B" + PARENT_PATH_SEPARATOR + "BE", "BE");
		otherColumnValuesMap.put("C" + PARENT_PATH_SEPARATOR + "CE", "CE");
		expectedColumnsList.add(otherColumnValuesMap);

		// for BE-CF
		otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("B" + PARENT_PATH_SEPARATOR + "BE", "BE");
		otherColumnValuesMap.put("C" + PARENT_PATH_SEPARATOR + "CF", "CF");
		expectedColumnsList.add(otherColumnValuesMap);

		Assert.assertTrue(compareResult(result, expectedColumnsList));
	}

	@Test
	public void testFlattenJsonForJsonArray() throws Exception {

		TargetModel target = new HdfsTargetModel("test6", hdfsExplorer);

		List<DataRecord> result = JsonAdapter.flattenJson(dataRecord, jsonArray, target);

		// creating the expected data [four data records should be there]
		List<Map<String, String>> expectedColumnsList = new ArrayList<>();
		Map<String, String> commonColumnValuesMap = new HashMap<>();
		commonColumnValuesMap.put("fp", "2017-09");
		commonColumnValuesMap.put("SR1", "Y");

		// for rt 1 item no 1
		Map<String, String> otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("rt", "1");
		otherColumnValuesMap.put("trnovr", "10000");
		otherColumnValuesMap.put("items" + PARENT_PATH_SEPARATOR + "no", "1");
		expectedColumnsList.add(otherColumnValuesMap);

		// for rt 1 item no 2
		otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("rt", "1");
		otherColumnValuesMap.put("trnovr", "10000");
		otherColumnValuesMap.put("items" + PARENT_PATH_SEPARATOR + "no", "2");
		expectedColumnsList.add(otherColumnValuesMap);

		// for rt 5 item no a
		otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("rt", "5");
		otherColumnValuesMap.put("trnovr", "20000");
		otherColumnValuesMap.put("items" + PARENT_PATH_SEPARATOR + "no", "a");
		expectedColumnsList.add(otherColumnValuesMap);

		// for rt 5 item no b
		otherColumnValuesMap = new HashMap<>();
		otherColumnValuesMap.putAll(commonColumnValuesMap);
		otherColumnValuesMap.put("rt", "5");
		otherColumnValuesMap.put("trnovr", "20000");
		otherColumnValuesMap.put("items" + PARENT_PATH_SEPARATOR + "no", "b");
		expectedColumnsList.add(otherColumnValuesMap);

		Assert.assertTrue(compareResult(result, expectedColumnsList));
	}

	@Test
	public void testFlattenJsonForAllMissingColumns() throws Exception {
		
		// trying to select non existing columns A>B -> AB1,AB2 | A>C -> AC |
		// A>D>E -> ADE

		TargetModel target = new HdfsTargetModel("test7", hdfsExplorer);

		List<DataRecord> result = JsonAdapter.flattenJson(dataRecord, nestedJson, target);

		// creating the expected data [one data record with only original
		// columns]
		List<Map<String, String>> expectedColumnsList = new ArrayList<>();
		Map<String, String> commonColumnValuesMap = new HashMap<>();
		commonColumnValuesMap.put("fp", "2017-09");
		commonColumnValuesMap.put("SR1", "Y");

		expectedColumnsList.add(commonColumnValuesMap);

		Assert.assertTrue(compareResult(result, expectedColumnsList));
	}

	private boolean compareResult(List<DataRecord> actual, List<Map<String, String>> expectedColumnsList)
			throws ColumnNotFoundException {

		Set<Integer> expectedMatchFound = new HashSet<>();
		for (int actualIndex = 0; actualIndex < actual.size(); actualIndex++) {
			DataRecord actualDataRecord = actual.get(actualIndex);
			Set<String> actualColumns = actualDataRecord.getAllColumnNames();
			int expectedIndex = 0;
			for (; expectedIndex < expectedColumnsList.size(); expectedIndex++) {

				if (!expectedMatchFound.contains(expectedIndex)) {
					Map<String, String> expectedColumnsValuesMap = expectedColumnsList.get(expectedIndex);
					Set<String> expectedColumns = expectedColumnsValuesMap.keySet();

					if (actualColumns.size() == expectedColumns.size()) {
						boolean result = compareDataRecord(actualDataRecord, expectedColumnsValuesMap);
						if (result) {
							expectedMatchFound.add(expectedIndex);
							break;
						}
					}
				}
			}
			if (expectedIndex == expectedColumnsList.size())
				return false;
		}
		return true;
	}

	private boolean compareDataRecord(DataRecord actual, Map<String, String> expectedColumnsValuesMap)
			throws ColumnNotFoundException {

		Set<String> actualColumns = actual.getAllColumnNames();
		Set<String> expectedColumns = expectedColumnsValuesMap.keySet();

		for (String actualColumn : actualColumns) {
			if (!expectedColumns.contains(actualColumn)) {
				return false;
			} else {
				String actualVal = actual.getColumnTupleValue(actualColumn);
				String expectedVal = expectedColumnsValuesMap.get(actualColumn);

				if (!actualVal.equals(expectedVal))
					return false;
			}
		}
		return true;
	}

	//main method for quick testing
	public static void main(String[] args) throws InvalidJSONException, ColumnFamilyNotFoundException, InvalidColumnException {
		
		HdfsFileExplorer hdfsExplorer = new HdfsFileExplorer("./src/test/resources/JsonAdapterTest.schema");
		
		TargetModel target = new HdfsTargetModel("test8", hdfsExplorer);
		
		//String json = "{\"A\":\"A1\",\"B\":{ \"C\":{\"C1\":\"C1val\",\"C2\":\"C2val\"} ,\"D\":{\"D1\":\"D1val\",\"D2\":\"D2val\"}}}";
		
		String json = "{\"A\":\"A1\",\"B\":{ \"C\":{\"C1\":\"C1val\",\"C2\":\"C2val\"} }}";
		
		System.out.println(JsonAdapter.flattenJson(new DataRecord(), json, target));
	}
}
