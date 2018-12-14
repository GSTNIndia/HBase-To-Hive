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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.entity.Tuple;
import org.gstn.schemaexplorer.exception.ColumnFamilyNotFoundException;
import org.gstn.schemaexplorer.exception.InvalidColumnException;
import org.gstn.schemaexplorer.exception.InvalidJSONException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * This class is used to fetch attributes from the JSON columns as required in
 * target.
 */
public class JsonAdapter {

	private static JsonParser parser = new JsonParser();

	/**
	 * This method flattons the input json for json columns mentioned in the
	 * target schema. By adding each row of the flattened json into input
	 * datarecord, it creates list of data records to be written into target.
	 * 
	 * @param dataRecord
	 *            input data record having columns added so far
	 * @param json
	 *            string json to be flattened
	 * @param targetModel
	 *            map of parent path vs. list of columns under that parent path
	 *            which are required in the target
	 * @return list of data records to be written into target.
	 * @throws InvalidJSONException
	 * @throws ColumnFamilyNotFoundException
	 * @throws InvalidColumnException
	 */
	public static List<DataRecord> flattenJson(DataRecord dataRecord, String json, TargetModel targetModel)
			throws InvalidJSONException, ColumnFamilyNotFoundException, InvalidColumnException {

		List<DataRecord> dataRecordList = new ArrayList<>();
		dataRecordList.add(dataRecord);

		boolean useParentKey = true;

		JsonElement jsonElement = parser.parse(json);

		if (jsonElement instanceof JsonObject) {
			return flattenJson(dataRecordList, (JsonObject) jsonElement, useParentKey, null, targetModel)
					.getDataRecordList();
		} else if (jsonElement instanceof JsonArray) {
			List<DataRecord> finalDataRecordList = new ArrayList<>();

			for (JsonElement jsonElement1 : jsonElement.getAsJsonArray()) {
				finalDataRecordList.addAll(flattenJson(dataRecord, jsonElement1.toString(), targetModel));
			}
			return finalDataRecordList;
		} else {
			throw new InvalidJSONException("Expected a JsonObject, but found " + jsonElement.getClass());
		}

	}

	/**
	 * @see #flattenJson(DataRecord, String, Map)
	 */
	private static Result flattenJson(List<DataRecord> dataRecordList, JsonObject object, boolean useParentKey,
			String parentPath, TargetModel targetModel)
			throws InvalidJSONException, ColumnFamilyNotFoundException, InvalidColumnException {

		boolean modifiedRecordList = false;

		Set<Map.Entry<String, JsonElement>> set = object.entrySet();
		Iterator<Map.Entry<String, JsonElement>> iterator = set.iterator();

		List<JsonArrayWrapper> jsonArrayList = new ArrayList<>();

		// we process all the nonarray json elements and add json array elements
		// to a list to be processed in the end
		while (iterator.hasNext()) {

			Map.Entry<String, JsonElement> entry = iterator.next();
			String key = entry.getKey();
			JsonElement value = entry.getValue();
			String currentPath;
			if (useParentKey == true) {
				currentPath = parentPath == null ? key : parentPath + PARENT_PATH_SEPARATOR + key;
			} else {
				currentPath = "";
			}

			boolean isKeyRequired = true;
			if (useParentKey == true) {
				// we check if this key is required
				isKeyRequired = targetModel.checkKey(parentPath, currentPath, key, value);
			}

			if (isKeyRequired) {

				if (null != value) {
					if (!value.isJsonPrimitive()) {
						if (value.isJsonObject()) {
							Result result = flattenJson(dataRecordList, (JsonObject) value, useParentKey, currentPath,
									targetModel);
							if (result.isModifiedRecordList()) {
								dataRecordList = result.getDataRecordList();
								modifiedRecordList = true;
							}
						} else if (value.isJsonArray() && value.toString().contains(":")) {

							JsonArray jsonArray = value.getAsJsonArray();
							if (null != jsonArray) {
								// value is array of jsons, so each element
								// within value array will have key so we are
								// passing currentPath as parentPath
								jsonArrayList.add(new JsonArrayWrapper(jsonArray, currentPath));
							}

						} else if (value.isJsonArray() && !value.toString().contains(":")) {
							// create jsonarray with key,value pair and add to
							// list. key will be name of the array field
							JsonArray array = value.getAsJsonArray();
							JsonArray array2 = new JsonArray();
							if (null != array) {
								for (JsonElement element : array) {
									JsonObject obj = new JsonObject();
									obj.addProperty(key, element.getAsString());
									array2.add(obj);
								}
								// value is array of non json values, so the
								// elements within value array will not have any
								// key.
								// so we have used current key as the key for
								// elements and so we are passing parentPath as
								// parentPath instead of current path
								jsonArrayList.add(new JsonArrayWrapper(array2, parentPath));
							}

						}
					} else {
						if (dataRecordList != null) {
							modifiedRecordList = true;
							if (useParentKey == true) {
								Class dataType = targetModel.getSchemaJsonColumns().get(currentPath);
								addTupleIntoDataRecords(dataRecordList,
										Tuple.staticColumn("", currentPath, value.getAsString(), dataType));
							} else {
								addTupleIntoDataRecords(dataRecordList, Tuple.rowkeyColumn(key, value.getAsString()));
							}
						}
					}
				}
			}
		}

		if (!jsonArrayList.isEmpty()) {
			for (int i = 0; i < jsonArrayList.size(); i++) {
				List<DataRecord> jsonArrayDataRecordList = new ArrayList<>();

				JsonArrayWrapper jsonArrayWrapper = jsonArrayList.get(i);

				if (jsonArrayWrapper.getJsonArray() != null) {
					for (JsonElement element : jsonArrayWrapper.getJsonArray()) {
						DataRecord blankDataRecord = new DataRecord();
						List<DataRecord> list = new ArrayList<>();
						list.add(blankDataRecord);
						Result result = flattenJson(list, (JsonObject) element, useParentKey,
								jsonArrayWrapper.getParentPath(), targetModel);
						if (result.isModifiedRecordList()) {
							list = result.getDataRecordList();
							jsonArrayDataRecordList.addAll(list);
						}
					}

					if (!jsonArrayDataRecordList.isEmpty()) {
						modifiedRecordList = true;
						dataRecordList = mergeDataRecordLists(dataRecordList, jsonArrayDataRecordList);
					}
				}

			}
		}

		return new Result(dataRecordList, modifiedRecordList);
	}

	/**
	 * Adds tuple into each of the DataRecord present in the list
	 * 
	 * @param dataRecordList
	 *            list of DataRecord
	 * @param tuple
	 *            Tuple to be added
	 * @throws InvalidColumnException
	 */
	private static void addTupleIntoDataRecords(List<DataRecord> dataRecordList, Tuple tuple)
			throws InvalidColumnException {
		for (DataRecord dataRecord : dataRecordList) {
			dataRecord.addTupleToColumn("", tuple.getColumnName(), tuple);
		}
	}

	/**
	 * @param dataRecordList1
	 *            List of DataRecord
	 * @param dataRecordList2
	 *            List of DataRecord
	 * @return List of DataRecords by merging each of the dataRecord from
	 *         dataRecordList1 into each of the dataRecord in dataRecordList2
	 * @throws ColumnFamilyNotFoundException
	 * @throws InvalidColumnException
	 */
	private static List<DataRecord> mergeDataRecordLists(List<DataRecord> dataRecordList1,
			List<DataRecord> dataRecordList2) throws ColumnFamilyNotFoundException, InvalidColumnException {
		List<DataRecord> mergedDataRecords = new ArrayList<>();

		if (dataRecordList1.isEmpty() && !dataRecordList2.isEmpty()) {
			return dataRecordList2;
		} else if (!dataRecordList1.isEmpty() && dataRecordList2.isEmpty()) {
			return dataRecordList1;
		} else if (dataRecordList1.isEmpty() && dataRecordList2.isEmpty()) {
			return dataRecordList1;
		} else {

			for (DataRecord dataRecord1 : dataRecordList1) {
				for (DataRecord dataRecord2 : dataRecordList2) {
					DataRecord dataRecord1Copy = dataRecord1.duplicate();

					for (Tuple tuple : dataRecord2.getTuplesInCF("")) {
						dataRecord1Copy.addTupleToColumn("", tuple.getColumnName(), tuple);
					}

					mergedDataRecords.add(dataRecord1Copy);
				}
			}
		}

		return mergedDataRecords;
	}

}

/**
 * Wrapper class for storing JsonArray and it's parent path (i.e. path of it's
 * parent in the input json)
 *
 */
class JsonArrayWrapper {
	
	private JsonArray jsonArray;
	private String parentPath;

	public JsonArrayWrapper(JsonArray jsonArray, String parentPath) {
		this.jsonArray = jsonArray;
		this.parentPath = parentPath;
	}

	public JsonArray getJsonArray() {
		return jsonArray;
	}

	public String getParentPath() {
		return parentPath;
	}

}

class Result {

	private List<DataRecord> dataRecordList;
	private boolean modifiedRecordList;

	public Result(List<DataRecord> dataRecordList, boolean modifiedRecordList) {
		this.dataRecordList = dataRecordList;
		this.modifiedRecordList = modifiedRecordList;
	}

	public List<DataRecord> getDataRecordList() {
		return dataRecordList;
	}

	public boolean isModifiedRecordList() {
		return modifiedRecordList;
	}

}
