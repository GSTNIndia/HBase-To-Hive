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

import org.gstn.schemaexplorer.entity.DataRecord;

import com.google.gson.JsonElement;

public interface TargetModel {
	
	/** 
	 * This method restructures the input data record as per the target schema.
	 * @param dataRecord
	 * 			source dataRecord to be restructured
	 * @return	
	 * 			datarecord which is restructured as per the target schema
	 * @throws Exception
	 */
	DataRecord structureDataRecord(DataRecord dataRecord) throws Exception;
	
	/** 
	 * This method restructures the input delete data record as per the target schema.
	 * As the Delete record has only key columns, this method creates data record as 
	 * per the target key columns.
	 * 
	 * @param deleteRecord
	 * 			deleteRecord to be restructured
	 * @return	
	 * 			datarecord which is restructured as per the target schema
	 * @throws Exception
	 */
	DataRecord structureDeleteRecord(DataRecord deleteRecord) throws Exception;

	boolean checkKey(String parentPath, String currentPath, String key, JsonElement value);
}
