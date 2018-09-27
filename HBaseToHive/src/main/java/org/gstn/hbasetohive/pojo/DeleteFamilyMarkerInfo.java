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
package org.gstn.hbasetohive.pojo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DeleteFamilyMarkerInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -362652335910340578L;

	private boolean deleteFamilyMarkerFound = false;
	private boolean deleteFamilyMarkerFoundForAll = false;
	private Map<String, Long> deleteFamilyTimestamp = new HashMap<>();

	public DeleteFamilyMarkerInfo(boolean deleteFamilyMarkerFound, boolean deleteFamilyMarkerFoundForAll,
			Map<String, Long> deleteFamilyTimestamp) {
		super();
		this.deleteFamilyMarkerFound = deleteFamilyMarkerFound;
		this.deleteFamilyMarkerFoundForAll = deleteFamilyMarkerFoundForAll;
		this.deleteFamilyTimestamp = deleteFamilyTimestamp;
	}

	public boolean isDeleteFamilyMarkerFound() {
		return deleteFamilyMarkerFound;
	}

	public boolean isDeleteFamilyMarkerFoundForAll() {
		return deleteFamilyMarkerFoundForAll;
	}

	public Long getDeleteFamilyTimestamp(String columnFamily) {
		return deleteFamilyTimestamp.get(columnFamily);
	}

}
