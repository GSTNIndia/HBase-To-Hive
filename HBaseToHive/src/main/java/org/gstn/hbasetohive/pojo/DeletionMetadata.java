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

public class DeletionMetadata implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9089350353465939936L;

	private boolean deleteAllAdded;
	private boolean deleteSingleAdded;

	public DeletionMetadata(boolean deleteAllAdded, boolean deleteSingleAdded) {
		this.deleteAllAdded = deleteAllAdded;
		this.deleteSingleAdded = deleteSingleAdded;
	}

	public boolean isDeleteAllAdded() {
		return deleteAllAdded;
	}

	public boolean isDeleteSingleAdded() {
		return deleteSingleAdded;
	}
	
}
