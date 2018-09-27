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
import java.util.NavigableMap;

public class ProcessMutationResult implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4999525661158762412L;
	
	private NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap;
	private boolean reprocessRow;

	public ProcessMutationResult(NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap,
			boolean reprocessRow) {
		this.noVersionMap = noVersionMap;
		this.reprocessRow = reprocessRow;
	}

	public NavigableMap<byte[], NavigableMap<byte[], byte[]>> getNoVersionMap() {
		return noVersionMap;
	}

	public void setNoVersionMap(NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap) {
		this.noVersionMap = noVersionMap;
	}

	public boolean isReprocessRow() {
		return reprocessRow;
	}

	public void setReprocessRow(boolean reprocessRow) {
		this.reprocessRow = reprocessRow;
	}

}
