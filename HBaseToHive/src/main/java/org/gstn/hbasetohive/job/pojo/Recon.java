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
package org.gstn.hbasetohive.job.pojo;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Recon implements Serializable {

	private String reconColumn;
	private String reconOperation;
	
	public String getReconColumn() {
		return reconColumn;
	}
	
	public void setReconColumn(String reconColumn) {
		this.reconColumn = reconColumn;
	}
	
	public String getReconOperation() {
		return reconOperation;
	}
	
	public void setReconOperation(String reconOperation) {
		this.reconOperation = reconOperation;
	}
	
}
