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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gstn.hbasetohive.exception.ValidationException;

@SuppressWarnings("serial")
public class Job implements Serializable {
	
	private String selectColumns;
	private String sourceSchemaName;
	private String conditions;
	private String targetSchemaName;
	private ReconList reconList;
		
	public String getTargetSchemaName() {
		return targetSchemaName;
	}
	
	public void setTargetSchemaName(String targetSchemaName) {
		this.targetSchemaName = targetSchemaName;
	}
	
	public String getSelectColumns() {
		return selectColumns;
	}
	
	public void setSelectColumns(String selectColumns) {
		this.selectColumns = selectColumns;
	}

	public ReconList getReconList() {
		return reconList;
	}
	
	public void setReconList(ReconList reconList) {
		this.reconList = reconList;
	}

	public String getSourceSchemaName() {
		return sourceSchemaName;
	}

	public void setSourceSchemaName(String sourceSchemaName) {
		this.sourceSchemaName = sourceSchemaName;
	}

	public String getConditions() {
		return conditions;
	}

	public void setConditions(String conditions) {
		this.conditions = conditions;
	}
	
	public String getQuery() {
		String query = "select " + selectColumns + " from " + sourceSchemaName;
		if(conditions !=null && !conditions.equals("")){
			query += " where " + conditions;
		}
		return query;
	}

	public Map<String, List<String>> getReconColumnOpMap() {
		if(reconList != null) {
			return reconList.getReconColumnOpMap();
		} else {
			return new HashMap<String, List<String>>();
		}
	}

	public void validateRecon(int counter, String jcFilePath) throws ValidationException {
		if(reconList != null) {
			reconList.validate(counter, jcFilePath);
		}
	}
	
}
