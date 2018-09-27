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
import java.util.List;
import java.util.Map;

import org.gstn.hbasetohive.exception.ValidationException;

@SuppressWarnings("serial")
public class Jobs implements Serializable {
	
	private List<Job> job;

	public List<Job> getJob() {
		return job;
	}

	public void setJob(List<Job> job) {
		this.job = job;
	}

	public String getSourceSchemaName(int jobId) {
		return job.get(jobId).getSourceSchemaName();
	}

	public String getTargetSchemaName(int jobId) {
		return job.get(jobId).getTargetSchemaName();
	}

	public int getNumberOfJobs() {
		return job.size();
	}

	public Map<String, List<String>> getReconColumnOpMap(int jobId) {
		return job.get(jobId).getReconColumnOpMap();
	}

	public String getQuery(int jobId) {
		return job.get(jobId).getQuery();
	}
	
	public void validate(String jcFilePath) throws ValidationException {

		if (job != null) {
			int counter = 1;
			for (Job job : job) {
				if (job.getTargetSchemaName() == null) {
					throw new ValidationException("<targetSchemaName> not specified inside job(" + counter
							+ ")tag in config file: " + jcFilePath);
				}
				if (job.getTargetSchemaName().isEmpty()) {
					throw new ValidationException("Please specify a valid string in <targetSchemaName> inside job(" + counter
							+ ")tag in config file: " + jcFilePath);
				}
				if (job.getSelectColumns() == null) {
					throw new ValidationException("<selectColumns> not specified inside job(" + counter
							+ ")tag in config file: " + jcFilePath);
				}
				if (job.getSelectColumns().isEmpty()) {
					throw new ValidationException("Please specify a valid string in <selectColumns> inside job(" + counter
							+ ")tag in config file: " + jcFilePath);
				}
				if (job.getSourceSchemaName() == null) {
					throw new ValidationException("<sourceSchemaName> not specified inside job(" + counter
							+ ")tag in config file: " + jcFilePath);
				}
				if (job.getSourceSchemaName().isEmpty()) {
					throw new ValidationException("Please specify a valid string in <sourceSchemaName> inside job(" + counter
							+ ")tag in config file: " + jcFilePath);
				}
				job.validateRecon(counter, jcFilePath);
				counter++;
			}
		} else {
			throw new ValidationException("Please provide at leat one <job> in config file: " + jcFilePath);
		}
	}

}
