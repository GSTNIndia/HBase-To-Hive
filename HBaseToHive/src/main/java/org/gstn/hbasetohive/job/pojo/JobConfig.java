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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.gstn.hbasetohive.exception.ValidationException;

@SuppressWarnings("serial")
@XmlRootElement
public class JobConfig implements Serializable {

	private String target;

	private String loadType;

	private Long minTimeStamp;
	private Long maxTimeStamp;

	private Jobs jobs;

	@XmlElement
	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	@XmlElement
	public String getLoadType() {
		return loadType;
	}

	public void setLoadType(String loadType) {
		this.loadType = loadType;
	}

	@XmlElement
	public Jobs getJobs() {
		return jobs;
	}

	public void setJobs(Jobs jobs) {
		this.jobs = jobs;
	}

	@XmlElement
	public Long getMinTimeStamp() {
		return minTimeStamp;
	}

	public void setMinTimeStamp(Long minTimeStamp) {
		this.minTimeStamp = minTimeStamp;
	}

	@XmlElement
	public Long getMaxTimeStamp() {
		return maxTimeStamp;
	}

	public void setMaxTimeStamp(Long maxTimeStamp) {
		this.maxTimeStamp = maxTimeStamp;
	}

	public String getSourceSchemaName(int jobId) {
		return jobs.getSourceSchemaName(jobId);
	}

	public String getTargetSchemaName(int jobId) {
		return jobs.getTargetSchemaName(jobId);
	}

	public int getNumberOfJobs() {
		return jobs.getNumberOfJobs();
	}

	public Map<String, List<String>> getReconColumnOpMap(int jobId) {
		return jobs.getReconColumnOpMap(jobId);
	}

	public String getQuery(int jobId) {
		return jobs.getQuery(jobId);
	}

	public void validate(String jcFilePath) throws ValidationException {

		if (target == null) {
			throw new ValidationException("<target> not specified in config: " + jcFilePath);
		}

		if (target.isEmpty()) {
			throw new ValidationException("Please specify a valid string in <target> in config file: " + jcFilePath);
		}

		if (loadType == null || (!loadType.equalsIgnoreCase("full") && !loadType.equalsIgnoreCase("incremental")
				&& !loadType.equalsIgnoreCase("partial"))) {
			throw new ValidationException("Please specify a valid <loadType> in config file: " + jcFilePath);
		} else if (loadType.equalsIgnoreCase("partial")) {
			
			if (minTimeStamp == null && maxTimeStamp == null) {
				throw new ValidationException(
						"Please specify at least one long value for <minTimeStamp> or <maxTimeStamp> in config file: "
								+ jcFilePath + ", for partial load to work");
			}

			if (minTimeStamp != null && maxTimeStamp != null) {
				if (minTimeStamp >= maxTimeStamp) {
					throw new ValidationException(
							"<minTimeStamp> should be strictly less than <maxTimeStamp> in config file: " + jcFilePath);
				}
			}
		}

		if (jobs == null) {
			throw new ValidationException("<jobs> not specified in config file: " + jcFilePath);
		} else {
			jobs.validate(jcFilePath);
		}
	}

}
