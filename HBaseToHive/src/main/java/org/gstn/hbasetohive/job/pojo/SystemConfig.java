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
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.gstn.hbasetohive.exception.ValidationException;

@SuppressWarnings("serial")
@XmlRootElement
public class SystemConfig implements Serializable {

	private String sparkMaster;
	private String driverMemory;
	private String executorMemory;
	private String initExecutors;
	private String minExecutors;
	private String maxExecutors;
	private String driverCores;
	private String executorCores;
	private String numberOfPartitions;
	private String runJobsParallelly;

	private String sourceHBaseZk;

	private String targetHBaseZk;

	private String activeCluster;

	private String jobExecutionCluster;
	
	private HdfsTargetProperties hdfsTargetProperties;

	private TimestampTable timestampTable;

	@XmlElement(name = "spark.master")
	public String getSparkMaster() {
		return sparkMaster;
	}

	public void setSparkMaster(String sparkMaster) {
		this.sparkMaster = sparkMaster;
	}

	@XmlElement(name = "spark.driver.memory")
	public String getDriverMemory() {
		return driverMemory;
	}

	public void setDriverMemory(String driverMemory) {
		this.driverMemory = driverMemory;
	}

	@XmlElement(name = "spark.executor.memory")
	public String getExecutorMemory() {
		return executorMemory;
	}

	public void setExecutorMemory(String executorMemory) {
		this.executorMemory = executorMemory;
	}

	@XmlElement(name = "spark.dynamicAllocation.initialExecutors")
	public String getInitExecutors() {
		return initExecutors;
	}

	public void setInitExecutors(String initExecutors) {
		this.initExecutors = initExecutors;
	}

	@XmlElement(name = "spark.dynamicAllocation.minExecutors")
	public String getMinExecutors() {
		return minExecutors;
	}

	public void setMinExecutors(String minExecutors) {
		this.minExecutors = minExecutors;
	}

	@XmlElement(name = "spark.dynamicAllocation.maxExecutors")
	public String getMaxExecutors() {
		return maxExecutors;
	}

	public void setMaxExecutors(String maxExecutors) {
		this.maxExecutors = maxExecutors;
	}

	@XmlElement(name = "spark.driver.cores")
	public String getDriverCores() {
		return driverCores;
	}

	public void setDriverCores(String cores) {
		this.driverCores = cores;
	}

	@XmlElement(name = "spark.executor.cores")
	public String getExecutorCores() {
		return executorCores;
	}

	public void setExecutorsCores(String executorsCores) {
		this.executorCores = executorsCores;
	}

	@XmlElement
	public String getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public void setNumberOfPartitions(String numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}

	public int getPartitions() {
		return Integer.parseInt(numberOfPartitions);
	}

	@XmlElement
	public String getRunJobsParallelly() {
		return runJobsParallelly;
	}

	public void setRunJobsParallelly(String runJobsParallelly) {
		this.runJobsParallelly = runJobsParallelly;
	}

	@XmlElement
	public String getSourceHBaseZk() {
		return sourceHBaseZk;
	}

	public void setSourceHBaseZk(String sourceHBaseZk) {
		this.sourceHBaseZk = sourceHBaseZk;
	}

	@XmlElement
	public String getTargetHBaseZk() {
		return targetHBaseZk;
	}

	public void setTargetHBaseZk(String targetHBaseZk) {
		this.targetHBaseZk = targetHBaseZk;
	}

	@XmlElement
	public String getActiveCluster() {
		return activeCluster;
	}

	public void setActiveCluster(String activeCluster) {
		this.activeCluster = activeCluster;
	}

	@XmlElement
	public String getJobExecutionCluster() {
		return jobExecutionCluster;
	}

	public void setJobExecutionCluster(String jobExecutionCluster) {
		this.jobExecutionCluster = jobExecutionCluster;
	}
	
	@XmlElement
	public HdfsTargetProperties getHdfsTargetProperties() {
		return hdfsTargetProperties;
	}

	public void setHdfsTargetProperties(HdfsTargetProperties hdfsTargetProperties) {
		this.hdfsTargetProperties = hdfsTargetProperties;
	}

	@XmlElement
	public TimestampTable getTimestampTable() {
		return timestampTable;
	}

	public void setTimestampTable(TimestampTable timestampTable) {
		this.timestampTable = timestampTable;
	}

	public Map<String, String> getSparkConfigurations() throws ValidationException {

		if (initExecutors == null && minExecutors != null) {
			setInitExecutors(minExecutors);
		}

		if (initExecutors != null && minExecutors == null) {
			setMinExecutors(initExecutors);
		}

		if (initExecutors != null && minExecutors != null) {
			if (Integer.parseInt(initExecutors) < Integer.parseInt(minExecutors)) {
				throw new ValidationException(
						"spark.dynamicAllocation.initialExecutors cannot be less than spark.dynamicAllocation.minExecutors");
			}
		}

		if (maxExecutors != null && minExecutors != null) {
			if (Integer.parseInt(maxExecutors) < Integer.parseInt(minExecutors)) {
				throw new ValidationException(
						"spark.dynamicAllocation.maxExecutors cannot be less than spark.dynamicAllocation.minExecutors");
			}
		}

		Map<String, String> confValueMap = new HashMap<>();

		if (sparkMaster != null) {
			confValueMap.put("spark.master", sparkMaster);
		}

		if (driverMemory != null) {
			confValueMap.put("spark.driver.memory", driverMemory);
		}

		if (executorMemory != null) {
			confValueMap.put("spark.executor.memory", executorMemory);
		}

		if (initExecutors != null) {
			confValueMap.put("spark.dynamicAllocation.initialExecutors", initExecutors);
		}

		if (minExecutors != null) {
			confValueMap.put("spark.dynamicAllocation.minExecutors", minExecutors);
		}

		if (maxExecutors != null) {
			confValueMap.put("spark.dynamicAllocation.maxExecutors", maxExecutors);
		}

		if (driverCores != null) {
			confValueMap.put("spark.driver.cores", driverCores);
		}

		if (executorCores != null) {
			confValueMap.put("spark.executor.cores", executorCores);
		}

		return confValueMap;
	}

	public String getClusterTimestampTable() {
		return activeCluster.toUpperCase() + "_DELETED_TRANSACTIONS";
	}
	
	public String getHdfsBasePath() {
		return hdfsTargetProperties.getHdfsBasePath();
	}

	public String getDestHdfsUrl() {
		return hdfsTargetProperties.getDestHdfsUrl();
	}

	public void validate(String scFilePath, String target, String loadType) throws ValidationException {

		if (sparkMaster == null) {
			throw new ValidationException("spark.master tag not specified in the config file: " + scFilePath);
		}

		if (numberOfPartitions != null) {
			try {
				Integer.parseInt(numberOfPartitions);
			} catch (NumberFormatException e) {
				throw new ValidationException(
						"Please specify an integer value for <numberOfPartitions> in config file: " + scFilePath);
			}
		}

		if (runJobsParallelly != null && !(runJobsParallelly.equals("1") || runJobsParallelly.equals("0")
				|| runJobsParallelly.equalsIgnoreCase("true") || runJobsParallelly.equalsIgnoreCase("false"))) {
			throw new ValidationException(
					"Please specify a boolean value for <runJobsParallelly> in config file: " + scFilePath);
		}

		if (loadType.equalsIgnoreCase("incremental")) {
			if (timestampTable == null) {
				throw new ValidationException(
						"Please specify <timestampTable>, with all of <tableName>, <columnFamily> and"
								+ " <columnName> inside it, in config file: " + scFilePath
								+ ", for incremental load to work");
			} else {
				timestampTable.validate(scFilePath);
			}
		}

		if (sourceHBaseZk == null) {
			throw new ValidationException("Please specify <sourceHBaseZk> in config file: " + scFilePath);
		}

		if (sourceHBaseZk.isEmpty()) {
			throw new ValidationException(
					"Please specify valid zookeeper quorum in <sourceHBaseZk> in config file: " + scFilePath);
		}

		if (activeCluster != null) {
			if (!(activeCluster.equalsIgnoreCase("DC1") || activeCluster.equalsIgnoreCase("DC2"))) {
				throw new ValidationException(
						"<activeCluster> should be either DC1 or DC2, in config file: " + scFilePath);
			}
		}

		if (jobExecutionCluster != null) {
			if (!(jobExecutionCluster.equalsIgnoreCase("DC1") || jobExecutionCluster.equalsIgnoreCase("DC2"))) {
				throw new ValidationException(
						"<jobExecutionCluster> should be either DC1 or DC2, in config file: " + scFilePath);
			}
		}
		
		if (target.equalsIgnoreCase("HBase")) {
			if (targetHBaseZk == null) {
				throw new ValidationException("Please specify <targetHBaseZk> in config file: " + scFilePath);
			}
			if (targetHBaseZk.isEmpty()) {
				throw new ValidationException(
						"Please specify valid zookeeper quorum in <targetHBaseZk> in config file: " + scFilePath);
			}
		} else if (target.equalsIgnoreCase("HDFS") || target.equalsIgnoreCase("Hive")) {
			if (hdfsTargetProperties == null) {
				throw new ValidationException(
						"Please specify <hdfsTargetProperties>, with <destHdfsUrl> and <hdfsBasePath>"
								+ " inside it in config file: " + scFilePath);
			} else {
				hdfsTargetProperties.validate(scFilePath);
			}
		} else {
			throw new ValidationException(
					"Invalid target: " + target + ". Please specify one of HBase, HDFS or Hive as target");
		}
	}

	public boolean runJobsParallely() {
		if (runJobsParallelly == null) {
			return true;
		} else if (runJobsParallelly.equalsIgnoreCase("true") || runJobsParallelly.equals("1")) {
			return true;
		}
		return false;
	}

	public String getTableName() {
		return timestampTable.getTableName();
	}

	public String getColumnFamily() {
		return timestampTable.getColumnFamily();
	}

	public String getColumnName() {
		return timestampTable.getColumnName();
	}

	public boolean checkTimestampTable(String loadType) {
		if(loadType.equalsIgnoreCase("full") || loadType.equalsIgnoreCase("incremental")) {
			return timestampTable != null && timestampTable.getTableName() != null;
		}
		return false;
	}

}
