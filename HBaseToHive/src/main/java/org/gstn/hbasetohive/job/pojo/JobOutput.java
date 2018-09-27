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

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.gstn.hbasetohive.entity.ReconEntity;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.gstn.schemaexplorer.hive.HiveTableExplorer;

public class JobOutput {
	
	private String appId;
	private String jobId;
	private JavaRDD<ReconEntity> reconRDD;
	private String hdfsFilePath;
	HiveTableExplorer hiveExplorer;
	private String targetSchema;
	private String sourceSchema;
	private boolean incremental;
	private boolean dynamicColumnsInSelect;
	
	public JobOutput(String appId, String jobId, JavaRDD<ReconEntity> reconRDD, String hdfsFilePath,
			HiveTableExplorer hiveExplorer, String targetSchema, String sourceSchema, boolean incremental, boolean dynamicColumnsInSelect) {
		this.appId = appId;
		this.jobId = jobId;
		this.reconRDD = reconRDD;
		this.hdfsFilePath = hdfsFilePath;
		this.hiveExplorer = hiveExplorer;
		this.targetSchema = targetSchema;
		this.sourceSchema = sourceSchema;
		this.incremental = incremental;
		this.dynamicColumnsInSelect = dynamicColumnsInSelect;
	}

	public String getAppId() {
		return appId;
	}

	public String getJobId() {
		return jobId;
	}

	public JavaRDD<ReconEntity> getReconRDD() {
		return reconRDD;
	}

	public String getHdfsFilePath() {
		return hdfsFilePath;
	}

	public HiveTableExplorer getHiveSchema() {
		return hiveExplorer;
	}

	public String getTargetSchema() {
		return targetSchema;
	}

	public String getSourceSchema() {
		return sourceSchema;
	}

	public boolean isDynamicColumnsInSelect() {
		return dynamicColumnsInSelect;
	}

	public void createHiveScript(String hdfsURL, String hdfsBasePath, HBaseTableExplorer hBaseTableExplorer, ReconEntity reducedReconEntity, Map<String, String> sparkConfMap) throws InvalidSchemaException {
		hiveExplorer.createHiveScript(targetSchema, hdfsURL, hdfsFilePath, hdfsBasePath, sparkConfMap, jobId,
				hBaseTableExplorer, sourceSchema, reducedReconEntity.isInsertAdded(),
				reducedReconEntity.isDeleteAllAdded(), reducedReconEntity.isDeleteSingleAdded(),
				dynamicColumnsInSelect);
	}

	public boolean isIncremental() {
		return incremental;
	}
}
