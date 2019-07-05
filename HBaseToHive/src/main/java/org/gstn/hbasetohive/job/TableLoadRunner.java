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
package org.gstn.hbasetohive.job;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.gstn.hbasetohive.entity.ReconEntity;
import org.gstn.hbasetohive.exception.ValidationException;
import org.gstn.hbasetohive.job.pojo.JobConfig;
import org.gstn.hbasetohive.job.pojo.JobOutput;
import org.gstn.hbasetohive.job.pojo.SystemConfig;
import org.gstn.hbasetohive.util.ConfigUtil;
import org.gstn.hbasetohive.util.TimeStampUtil;
import org.gstn.schemaexplorer.exception.ColumnNotFoundException;
import org.gstn.schemaexplorer.exception.HQLException;
import org.gstn.schemaexplorer.exception.InvalidConfigFileException;
import org.gstn.schemaexplorer.exception.InvalidReconColumnException;
import org.gstn.schemaexplorer.exception.InvalidReconOperationException;
import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;

public class TableLoadRunner {

	public static void main(String[] args) {

		if (args.length != 4) {
			System.out.println("Please provide system and job config files (.xml) and path of schema files "
					+ "for source and target as argument in mentioned order!");
			return;
		}

		long startTime = System.currentTimeMillis();

		String systemConfigFilePath = args[0];
		String jobConfigFilePath = args[1];
		String sourceSchemaFilePath = args[2];
		String targetSchemaFilePath = args[3];

		JavaSparkContext jsc = null;

		try {
			SystemConfig systemConfig = generateSystemConfig(systemConfigFilePath);
			JobConfig jobConfig = generateJobConfig(jobConfigFilePath);

			String target = jobConfig.getTarget();
			String loadType = jobConfig.getLoadType();

			jobConfig.validate(jobConfigFilePath);
			systemConfig.validate(systemConfigFilePath, target, loadType);

			// flag to indicate if current time stamp has to be written to the
			// HBase table storing time stamp details
			boolean writeTimestampFlag = systemConfig.checkTimestampTable(loadType);

			// creating spark context
			SparkConf sc = new SparkConf().setAppName("HbaseToHive Job");

			Map<String, String> sparkConfMap = systemConfig.getSparkConfigurations();

			for (String conf : sparkConfMap.keySet()) {
				sc.set(conf, sparkConfMap.get(conf));
			}

			jsc = new JavaSparkContext(sc);

			// Read all the hbase schemas
			HBaseTableExplorer hBaseExplorer = new HBaseTableExplorer(sourceSchemaFilePath);

			long maxTimestamp = startTime;

			String activeCluster = systemConfig.getActiveCluster();
			String jobCluster = systemConfig.getJobExecutionCluster();

			if (activeCluster != null && jobCluster != null && !activeCluster.equals(jobCluster)) {
				maxTimestamp = TimeStampUtil.readClusterTimestamp(systemConfig.getClusterTimestampTable(),
						systemConfig.getSourceHBaseZk());
			}
			List<JobOutput> jobOutputList = createJobs(jsc, hBaseExplorer, systemConfig, jobConfig,
					targetSchemaFilePath, maxTimestamp, loadType);

			Map<String, ReconEntity> reconMap = new HashMap<>();

			if (systemConfig.runJobsParallely() == true) {
				jobOutputList.parallelStream().forEach(jobOutput -> reconMap.putAll(reduceRDD(jobOutput)));
			} else {
				jobOutputList.stream().forEach(jobOutput -> reconMap.putAll(reduceRDD(jobOutput)));
			}

			if (target.equals("hive")) {
				createHiveScripts(jobOutputList, systemConfig, hBaseExplorer, reconMap, sparkConfMap);
			}

			if (writeTimestampFlag == true) {
				writeTimeStampToHBase(systemConfig, jobConfig, maxTimestamp);
			}

			printJobDetails(jobOutputList, reconMap, systemConfig, jobConfig);

		} catch (Exception e) {
			System.err.println("Error occurred: " + e);
			e.printStackTrace();
			System.exit(1);
		} finally {
			if (jsc != null) {
				jsc.close();
			}
		}

		long endTime = System.currentTimeMillis();
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n****************************************************************************");
		System.out.println("****************************************************************************\n");
		System.out.println("Total execution time is " + formatter.format((endTime - startTime) / 1000d) + " seconds");
		System.out.print("\n****************************************************************************");
		System.out.println("****************************************************************************\n");
	}

	private static SystemConfig generateSystemConfig(String systemConfigFilePath)
			throws InvalidConfigFileException, IOException {

		SystemConfig systemConfig = null;
		try {
			systemConfig = ConfigUtil.getSystemConfig(systemConfigFilePath);
		} catch (JAXBException e) {
			System.out.println("Error while parsing " + systemConfigFilePath + ": " + e);
			throw new InvalidConfigFileException(e.getMessage());
		} catch (IOException e) {
			System.out.println("IOException occurred while parsing " + systemConfigFilePath + ": " + e);
			throw e;
		}
		return systemConfig;
	}

	private static JobConfig generateJobConfig(String jobConfigFilePath)
			throws InvalidConfigFileException, IOException {

		JobConfig jobConfig = null;
		try {
			jobConfig = ConfigUtil.getJobConfig(jobConfigFilePath);
		} catch (JAXBException e) {
			System.out.println("Error while parsing " + jobConfigFilePath + ": " + e);
			throw new InvalidConfigFileException(e.getMessage());
		} catch (IOException e) {
			System.out.println("IOException occurred while parsing " + jobConfigFilePath + ": " + e);
			throw e;
		}
		return jobConfig;
	}

	private static List<JobOutput> createJobs(JavaSparkContext jsc, HBaseTableExplorer hbaseExplorer,
			SystemConfig systemConfig, JobConfig jobConfig, String tsFilePath, long maxTimestamp, String loadType)
			throws ValidationException, IOException, HQLException, ColumnNotFoundException, InvalidReconColumnException,
			InvalidReconOperationException, InvalidSchemaException {

		List<JobOutput> jobOutputList = new ArrayList<>();

		String query;
		String sourceSchema;
		String targetSchema;
		String jobId;
		String appId = jsc.sc().applicationId();

		Map<String, List<String>> reconColumnOpMap = null;

		for (int i = 0; i < jobConfig.getNumberOfJobs(); i++) {

			jobId = "job" + (i + 1);
			reconColumnOpMap = jobConfig.getReconColumnOpMap(i);
			query = jobConfig.getQuery(i);
			targetSchema = jobConfig.getTargetSchemaName(i);
			sourceSchema = jobConfig.getSourceSchemaName(i);

			if (hbaseExplorer.isSchemaDefined(sourceSchema) == false) {
				throw new ValidationException("Schema definition not found for source schema: " + targetSchema);
			}

			JobOutput jobOutput = JobUtil.runJob(jsc, query, sourceSchema, targetSchema, appId, jobId, systemConfig,
					jobConfig, hbaseExplorer, tsFilePath, reconColumnOpMap, maxTimestamp, loadType);

			jobOutputList.add(jobOutput);
		}
		return jobOutputList;
	}

	private static Map<String, ReconEntity> reduceRDD(JobOutput jobOutput) {

		Map<String, ReconEntity> map = new HashMap<>();

		ReconEntity reconEntity = jobOutput.getReconRDD().reduce((x, y) -> {
			x.addReconEntity(y);
			return x;
		});
		map.put(jobOutput.getJobId(), reconEntity);

		return map;
	}

	private static void createHiveScripts(List<JobOutput> jobOutputList, SystemConfig systemConfig,
			HBaseTableExplorer hBaseTableExplorer, Map<String, ReconEntity> reconMap, Map<String, String> sparkConfMap)
			throws InvalidSchemaException {

		for (JobOutput jobOutput : jobOutputList) {
			ReconEntity reducedReconEntity = reconMap.get(jobOutput.getJobId());
			jobOutput.createHiveScript(systemConfig.getDestHdfsUrl(), systemConfig.getHdfsBasePath(),
					hBaseTableExplorer, reducedReconEntity, sparkConfMap);
		}
	}

	private static void writeTimeStampToHBase(SystemConfig systemConfig, JobConfig jobConfig,
			long maxTimestamp) throws IOException {

		for (int i = 0; i < jobConfig.getNumberOfJobs(); i++) {
			String sourceSchema = jobConfig.getSourceSchemaName(i);
			String targetSchema = jobConfig.getTargetSchemaName(i);

			TimeStampUtil.writeTimestampToHBaseTable(sourceSchema, targetSchema, systemConfig, maxTimestamp);
		}
	}

	private static void printJobDetails(List<JobOutput> jobOutputList, Map<String, ReconEntity> reconMap,
			SystemConfig systemConfig, JobConfig jobConfig) {

		for (JobOutput jobOutput : jobOutputList) {

			ReconEntity reconEntity = reconMap.get(jobOutput.getJobId());

			System.out.print("\n****************************************************************************");
			System.out.println("****************************************************************************\n");
			System.out.println("Application Id: " + jobOutput.getAppId());
			System.out.println("\nJob Id: " + jobOutput.getJobId());

			if (jobConfig.getTarget().equalsIgnoreCase("hdfs") || jobConfig.getTarget().equalsIgnoreCase("hive")) {
				if (reconEntity.areHdfsFilesCreated()) {
					System.out.println("\nHdfs file path: " + jobOutput.getHdfsFilePath());
				} else {
					System.out.println("\nHdfs file path: No Files Created in HDFS.");
				}
			}

			if (!jobOutput.isIncremental()) {
				System.out.println("\nRecon Details: ");
				System.out.println("        Number of target Rows written: " + reconEntity.getRowCount());

				String jobId = jobOutput.getJobId();

				int index = Integer.parseInt(jobId.split("job")[1]);
				Map<String, List<String>> reconColumnOpMap = jobConfig.getReconColumnOpMap(index - 1);

				for (String column : reconColumnOpMap.keySet()) {

					for (String operation : reconColumnOpMap.get(column)) {

						String reconEntityKey = column + "_" + operation;

						System.out.println("        Column name: " + column + " Operation: " + operation + " Value: "
								+ reconEntity.getReconValue(reconEntityKey));
					}
				}
			}
		}
		System.out.print("\n****************************************************************************");
		System.out.println("****************************************************************************\n");
	}

}
