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
package org.gstn.hbasetohive.util;

import java.io.File;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.gstn.hbasetohive.exception.ValidationException;
import org.gstn.hbasetohive.job.pojo.Job;
import org.gstn.hbasetohive.job.pojo.JobConfig;
import org.gstn.hbasetohive.job.pojo.SystemConfig;

public class XmlToObject {

	public static void main(String[] args) throws ValidationException {

		SystemConfig systemConfig = null;
		JobConfig jobConfig = null;
		
		try {
			File file = new File("./src/main/resources/SampleJobConfig.xml");

			JAXBContext jaxbContext = JAXBContext.newInstance(JobConfig.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			
			jobConfig = (JobConfig) jaxbUnmarshaller.unmarshal(file);

			jobConfig.validate("SampleJobConfig.xml");
			
			System.out.println("JobConfig.xml");
			
			System.out.println("\nTarget: " + jobConfig.getTarget());
			
			System.out.println("Load type: " + jobConfig.getLoadType());
			
			System.out.println("\nJobs:");

			List<Job> jobs = jobConfig.getJobs().getJob();
			
			for (int i = 0; i < jobs.size(); i++) {
				System.out.println("\tJob" + (i + 1) + ": ");
				System.out.println("\t\tTarget Schema: " + jobs.get(i).getTargetSchemaName());
				System.out.println("\t\tQuery: " + jobs.get(i).getQuery());
				System.out.println("\t\tSource Schema: " + jobs.get(i).getSourceSchemaName());
			}

		} catch (JAXBException e) {
			System.err.println(e);
		} catch (ValidationException e) {
			System.err.println(e);
		}
		
		try {
			File file = new File("./src/main/resources/SampleSystemConfig.xml");

			JAXBContext jaxbContext = JAXBContext.newInstance(SystemConfig.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			
			systemConfig = (SystemConfig) jaxbUnmarshaller.unmarshal(file);

			systemConfig.validate("SampleSystemConfig.xml", jobConfig.getTarget(), jobConfig.getLoadType());
			
			Map<String, String> sparkConfMap = systemConfig.getSparkConfigurations();

			System.out.println("\n\nSystemConfig.xml");
			
			System.out.println("\nSpark Configurations: ");

			for (String conf : sparkConfMap.keySet()) {
				System.out.println("\t" + conf + " = " + sparkConfMap.get(conf));
			}

			System.out.println("\nNumber of partitions: " + systemConfig.getNumberOfPartitions());

			System.out.println("\nRun jobs parallelly: " + systemConfig.getRunJobsParallelly());
			
			System.out.println("\nSource zk quorum: " + systemConfig.getSourceHBaseZk());

			System.out.println("\nTarget zk quorum: " + systemConfig.getTargetHBaseZk());

			System.out.println("\nActive cluster: " + systemConfig.getActiveCluster());
			
			System.out.println("\nJob execution cluster: " + systemConfig.getJobExecutionCluster());
			
			System.out.println("\nTarget namenode: " + systemConfig.getDestHdfsUrl());
			
			System.out.println("\nTarget hdfs path: " + systemConfig.getHdfsBasePath());
			
			System.out.println("\nTimestamp table details: ");
			System.out.println("\tTable name   : " + systemConfig.getTableName());
			System.out.println("\tColumn family: " + systemConfig.getColumnFamily());
			System.out.println("\tColumn name  : " + systemConfig.getColumnName());
			
		} catch (JAXBException e) {
			System.err.println(e);
		} catch (ValidationException e) {
			System.err.println(e);
		}

	}
}
