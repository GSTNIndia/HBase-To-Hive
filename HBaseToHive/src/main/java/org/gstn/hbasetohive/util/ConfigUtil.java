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
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gstn.hbasetohive.job.pojo.JobConfig;
import org.gstn.hbasetohive.job.pojo.SystemConfig;

public class ConfigUtil {

	public static SystemConfig getSystemConfig(String xmlConfFilePath) throws JAXBException, IOException {

		JAXBContext jaxbContext = JAXBContext.newInstance(SystemConfig.class);

		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();

		SystemConfig systemConfig = null;

		Configuration configuration = new Configuration();

		if (xmlConfFilePath.contains("hdfs://")) {
			FileSystem fileSystem = FileSystem.get(configuration);
			Path path = new Path(xmlConfFilePath);
			FSDataInputStream inputStream = fileSystem.open(path);
			systemConfig = (SystemConfig) jaxbUnmarshaller.unmarshal(inputStream);
		} else {
			File file = new File(xmlConfFilePath);
			systemConfig = (SystemConfig) jaxbUnmarshaller.unmarshal(file);
		}
		return systemConfig;

	}

	public static JobConfig getJobConfig(String xmlConfFilePath) throws JAXBException, IOException {

		JAXBContext jaxbContext = JAXBContext.newInstance(JobConfig.class);

		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();

		JobConfig jobConfig = null;

		Configuration configuration = new Configuration();

		if (xmlConfFilePath.contains("hdfs://")) {
			FileSystem fileSystem = FileSystem.get(configuration);
			Path path = new Path(xmlConfFilePath);
			FSDataInputStream inputStream = fileSystem.open(path);
			jobConfig = (JobConfig) jaxbUnmarshaller.unmarshal(inputStream);
		} else {
			File file = new File(xmlConfFilePath);
			jobConfig = (JobConfig) jaxbUnmarshaller.unmarshal(file);
		}
		return jobConfig;

	}

	public static String gethdfsFilePath(String hdfsBasePath, String target, String targetSchema, String appId,
			String identifier) {

		String hdfsFilePath;
		
		if (target.equalsIgnoreCase("hive") || target.equalsIgnoreCase("hdfs")) {
			if (hdfsBasePath.endsWith(File.separator)) {
				hdfsFilePath = hdfsBasePath + targetSchema + File.separator + appId + File.separator + identifier;
			} else {
				hdfsFilePath = hdfsBasePath + File.separator + targetSchema + File.separator + appId + File.separator
						+ identifier;
			}
		} else {
			hdfsFilePath = "";
		}
		return hdfsFilePath;

	}

}
