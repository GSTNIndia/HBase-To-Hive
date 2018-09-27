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

import java.util.HashMap;
import java.util.Map;

import org.gstn.schemaexplorer.hive.HiveTableExplorer;

public class CreateHiveScript {

	public static void main(String[] args) {

		if (args.length != 6) {
			System.out.println("Five arguments are required in the order: Schema file, Target schema name,"
					+ " HDFS URL, Hdfs file path, Hdfs base path and a suffix to add to file name");
			return;
		}

		String targetSchemaPath = args[0];
		String targetSchemaName = args[1];
		String hdfsUrl          = args[2];
		String hdfsFilePath     = args[3];
		String hdfsBasePath     = args[4];
		String jobId            = args[5];
		Map<String, String> map = new HashMap<>();
		
		
		HiveTableExplorer hiveExp = new HiveTableExplorer(targetSchemaPath);

		hiveExp.createInsertionHiveScript(targetSchemaName, hdfsUrl, hdfsFilePath, hdfsBasePath, map, jobId);
		
	}

}
