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

import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;
import org.gstn.schemaexplorer.hive.HiveTableExplorer;

public class CreateIncrementalHiveScript {

	public static void main(String[] args) throws InvalidSchemaException {

		if (args.length < 8 || args.length > 9) {
			System.out.println("Nine arguments are supported as follows: Source schema file path, Target Schema file path, Source schema name, Target schema name,"
					+ " HDFS URL, Hdfs file path, Hdfs base path and a suffix to add to file name, dynamicColumnsInSelect(true/false - optional)");
			return;
		}

		String sourceSchemaPath = args[0];
		String targetSchemaPath = args[1];
		String sourceSchemaName = args[2];
		String targetSchemaName = args[3];
		String hdfsUrl          = args[4];
		String hdfsFilePath     = args[5];
		String hdfsBasePath     = args[6];
		String jobId            = args[7];
		
		boolean dynamicColumnsInSelect=false;
		if(args.length>=9){
			dynamicColumnsInSelect = Boolean.valueOf(args[8]);
		}
		
		Map<String, String> map = new HashMap<>();
		
		
		HiveTableExplorer hiveExp = new HiveTableExplorer(targetSchemaPath);

		HBaseTableExplorer hBaseTableExplorer = new HBaseTableExplorer(sourceSchemaPath);
		
		hiveExp.createHiveScript(targetSchemaName, hdfsUrl, hdfsFilePath, hdfsBasePath, map, jobId, hBaseTableExplorer, sourceSchemaName, true, true, true, dynamicColumnsInSelect);
		
	}

}
