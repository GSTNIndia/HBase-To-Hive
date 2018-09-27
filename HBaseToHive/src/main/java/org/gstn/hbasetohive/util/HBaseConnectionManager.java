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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseConnectionManager {
	// map of hbase zk quorum vs hbase connection
	private static Map<String, Connection> connectionMap = null;

	public static Connection getConnection(String hBaseZkQuorum) throws IOException {
		Connection connection = null;
		if (connectionMap == null) {
			connectionMap = new HashMap<String, Connection>();
		} else {
			connection = connectionMap.get(hBaseZkQuorum);
		}

		if (connection == null || connection.isClosed()) {			
			synchronized (HBaseConnectionManager.class) {
				connection = connectionMap.get(hBaseZkQuorum);
				if (connection == null || connection.isClosed()) {
					Configuration conf = HBaseConfiguration.create();
					conf.set("hbase.zookeeper.quorum", hBaseZkQuorum);
					connection = ConnectionFactory.createConnection(conf);
					connectionMap.put(hBaseZkQuorum, connection);
				}
			}
		}
		return connection;
	}
}
