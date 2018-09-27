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
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.gstn.hbasetohive.exception.ActiveClusterException;
import org.gstn.hbasetohive.job.pojo.MinTimestampAndJobType;
import org.gstn.hbasetohive.job.pojo.SystemConfig;

public class TimeStampUtil {

	public static MinTimestampAndJobType getMinTimestampAndJobType(String sourceSchema, String targetSchema,
			String sourceZK, SystemConfig systemConfig) throws IOException {

		boolean minTimestampFound = false;

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", sourceZK);

		Connection hbaseConnection = ConnectionFactory.createConnection(conf);

		String tableName = systemConfig.getTableName();
		String rowKey = sourceSchema + "_" + targetSchema;
		String cf = systemConfig.getColumnFamily();
		String cq = systemConfig.getColumnName();

		Table table = hbaseConnection.getTable(TableName.valueOf(tableName));

		Get get = new Get(Bytes.toBytes(rowKey));

		long minTimeStamp = 0L;

		Result result = table.get(get);
		if (result != null) {
			byte[] value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(cq));
			if (value != null) {
				minTimeStamp = Bytes.toLong(value);
				minTimestampFound = true;
			}
		}
		return new MinTimestampAndJobType(minTimestampFound, minTimeStamp);
	}

	public static void writeTimestampToHBaseTable(String sourceSchema, String targetSchema, String sourceZK,
			SystemConfig systemConfig, long time) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", sourceZK);

		Connection hbaseConnection = ConnectionFactory.createConnection(conf);

		String tableName = systemConfig.getTableName();
		String rowKey = sourceSchema + "_" + targetSchema;
		String cf = systemConfig.getColumnFamily();
		String cq = systemConfig.getColumnName();

		Table table = hbaseConnection.getTable(TableName.valueOf(tableName));

		Put put = new Put(Bytes.toBytes(rowKey));

		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes(time));
		table.put(put);
		table.close();
	}

	public static long readClusterTimestamp(String tableName, String sourceZK)
			throws IOException, ActiveClusterException {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", sourceZK);

		String cf = "DC";
		String cq1 = "DELETION_COUNT";
		String cq2 = "DATA_SOURCE";

		FamilyFilter cfFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(cf)));
		QualifierFilter cqFilter1 = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(cq1)));
		QualifierFilter cqFilter2 = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(cq2)));

		FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE, cfFilter, cqFilter1, cqFilter2);

		Scan scan = new Scan();
		scan.setFilter(filter);
		scan.setReversed(true);

		conf.set(TableInputFormat.SCAN, convertScanToString(scan));

		Connection hbaseConnection = ConnectionFactory.createConnection(conf);
		Table table = hbaseConnection.getTable(TableName.valueOf(tableName));

		ResultScanner results = table.getScanner(scan);

		if (results != null) {
			for (Result result : results) {
				NavigableMap<byte[], byte[]> cnMap = result.getNoVersionMap().get(cf.getBytes());
				int deletionCount = Integer.parseInt(Bytes.toString(cnMap.get(cq1.getBytes())));
				String dataSource = Bytes.toString(cnMap.get(cq2.getBytes()));
				if (dataSource.equalsIgnoreCase("DR") && deletionCount >= 0) {
					return Long.parseLong(Bytes.toString(result.getRow()));
				}
			}
		} else {
			throw new ActiveClusterException("Unable to read data from: " + tableName);
		}
		return 0L;

	}

	private static String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}

}
