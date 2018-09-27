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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.xerial.snappy.Snappy;

public class ReadCompressedData {

	public static void main(String[] args) throws IOException, Exception {

		Configuration config = HBaseConfiguration.create();

		if (args.length < 4) {
			System.out.println("Expected 4 arguments in order zookeeper quorum, tableName, columnFamily, columnQualifier");
			return;
		}

		config.set("hbase.zookeeper.quorum", args[0]);
		
		String cf = args[2];
		String cq = args[3];
		String rk = null;

		if (args.length == 5) {
			rk = "|" + args[4] + "|";
			System.out.println(rk);
		}

		Scan scan = new Scan();

		QualifierFilter cqFilter = new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(cq));
		FamilyFilter cfFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(cf)));

		RowFilter rkFilter = null;
		FilterList filter = null;

		if (rk != null) {
			rkFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(rk));
			filter = new FilterList(FilterList.Operator.MUST_PASS_ALL, cqFilter, cfFilter, rkFilter);
		} else {
			filter = new FilterList(FilterList.Operator.MUST_PASS_ALL, cqFilter, cfFilter);
		}

		// long minStamp = 1510129000000l;
		// long maxStamp = 1511861470882l;

		scan.setFilter(filter);
		// scan.setTimeRange(minStamp, maxStamp);

		config.set(TableInputFormat.SCAN, convertScanToString(scan));

		Connection hbaseConnection = ConnectionFactory.createConnection(config);
		Table table = hbaseConnection.getTable(TableName.valueOf(args[1]));

		int count = 0;
		ResultScanner results = table.getScanner(scan);

		if (results != null) {
			for (Result result : results) {
				String rowKey = Bytes.toString(result.getRow());
				NavigableMap<byte[], byte[]> cnMap = result.getNoVersionMap().get(cf.getBytes());
				for (byte[] cnBytes : cnMap.keySet()) {
					String cn = Bytes.toString(cnBytes);
					byte[] value = cnMap.get(cnBytes);
					String valueString = null;
					try {
						valueString = Bytes.toString(Snappy.uncompress(value));
						count++;
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println("RowKey: " + rowKey + " Column Name: " + cn + "	Column Value: " + valueString);
				}
			}
		}
		System.out.println("********************Total rows: " + count);
		table.close();
	}

	private static String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}

}
