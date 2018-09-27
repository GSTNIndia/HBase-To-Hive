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
package org.gstn.hbasetohive.entity;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.gstn.hbasetohive.adapter.TargetAdapter;

import scala.Tuple2;

public class TargetAdapterWrapper {
	
	private TargetAdapter targetAdapter;

	// Tuple of (rowkey, result) for a given spark Partition
	private Iterator<Tuple2<ImmutableBytesWritable, Result>> sparkPartitionIterator;

	public TargetAdapterWrapper(TargetAdapter targetAdapter,
			Iterator<Tuple2<ImmutableBytesWritable, Result>> sparkPartitionIterator) {
		this.targetAdapter = targetAdapter;
		this.sparkPartitionIterator = sparkPartitionIterator;
	}

	public TargetAdapter getTargetAdapter() {
		return targetAdapter;
	}

	public Iterator<Tuple2<ImmutableBytesWritable, Result>> getSparkPartitionIterator() {
		return sparkPartitionIterator;
	}

}
