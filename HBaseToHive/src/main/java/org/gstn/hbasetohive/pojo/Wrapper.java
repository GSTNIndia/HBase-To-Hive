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
package org.gstn.hbasetohive.pojo;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.gstn.hbasetohive.job.pojo.ScanAndJobType;

@SuppressWarnings("serial")
public class Wrapper implements Serializable {
	
	JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD;
	ScanAndJobType scanAndJobType;
	
	public Wrapper(JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD, ScanAndJobType scanAndJobType) {
		this.hBaseRDD = hBaseRDD;
		this.scanAndJobType=scanAndJobType;
	}

	public JavaPairRDD<ImmutableBytesWritable, Result> gethBaseRDD() {
		return hBaseRDD;
	}

	public ScanAndJobType getScanAndJobType() {
		return scanAndJobType;
	}

}
